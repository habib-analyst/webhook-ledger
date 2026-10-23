package engine

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"webhookledger/internal/ledger"
	"webhookledger/internal/sign"
	"webhookledger/internal/store"
)

var (
	engSecret = []byte("engine-test-secret")
	engNow    = time.Unix(1_760_000_000, 0)
)

func newTestEngine(t *testing.T, handler Handler, maxAttempts int) *Engine {
	t.Helper()
	e, err := New(Config{
		Secret:      engSecret,
		MaxAttempts: maxAttempts,
		Backoff:     Backoff{Base: time.Millisecond, Cap: 5 * time.Millisecond, Rand: rand.New(rand.NewSource(1))},
		Sleep:       func(time.Duration) {},
		Now:         func() time.Time { return engNow },
		Handler:     handler,
	})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func bodyOf(id, typ, order string, amount int64) []byte {
	return []byte(fmt.Sprintf(`{"id":"%s","type":"%s","data":{"order_id":"%s","amount_cents":%d}}`, id, typ, order, amount))
}

// deliver signs body at ts and processes it.
func deliver(e *Engine, id string, ts time.Time, body []byte) Outcome {
	return e.Process(id, strconv.FormatInt(ts.Unix(), 10), sign.SignatureHeader(engSecret, id, ts, body), body)
}

func TestApplyOnce(t *testing.T) {
	e := newTestEngine(t, nil, 3)
	out := deliver(e, "evt_1", engNow, bodyOf("evt_1", store.TypeOrderPaid, "ord_1", 700))
	if out.Result != "applied" || out.HTTPStatus != 200 {
		t.Fatalf("outcome = %+v", out)
	}
	if got := e.Store().Orders["ord_1"].TotalPaid; got != 700 {
		t.Fatalf("total paid = %d, want 700", got)
	}
	if e.Stats().ReceiptsIssued != 1 {
		t.Fatalf("receipts = %d, want 1", e.Stats().ReceiptsIssued)
	}
}

func TestDuplicateSuppressed(t *testing.T) {
	e := newTestEngine(t, nil, 3)
	body := bodyOf("evt_1", store.TypeOrderPaid, "ord_1", 700)
	deliver(e, "evt_1", engNow, body)
	// Provider retry: same body, re-signed at a later timestamp.
	out := deliver(e, "evt_1", engNow.Add(time.Minute), body)
	if out.Result != "duplicate" {
		t.Fatalf("second delivery outcome = %+v, want duplicate", out)
	}
	if got := e.Store().Orders["ord_1"].TotalPaid; got != 700 {
		t.Fatalf("duplicate changed state: total = %d, want 700", got)
	}
	if e.Stats().DuplicatesSuppressed != 1 {
		t.Fatalf("dup stat = %d, want 1", e.Stats().DuplicatesSuppressed)
	}
}

func TestConflictGoesToDLQAndNeverApplies(t *testing.T) {
	e := newTestEngine(t, nil, 3)
	deliver(e, "evt_1", engNow, bodyOf("evt_1", store.TypeOrderPaid, "ord_1", 700))
	out := deliver(e, "evt_1", engNow.Add(time.Minute), bodyOf("evt_1", store.TypeOrderPaid, "ord_1", 9999))
	if out.Result != "conflict" || out.HTTPStatus != 409 {
		t.Fatalf("conflict outcome = %+v", out)
	}
	if got := e.Store().Orders["ord_1"].TotalPaid; got != 700 {
		t.Fatalf("conflicting payload was applied: total = %d, want 700", got)
	}
	dlq := e.DLQ()
	if len(dlq) != 1 || dlq[0].Reason != ReasonPayloadConflict {
		t.Fatalf("dlq = %+v, want one payload_conflict", dlq)
	}
}

func TestBadSignatureRejected(t *testing.T) {
	e := newTestEngine(t, nil, 3)
	body := bodyOf("evt_1", store.TypeOrderPaid, "ord_1", 700)
	bad := sign.SignatureHeader([]byte("forged"), "evt_1", engNow, body)
	out := e.Process("evt_1", strconv.FormatInt(engNow.Unix(), 10), bad, body)
	if out.Result != "rejected" || out.Reason != ReasonBadSignature {
		t.Fatalf("outcome = %+v", out)
	}
	if e.Store().EffectCount() != 0 {
		t.Fatal("forged delivery must not apply effects")
	}
}

func TestMalformedJSONRejected(t *testing.T) {
	e := newTestEngine(t, nil, 3)
	body := []byte(`{"id":"evt_x","type":"order.paid","data":`)
	out := deliver(e, "evt_x", engNow, body)
	if out.Result != "rejected" || out.Reason != ReasonMalformedJSON {
		t.Fatalf("outcome = %+v", out)
	}
}

func TestSignatureReplayRejected(t *testing.T) {
	e := newTestEngine(t, nil, 3)
	body := bodyOf("evt_1", store.TypeOrderPaid, "ord_1", 700)
	sig := sign.SignatureHeader(engSecret, "evt_1", engNow, body)
	ts := strconv.FormatInt(engNow.Unix(), 10)
	if out := e.Process("evt_1", ts, sig, body); out.Result != "applied" {
		t.Fatalf("first delivery: %+v", out)
	}
	// Byte-identical retransmission: a replay, not a retry.
	out := e.Process("evt_1", ts, sig, body)
	if out.Result != "rejected" || out.Reason != ReasonSignatureReplay {
		t.Fatalf("replay outcome = %+v, want signature_replay rejection", out)
	}
}

func TestStaleTimestampRejected(t *testing.T) {
	e := newTestEngine(t, nil, 3)
	body := bodyOf("evt_1", store.TypeOrderPaid, "ord_1", 700)
	out := deliver(e, "evt_1", engNow.Add(-2*time.Hour), body)
	if out.Result != "rejected" || out.Reason != ReasonStaleTimestamp {
		t.Fatalf("outcome = %+v", out)
	}
}

func TestTransientFailureRetriedWithBackoff(t *testing.T) {
	calls := 0
	handler := func(ev store.Event) error {
		calls++
		if calls < 3 {
			return fmt.Errorf("transient boom %d", calls)
		}
		return nil
	}
	e := newTestEngine(t, handler, 5)
	out := deliver(e, "evt_1", engNow, bodyOf("evt_1", store.TypeOrderPaid, "ord_1", 700))
	if out.Result != "applied" || out.Attempts != 3 {
		t.Fatalf("outcome = %+v, want applied after 3 attempts", out)
	}
	if e.Stats().HandlerRetries != 2 {
		t.Fatalf("retries = %d, want 2", e.Stats().HandlerRetries)
	}
	kinds := map[string]int{}
	for _, ent := range e.Ledger().Entries() {
		kinds[ent.Kind]++
	}
	if kinds[ledger.KindHandlerFailed] != 2 || kinds[ledger.KindRetryScheduled] != 2 {
		t.Fatalf("ledger kinds = %v, want 2 handler_failed + 2 retry_scheduled", kinds)
	}
}

func TestPermanentFailureDeadLetters(t *testing.T) {
	handler := func(ev store.Event) error { return fmt.Errorf("always broken") }
	e := newTestEngine(t, handler, 3)
	body := bodyOf("evt_1", store.TypeOrderPaid, "ord_1", 700)
	out := deliver(e, "evt_1", engNow, body)
	if out.Result != "dead_lettered" || out.Attempts != 3 {
		t.Fatalf("outcome = %+v", out)
	}
	dlq := e.DLQ()
	if len(dlq) != 1 || dlq[0].Reason != ReasonHandlerExhausted || dlq[0].Attempts != 3 {
		t.Fatalf("dlq = %+v", dlq)
	}
	if e.Store().EffectCount() != 0 {
		t.Fatal("dead-lettered event must not apply effects")
	}
	// A later provider retry of the same payload is a duplicate of a
	// known event, not a second bite at the handler.
	out = deliver(e, "evt_1", engNow.Add(time.Minute), body)
	if out.Result != "duplicate" {
		t.Fatalf("redelivery after DLQ: outcome = %+v, want duplicate", out)
	}
}

func TestBackoffSchedule(t *testing.T) {
	b := Backoff{Base: 100 * time.Millisecond, Cap: time.Second}
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, time.Second, time.Second}
	for i, w := range want {
		if got := b.Delay(i + 1); got != w {
			t.Fatalf("Delay(%d) = %s, want %s", i+1, got, w)
		}
	}
	jittered := Backoff{Base: 100 * time.Millisecond, Cap: time.Second, Rand: rand.New(rand.NewSource(7))}
	for i := 0; i < 50; i++ {
		if d := jittered.Delay(3); d < 0 || d > 400*time.Millisecond {
			t.Fatalf("jittered delay out of bounds: %s", d)
		}
	}
}

func TestReceiptVerifiesAndTamperingFails(t *testing.T) {
	e := newTestEngine(t, nil, 3)
	deliver(e, "evt_1", engNow, bodyOf("evt_1", store.TypeOrderPaid, "ord_1", 700))
	rcpt, ok := e.ReceiptFor("evt_1")
	if !ok {
		t.Fatal("no receipt issued")
	}
	if !VerifyReceipt(engSecret, rcpt) {
		t.Fatal("issued receipt must verify")
	}
	if VerifyReceipt([]byte("other-secret"), rcpt) {
		t.Fatal("receipt must not verify under a different secret")
	}
	tampered := rcpt
	tampered.PayloadHash = "0000tampered"
	if VerifyReceipt(engSecret, tampered) {
		t.Fatal("tampered receipt must not verify")
	}
	okCount, bad := VerifyAllReceipts(engSecret, e.AllReceipts(), e.Ledger().Entries())
	if okCount != 1 || len(bad) != 0 {
		t.Fatalf("VerifyAllReceipts = %d ok, %v bad", okCount, bad)
	}
	// Receipt must point at the actual "applied" ledger entry.
	ent := e.Ledger().Entries()[rcpt.LedgerSeq]
	if ent.Kind != ledger.KindApplied || ent.Hash != rcpt.LedgerHash {
		t.Fatal("receipt does not commit to the applied ledger entry")
	}
}

func TestReplayIdentity(t *testing.T) {
	e := newTestEngine(t, nil, 3)
	deliver(e, "evt_1", engNow, bodyOf("evt_1", store.TypeOrderCreated, "ord_1", 1000))
	deliver(e, "evt_2", engNow, bodyOf("evt_2", store.TypeOrderPaid, "ord_1", 1000))
	deliver(e, "evt_2", engNow.Add(time.Minute), bodyOf("evt_2", store.TypeOrderPaid, "ord_1", 1000)) // duplicate
	deliver(e, "evt_3", engNow, bodyOf("evt_3", store.TypeOrderRefunded, "ord_1", 200))

	entries := e.Ledger().Entries()
	payloads := map[string][]byte{}
	for _, ent := range entries {
		if ent.Kind == ledger.KindApplied {
			body, ok := e.Payload(ent.PayloadHash)
			if !ok {
				t.Fatalf("payload for seq %d not retained", ent.Seq)
			}
			payloads[ent.PayloadHash] = body
		}
	}
	replayed, applied, err := ReplayEffects(entries, payloads)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 3 {
		t.Fatalf("replayed %d applied entries, want 3", applied)
	}
	res := DiffStores(replayed, e.Store())
	if !res.Identical {
		t.Fatalf("replay differs from live: %v", res.Diffs)
	}
}

func TestHTTPEndToEnd(t *testing.T) {
	e := newTestEngine(t, nil, 3)
	srv := httptest.NewServer(e.Handler())
	defer srv.Close()

	body := bodyOf("evt_http", store.TypeOrderPaid, "ord_9", 1234)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/webhooks", strings.NewReader(string(body)))
	req.Header.Set("webhook-id", "evt_http")
	req.Header.Set("webhook-timestamp", strconv.FormatInt(engNow.Unix(), 10))
	req.Header.Set("webhook-signature", sign.SignatureHeader(engSecret, "evt_http", engNow, body))
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	rresp, err := srv.Client().Get(srv.URL + "/receipts/evt_http")
	if err != nil {
		t.Fatal(err)
	}
	defer rresp.Body.Close()
	if rresp.StatusCode != 200 {
		t.Fatalf("receipt status = %d", rresp.StatusCode)
	}
	var rcpt Receipt
	if err := json.NewDecoder(rresp.Body).Decode(&rcpt); err != nil {
		t.Fatal(err)
	}
	if !VerifyReceipt(engSecret, rcpt) {
		t.Fatal("receipt served over HTTP must verify")
	}

	bad, _ := http.NewRequest(http.MethodPost, srv.URL+"/webhooks", strings.NewReader(string(body)))
	bad.Header.Set("webhook-id", "evt_http")
	bad.Header.Set("webhook-timestamp", strconv.FormatInt(engNow.Unix(), 10))
	bad.Header.Set("webhook-signature", "v1,AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	bresp, err := srv.Client().Do(bad)
	if err != nil {
		t.Fatal(err)
	}
	defer bresp.Body.Close()
	if bresp.StatusCode != 400 {
		t.Fatalf("forged delivery status = %d, want 400", bresp.StatusCode)
	}
}
