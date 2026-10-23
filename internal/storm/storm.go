// Package storm is the fault-injection harness that turns the engine's
// correctness from a claim into a measurement. A simulated provider
// generates a scripted event set with a ground-truth oracle, then delivers
// it through the real HTTP receiver under a delivery storm: duplicate
// deliveries, windowed reordering, poison deliveries (bad signatures,
// malformed JSON, payload conflicts under a known ID, exact-signature
// replays, stale timestamps), and injected transient handler failures.
// Afterwards the live effect store is diffed against the oracle, the
// receipt chain is verified end-to-end, and the replay tool must
// reproduce live state byte-for-byte. A naive processor with no dedup,
// no conflict detection, and no retries runs the identical storm as the
// honest contrast.
//
// Multi-hour provider retry delays are compressed: retries arrive
// re-signed with fresh timestamps seconds apart instead of hours apart,
// which is exactly the property the receiver can observe. The one
// exception — a captured request replayed verbatim hours later — is
// modelled by the stale-timestamp and signature-replay poisons.
package storm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"time"

	"webhookledger/internal/engine"
	"webhookledger/internal/ledger"
	"webhookledger/internal/sign"
	"webhookledger/internal/store"
)

// Params controls one storm run. Zero values select the defaults used for
// the published results (240 unique events).
type Params struct {
	Seed              int64
	UniqueEvents      int
	Orders            int
	DuplicateFraction float64 // share of ALL deliveries that are duplicates
	PoisonFraction    float64 // share of ALL deliveries that are poison
	ShuffleWindow     int
	FailFirstRate     float64 // fraction of events whose 1st handler attempt fails
	FailSecondRate    float64 // fraction of events whose 1st two attempts fail
	MaxAttempts       int
}

func (p *Params) defaults() {
	if p.UniqueEvents <= 0 {
		p.UniqueEvents = 240
	}
	if p.Orders <= 0 {
		p.Orders = 60
	}
	if p.DuplicateFraction <= 0 {
		p.DuplicateFraction = 0.30
	}
	if p.PoisonFraction <= 0 {
		p.PoisonFraction = 0.05
	}
	if p.ShuffleWindow <= 0 {
		p.ShuffleWindow = 24
	}
	if p.FailFirstRate <= 0 {
		p.FailFirstRate = 0.25
	}
	if p.FailSecondRate <= 0 {
		p.FailSecondRate = 0.10
	}
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 5
	}
}

// delivery is one scripted HTTP delivery.
type delivery struct {
	kind     string // first | duplicate | badsig | malformed | conflict | sigreplay | stale
	headerID string
	ts       time.Time
	sig      string
	body     []byte
	eventID  string // payload event ID when parseable
}

// Results is the full measured record of one storm run (results.json).
type Results struct {
	Params             Params         `json:"params"`
	SecretNote         string         `json:"secret_note"`
	UniqueEvents       int            `json:"unique_events"`
	Orders             int            `json:"orders"`
	DeliveriesTotal    int            `json:"deliveries_total"`
	FirstDeliveries    int            `json:"first_deliveries"`
	DuplicateDeliverys int            `json:"duplicate_deliveries"`
	PoisonDeliveries   int            `json:"poison_deliveries"`
	PoisonByKind       map[string]int `json:"poison_by_kind"`
	DoomedEvent        string         `json:"doomed_event_id"`

	Engine                engine.Stats `json:"engine_stats"`
	EngineOracleDiffs     []string     `json:"engine_oracle_diffs,omitempty"`
	EngineOracleMismatch  int          `json:"engine_oracle_mismatched_orders"`
	EngineOracleIdentical bool         `json:"engine_oracle_identical"`

	NaiveApplications    int      `json:"naive_applications"`
	NaiveDoubleApplies   int      `json:"naive_double_applies"`
	NaiveDropped         int      `json:"naive_dropped_events"`
	NaiveOracleDiffs     []string `json:"naive_oracle_diffs,omitempty"`
	NaiveOracleMismatch  int      `json:"naive_oracle_mismatched_orders"`
	NaiveOracleIdentical bool     `json:"naive_oracle_identical"`

	LedgerEntries    int      `json:"ledger_entries"`
	ChainValid       bool     `json:"chain_valid"`
	ChainError       string   `json:"chain_error,omitempty"`
	ReceiptsVerified int      `json:"receipts_verified"`
	ReceiptFailures  []string `json:"receipt_failures,omitempty"`
	ReplayIdentical  bool     `json:"replay_identical"`
	ReplayDiffs      []string `json:"replay_diffs,omitempty"`
	ReplayEffects    int      `json:"replay_effects"`
	WallTimeMillis   int64    `json:"wall_time_ms"`
}

// failInjector returns a Handler that fails each event a scripted number
// of times before succeeding. The doomed event fails effectively forever.
type failInjector struct {
	remaining map[string]int
}

func (f *failInjector) handler(ev store.Event) error {
	if f.remaining[ev.ID] > 0 {
		f.remaining[ev.ID]--
		return fmt.Errorf("injected transient failure for %s", ev.ID)
	}
	return nil
}

func buildFailPlan(rng *rand.Rand, events []store.Event, p Params, doomedID string) map[string]int {
	plan := map[string]int{}
	for _, ev := range events {
		switch {
		case ev.ID == doomedID:
			plan[ev.ID] = 1 << 30
		case rng.Float64() < p.FailSecondRate:
			plan[ev.ID] = 2
		case rng.Float64() < p.FailFirstRate:
			plan[ev.ID] = 1
		}
	}
	return plan
}

func eventBody(ev store.Event) []byte {
	b, _ := json.Marshal(ev)
	return b
}

func signedDelivery(secret []byte, kind, headerID string, ts time.Time, body []byte, eventID string) delivery {
	return delivery{
		kind:     kind,
		headerID: headerID,
		ts:       ts,
		sig:      sign.SignatureHeader(secret, headerID, ts, body),
		body:     body,
		eventID:  eventID,
	}
}

// Run executes one full storm and returns the measured results. dataDir
// receives the engine's ledger/receipts/effects/payloads files.
func Run(p Params, dataDir string, secret []byte) (Results, error) {
	p.defaults()
	start := time.Now()
	rng := rand.New(rand.NewSource(p.Seed))
	base := time.Now().UTC().Truncate(time.Second)

	// ---- Generate the unique event set ---------------------------------
	var events []store.Event
	items := []string{"widget", "gadget", "sensor", "valve", "motor", "panel", "cable", "pump"}
	for i := 0; i < p.Orders; i++ {
		var ev store.Event
		ev.ID = fmt.Sprintf("evt_%04d", len(events))
		ev.Type = store.TypeOrderCreated
		ev.Data.OrderID = fmt.Sprintf("ord_%03d", i+1)
		ev.Data.AmountCents = int64(500 + rng.Intn(20000))
		ev.Data.Item = items[rng.Intn(len(items))]
		events = append(events, ev)
	}
	types := []string{store.TypeOrderPaid, store.TypeOrderRefunded, store.TypeOrderCanceled}
	for len(events) < p.UniqueEvents {
		var ev store.Event
		ev.ID = fmt.Sprintf("evt_%04d", len(events))
		ev.Type = types[rng.Intn(len(types))]
		ev.Data.OrderID = fmt.Sprintf("ord_%03d", 1+rng.Intn(p.Orders))
		ev.Data.AmountCents = int64(100 + rng.Intn(15000))
		events = append(events, ev)
	}
	doomed := events[len(events)-1].ID

	// ---- Oracle: every unique event applied exactly once, except the
	// doomed one (its handler never succeeds, so its true downstream
	// effect is the DLQ, not the store).
	oracle := store.New()
	for _, ev := range events {
		if ev.ID == doomed {
			continue
		}
		oracle.Apply(ev)
	}

	// ---- Delivery script ------------------------------------------------
	total := int(float64(p.UniqueEvents) / (1 - p.DuplicateFraction - p.PoisonFraction))
	dupCount := int(float64(total) * p.DuplicateFraction)
	poisonCount := total - p.UniqueEvents - dupCount
	if poisonCount < 0 {
		poisonCount = 0
	}

	var deliveries []delivery
	firstIndex := map[string]int{} // event ID -> index of first delivery (set after shuffle via fix-up)
	for _, ev := range events {
		d := signedDelivery(secret, "first", ev.ID, base, eventBody(ev), ev.ID)
		deliveries = append(deliveries, d)
	}
	// Duplicates: provider retries, re-signed with a fresh timestamp (the
	// observable property of a retry hours later, compressed to seconds).
	copyCount := map[string]int{}
	for i := 0; i < dupCount; i++ {
		ev := events[rng.Intn(len(events))]
		copyCount[ev.ID]++
		off := time.Duration(copyCount[ev.ID]*37) * time.Second
		d := signedDelivery(secret, "duplicate", ev.ID, base.Add(off), eventBody(ev), ev.ID)
		deliveries = append(deliveries, d)
	}
	// Poison: pattern of 9 -> 3 bad signature, 2 malformed, 2 conflict,
	// 1 signature replay, 1 stale timestamp.
	pattern := []string{"badsig", "badsig", "badsig", "malformed", "malformed", "conflict", "conflict", "sigreplay", "stale"}
	poisonByKind := map[string]int{}
	var pendingReplays []delivery // appended after shuffle fix-up
	var pendingConflicts []delivery
	conflictTargets := map[string]bool{}
	for i := 0; i < poisonCount; i++ {
		kind := pattern[i%len(pattern)]
		poisonByKind[kind]++
		switch kind {
		case "badsig":
			ev := events[rng.Intn(len(events))]
			body := eventBody(ev)
			d := signedDelivery([]byte("wrong-secret"), kind, ev.ID, base, body, ev.ID)
			deliveries = append(deliveries, d)
		case "malformed":
			id := fmt.Sprintf("evt_poison_m%02d", i)
			body := []byte(`{"id":"` + id + `","type":"order.paid","data":{"order_id":"ord_001","amount_cents":`)
			d := signedDelivery(secret, kind, id, base, body, id)
			deliveries = append(deliveries, d)
		case "conflict":
			// Same event ID, different payload bytes, validly signed —
			// the provider-side bug no dedup window saves you from.
			var ev store.Event
			for tries := 0; tries < 20; tries++ {
				cand := events[rng.Intn(len(events))]
				if cand.ID != doomed && !conflictTargets[cand.ID] {
					ev = cand
					break
				}
			}
			if ev.ID == "" {
				ev = events[0]
			}
			conflictTargets[ev.ID] = true
			bad := ev
			bad.Data.AmountCents += 777
			if bad.Type == store.TypeOrderCreated {
				bad.Data.Item = "tampered-item"
			}
			d := signedDelivery(secret, kind, ev.ID, base.Add(91*time.Second), eventBody(bad), ev.ID)
			pendingConflicts = append(pendingConflicts, d)
		case "sigreplay":
			// Byte-identical retransmission of a real delivery (the
			// captured-request replay). Rebuilt from the event so the
			// signature matches the original first delivery exactly.
			ev := events[rng.Intn(len(events))]
			d := signedDelivery(secret, "first", ev.ID, base, eventBody(ev), ev.ID)
			d.kind = "sigreplay"
			pendingReplays = append(pendingReplays, d)
		case "stale":
			ev := events[rng.Intn(len(events))]
			d := signedDelivery(secret, kind, ev.ID, base.Add(-2*time.Hour), eventBody(ev), ev.ID)
			deliveries = append(deliveries, d)
		}
	}

	// ---- Windowed shuffle (out-of-order delivery), then fix-ups so a
	// conflict or replay never precedes the original's first delivery —
	// that ordering is what "first payload wins" means, and the oracle is
	// defined against it.
	for i := 0; i < len(deliveries); i += p.ShuffleWindow {
		end := i + p.ShuffleWindow
		if end > len(deliveries) {
			end = len(deliveries)
		}
		rng.Shuffle(end-i, func(a, b int) { deliveries[i+a], deliveries[i+b] = deliveries[i+b], deliveries[i+a] })
	}
	for idx, d := range deliveries {
		if d.kind == "first" {
			if _, ok := firstIndex[d.eventID]; !ok {
				firstIndex[d.eventID] = idx
			}
		}
	}
	insertAfter := func(d delivery, afterIdx int) {
		pos := afterIdx + 1 + rng.Intn(10)
		if pos > len(deliveries) {
			pos = len(deliveries)
		}
		deliveries = append(deliveries, delivery{})
		copy(deliveries[pos+1:], deliveries[pos:])
		deliveries[pos] = d
	}
	for _, d := range pendingConflicts {
		insertAfter(d, firstIndex[d.eventID])
	}
	for _, d := range pendingReplays {
		insertAfter(d, firstIndex[d.eventID])
	}

	// ---- Run the engine over real HTTP ----------------------------------
	failPlan := buildFailPlan(rand.New(rand.NewSource(p.Seed+1)), events, p, doomed)
	injector := &failInjector{remaining: map[string]int{}}
	for k, v := range failPlan {
		injector.remaining[k] = v
	}
	eng, err := engine.New(engine.Config{
		Secret:      secret,
		DataDir:     dataDir,
		MaxAttempts: p.MaxAttempts,
		Backoff:     engine.Backoff{Base: 2 * time.Millisecond, Cap: 50 * time.Millisecond, Rand: rand.New(rand.NewSource(p.Seed + 2))},
		Now:         func() time.Time { return base },
		Handler:     injector.handler,
	})
	if err != nil {
		return Results{}, err
	}
	srv := httptest.NewServer(eng.Handler())
	defer srv.Close()
	client := srv.Client()
	for _, d := range deliveries {
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/webhooks", bytes.NewReader(d.body))
		if err != nil {
			return Results{}, err
		}
		req.Header.Set("webhook-id", d.headerID)
		req.Header.Set("webhook-timestamp", strconv.FormatInt(d.ts.Unix(), 10))
		req.Header.Set("webhook-signature", d.sig)
		resp, err := client.Do(req)
		if err != nil {
			return Results{}, err
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	if err := eng.PersistPayloads(); err != nil {
		return Results{}, err
	}

	// ---- Run the naive processor in-process on the identical script -----
	naiveInjector := &failInjector{remaining: map[string]int{}}
	for k, v := range failPlan {
		naiveInjector.remaining[k] = v
	}
	naive := engine.NewNaive(secret, naiveInjector.handler)
	naive.Now = func() time.Time { return base }
	for _, d := range deliveries {
		naive.Process(d.headerID, strconv.FormatInt(d.ts.Unix(), 10), d.sig, d.body)
	}

	// ---- Verification phase ----------------------------------------------
	entries := eng.Ledger().Entries()
	chainErr := ledger.VerifyEntries(entries)
	receipts := eng.AllReceipts()
	receiptsOK, receiptBad := engine.VerifyAllReceipts(secret, receipts, entries)
	payloads := map[string][]byte{}
	for _, ent := range entries {
		if ent.Kind == ledger.KindApplied {
			if body, ok := eng.Payload(ent.PayloadHash); ok {
				payloads[ent.PayloadHash] = body
			}
		}
	}
	replayed, _, replayErr := engine.ReplayEffects(entries, payloads)
	var replayRes engine.ReplayResult
	if replayErr == nil {
		replayRes = engine.DiffStores(replayed, eng.Store())
	} else {
		replayRes = engine.ReplayResult{Identical: false, Diffs: []string{replayErr.Error()}}
	}

	engineDiffs := store.Diff(oracle, eng.Store())
	naiveDiffs := store.Diff(oracle, naive.Store)

	res := Results{
		Params:                p,
		SecretNote:            "storm uses a fixed test secret; receipts verify under it",
		UniqueEvents:          len(events),
		Orders:                p.Orders,
		DeliveriesTotal:       len(deliveries),
		FirstDeliveries:       p.UniqueEvents,
		DuplicateDeliverys:    dupCount,
		PoisonDeliveries:      poisonCount,
		PoisonByKind:          poisonByKind,
		DoomedEvent:           doomed,
		Engine:                eng.Stats(),
		EngineOracleDiffs:     engineDiffs,
		EngineOracleMismatch:  len(engineDiffs),
		EngineOracleIdentical: len(engineDiffs) == 0 && bytes.Equal(oracle.Canonical(), eng.Store().Canonical()),
		NaiveApplications:     naive.Applications,
		NaiveDoubleApplies:    naive.DoubleApplies(),
		NaiveDropped:          naive.Dropped,
		NaiveOracleDiffs:      naiveDiffs,
		NaiveOracleMismatch:   len(naiveDiffs),
		NaiveOracleIdentical:  len(naiveDiffs) == 0 && bytes.Equal(oracle.Canonical(), naive.Store.Canonical()),
		LedgerEntries:         len(entries),
		ChainValid:            chainErr == nil,
		ReceiptsVerified:      receiptsOK,
		ReceiptFailures:       receiptBad,
		ReplayIdentical:       replayRes.Identical,
		ReplayDiffs:           replayRes.Diffs,
		ReplayEffects:         replayRes.ReplayEffects,
		WallTimeMillis:        time.Since(start).Milliseconds(),
	}
	if chainErr != nil {
		res.ChainError = chainErr.Error()
	}
	// Stable ordering for readable diffs in results.json.
	sort.Strings(res.EngineOracleDiffs)
	sort.Strings(res.NaiveOracleDiffs)
	return res, nil
}
