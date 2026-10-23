// Package engine is the reconciliation core: a Standard Webhooks receiver
// in front of an idempotent effect processor. Every delivery — accepted,
// rejected, duplicated, conflicted, dead-lettered — is recorded in the
// hash-chained ledger, and every applied event produces a signed receipt
// the provider can verify with the shared secret.
package engine

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"webhookledger/internal/ledger"
	"webhookledger/internal/sign"
	"webhookledger/internal/store"
)

// DLQ reasons (classified, never a bare "failed").
const (
	ReasonPayloadConflict   = "payload_conflict"    // same event ID, different payload bytes
	ReasonHandlerExhausted  = "handler_exhausted"   // handler kept failing through all retries
	ReasonBadSignature      = "bad_signature"       // signature did not verify
	ReasonStaleTimestamp    = "stale_timestamp"     // outside tolerance window
	ReasonSignatureReplay   = "signature_replay"    // exact signature bytes already seen
	ReasonMalformedJSON     = "malformed_json"      // body is not a structurally valid event
	ReasonUnknownEventShape = "unknown_event_shape" // valid JSON, unusable event
)

// Outcome of one delivery, returned to the HTTP layer and the storm.
type Outcome struct {
	HTTPStatus int    `json:"http_status"`
	Result     string `json:"result"` // applied | duplicate | conflict | rejected | dead_lettered
	Reason     string `json:"reason,omitempty"`
	EventID    string `json:"event_id,omitempty"`
	Attempts   int    `json:"attempts,omitempty"`
}

// Handler applies one event's effect. Returning an error marks the
// attempt as a transient failure (retried with backoff) — the handler
// must NOT partially apply before erroring; the engine calls it again
// from scratch.
type Handler func(ev store.Event) error

// Backoff computes retry delays: base * 2^(attempt-1), capped, with full
// jitter drawn from the injected RNG (deterministic under a seeded RNG).
type Backoff struct {
	Base time.Duration
	Cap  time.Duration
	Rand *rand.Rand
}

// Delay returns the wait before retry number attempt (attempt starts at 1
// for the first retry).
func (b Backoff) Delay(attempt int) time.Duration {
	d := b.Base
	for i := 1; i < attempt; i++ {
		d *= 2
		if d > b.Cap {
			d = b.Cap
			break
		}
	}
	if d > b.Cap {
		d = b.Cap
	}
	if b.Rand != nil && d > 0 {
		return time.Duration(b.Rand.Int63n(int64(d)))
	}
	return d
}

// Config wires an Engine.
type Config struct {
	Secret        []byte
	DataDir       string // ledger.jsonl, payloads.jsonl, receipts.jsonl, dlq.jsonl, effects.json
	Tolerance     time.Duration
	MaxAttempts   int // total handler attempts per delivery before DLQ (default 5)
	Backoff       Backoff
	Sleep         func(time.Duration) // injectable; default time.Sleep
	Now           func() time.Time    // injectable clock
	Handler       Handler             // business effect handler (in addition to the built-in store)
	SignTolerance time.Duration
}

// eventRecord is the registry entry for one provider event ID.
type eventRecord struct {
	PayloadHash string
	Status      string // applied | dead_lettered
	ReceiptSeq  int64
}

// DLQEntry is one dead-letter record.
type DLQEntry struct {
	EventID     string    `json:"event_id"`
	PayloadHash string    `json:"payload_hash"`
	Reason      string    `json:"reason"`
	Detail      string    `json:"detail,omitempty"`
	Attempts    int       `json:"attempts,omitempty"`
	TS          time.Time `json:"ts"`
}

// Receipt proves one applied event. Signature is HMAC-SHA256 (base64) over
// the canonical receipt string, keyed with the provider secret: any holder
// of the secret can verify what was applied, at which ledger position.
type Receipt struct {
	EventID     string    `json:"event_id"`
	PayloadHash string    `json:"payload_hash"`
	LedgerSeq   int64     `json:"ledger_seq"`
	LedgerHash  string    `json:"ledger_hash"`
	AppliedAt   time.Time `json:"applied_at"`
	Signature   string    `json:"signature"`
}

// Canonical is the exact string the receipt signature commits to.
func (r Receipt) Canonical() string {
	return fmt.Sprintf("%s|%s|%d|%s|%s", r.EventID, r.PayloadHash, r.LedgerSeq, r.LedgerHash, r.AppliedAt.UTC().Format(time.RFC3339Nano))
}

// VerifyReceipt checks a receipt's signature against the shared secret.
func VerifyReceipt(secret []byte, r Receipt) bool {
	return sign.EqualHMAC(sign.HMACBase64(secret, r.Canonical()), r.Signature)
}

// Engine is the full receiver + processor. Processing is serialised by a
// mutex: single-node by design (see PAPER.md honest limits), which makes
// the applied sequence — and therefore receipts and replay — total.
type Engine struct {
	cfg     Config
	mu      sync.Mutex
	ledger  *ledger.Ledger
	store   *store.Store
	records map[string]*eventRecord
	seenSig map[string]bool
	receipt map[string]Receipt
	dlq     []DLQEntry
	// payloads retains verified raw bodies by payload hash for replay.
	payloads map[string][]byte
	stats    Stats
}

// Stats counts delivery outcomes over the engine's lifetime.
type Stats struct {
	Deliveries           int `json:"deliveries"`
	Applied              int `json:"applied"`
	DuplicatesSuppressed int `json:"duplicates_suppressed"`
	Conflicts            int `json:"conflicts"`
	RejectedBadSig       int `json:"rejected_bad_signature"`
	RejectedStale        int `json:"rejected_stale_timestamp"`
	RejectedSigReplay    int `json:"rejected_signature_replay"`
	RejectedMalformed    int `json:"rejected_malformed"`
	HandlerAttempts      int `json:"handler_attempts"`
	HandlerRetries       int `json:"handler_retries"`
	DeadLettered         int `json:"dead_lettered"`
	ReceiptsIssued       int `json:"receipts_issued"`
}

// New builds an engine over a data directory (created if needed). Pass
// DataDir "" for a fully in-memory engine.
func New(cfg Config) (*Engine, error) {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.Backoff.Base <= 0 {
		cfg.Backoff.Base = 100 * time.Millisecond
	}
	if cfg.Backoff.Cap <= 0 {
		cfg.Backoff.Cap = 30 * time.Second
	}
	if cfg.Sleep == nil {
		cfg.Sleep = time.Sleep
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Tolerance <= 0 {
		cfg.Tolerance = sign.DefaultTolerance
	}
	e := &Engine{
		cfg:      cfg,
		store:    store.New(),
		records:  map[string]*eventRecord{},
		seenSig:  map[string]bool{},
		receipt:  map[string]Receipt{},
		payloads: map[string][]byte{},
	}
	ledgerPath := ""
	if cfg.DataDir != "" {
		if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
			return nil, err
		}
		ledgerPath = filepath.Join(cfg.DataDir, "ledger.jsonl")
	}
	lg, err := ledger.Open(ledgerPath)
	if err != nil {
		return nil, err
	}
	lg.SetClock(cfg.Now)
	e.ledger = lg
	return e, nil
}

// Store exposes the live effect store.
func (e *Engine) Store() *store.Store { return e.store }

// Ledger exposes the ledger.
func (e *Engine) Ledger() *ledger.Ledger { return e.ledger }

// Stats returns a snapshot of outcome counters.
func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stats
}

// DLQ returns dead-letter entries.
func (e *Engine) DLQ() []DLQEntry {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]DLQEntry, len(e.dlq))
	copy(out, e.dlq)
	return out
}

// ReceiptFor returns the receipt issued for an applied event.
func (e *Engine) ReceiptFor(eventID string) (Receipt, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.receipt[eventID]
	return r, ok
}

// AllReceipts returns every issued receipt, ordered by ledger sequence.
func (e *Engine) AllReceipts() []Receipt {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Receipt, 0, len(e.receipt))
	for _, r := range e.receipt {
		out = append(out, r)
	}
	for i := 1; i < len(out); i++ { // insertion sort by seq; sets are small
		for j := i; j > 0 && out[j-1].LedgerSeq > out[j].LedgerSeq; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

// Payload returns the retained raw body for a payload hash.
func (e *Engine) Payload(hash string) ([]byte, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	b, ok := e.payloads[hash]
	return b, ok
}

func (e *Engine) appendLedger(kind, eventID, payloadHash, detail string) ledger.Entry {
	ent, err := e.ledger.Append(kind, eventID, payloadHash, detail)
	if err != nil {
		// Ledger persistence is on the critical path by design: if we
		// cannot record it, we must not pretend we processed it.
		panic(fmt.Sprintf("ledger append failed: %v", err))
	}
	return ent
}

func (e *Engine) deadLetter(eventID, payloadHash, reason, detail string, attempts int) {
	entry := DLQEntry{
		EventID: eventID, PayloadHash: payloadHash, Reason: reason,
		Detail: detail, Attempts: attempts, TS: e.cfg.Now().UTC(),
	}
	e.dlq = append(e.dlq, entry)
	e.stats.DeadLettered++
	if e.cfg.DataDir != "" {
		line, _ := json.Marshal(entry)
		f, err := os.OpenFile(filepath.Join(e.cfg.DataDir, "dlq.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err == nil {
			f.Write(append(line, '\n'))
			f.Close()
		}
	}
}

// Process handles one raw delivery end-to-end and returns its outcome.
// headerID/tsHeader/sigHeader are the Standard Webhooks headers.
func (e *Engine) Process(headerID, tsHeader, sigHeader string, body []byte) Outcome {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.stats.Deliveries++
	bodyHash := store.PayloadHash(body)

	// 1. Everything is ledgered, even what we are about to reject: the
	// ledger is the evidence, not the highlight reel.
	e.appendLedger(ledger.KindReceived, headerID, bodyHash, "")

	// 2. Signature replay: the exact signature bytes were already
	// presented once. A legitimate provider retry re-signs with a fresh
	// timestamp, so byte-identical signatures are replays, not retries.
	if sigHeader != "" && e.seenSig[sigHeader] {
		e.stats.RejectedSigReplay++
		e.appendLedger(ledger.KindRejected, headerID, bodyHash, ReasonSignatureReplay)
		return Outcome{HTTPStatus: 400, Result: "rejected", Reason: ReasonSignatureReplay, EventID: headerID}
	}

	// 3. Cryptographic verification + timestamp tolerance.
	if err := sign.Verify(e.cfg.Secret, headerID, tsHeader, sigHeader, body, e.cfg.Now(), e.cfg.Tolerance); err != nil {
		reason := ReasonBadSignature
		if err == sign.ErrTimestampTooOld || err == sign.ErrTimestampTooNew {
			reason = ReasonStaleTimestamp
			e.stats.RejectedStale++
		} else {
			e.stats.RejectedBadSig++
		}
		e.appendLedger(ledger.KindRejected, headerID, bodyHash, reason+": "+err.Error())
		return Outcome{HTTPStatus: 400, Result: "rejected", Reason: reason, EventID: headerID}
	}
	if sigHeader != "" {
		e.seenSig[sigHeader] = true
	}

	// 4. Structural parse. Bad JSON never reaches dedup or the handler.
	ev, err := store.ParseEvent(body)
	if err != nil {
		e.stats.RejectedMalformed++
		e.appendLedger(ledger.KindRejected, headerID, bodyHash, ReasonMalformedJSON+": "+err.Error())
		return Outcome{HTTPStatus: 400, Result: "rejected", Reason: ReasonMalformedJSON, EventID: headerID}
	}
	e.payloads[bodyHash] = append([]byte(nil), body...)
	e.appendLedger(ledger.KindVerified, ev.ID, bodyHash, "")

	// 5. Idempotency + conflict gate, keyed by event ID + payload hash.
	if rec, seen := e.records[ev.ID]; seen {
		if rec.PayloadHash != bodyHash {
			e.stats.Conflicts++
			e.appendLedger(ledger.KindConflict, ev.ID, bodyHash,
				fmt.Sprintf("%s: first payload %s, this payload %s", ReasonPayloadConflict, rec.PayloadHash, bodyHash))
			e.deadLetter(ev.ID, bodyHash, ReasonPayloadConflict,
				"same event ID as an earlier delivery with a different payload; first payload wins, this one is never applied", 0)
			return Outcome{HTTPStatus: 409, Result: "conflict", Reason: ReasonPayloadConflict, EventID: ev.ID}
		}
		e.stats.DuplicatesSuppressed++
		e.appendLedger(ledger.KindDuplicateSuppressed, ev.ID, bodyHash, "status="+rec.Status)
		return Outcome{HTTPStatus: 200, Result: "duplicate", EventID: ev.ID}
	}

	// 6. Apply with exponential-backoff retries on transient failure.
	attempts := 0
	var lastErr error
	for attempts < e.cfg.MaxAttempts {
		attempts++
		e.stats.HandlerAttempts++
		if attempts > 1 {
			e.stats.HandlerRetries++
		}
		lastErr = e.applyOnce(ev)
		if lastErr == nil {
			break
		}
		e.appendLedger(ledger.KindHandlerFailed, ev.ID, bodyHash, fmt.Sprintf("attempt %d: %v", attempts, lastErr))
		if attempts < e.cfg.MaxAttempts {
			delay := e.cfg.Backoff.Delay(attempts)
			e.appendLedger(ledger.KindRetryScheduled, ev.ID, bodyHash, fmt.Sprintf("attempt %d failed; retry in %s", attempts, delay))
			e.cfg.Sleep(delay)
		}
	}
	if lastErr != nil {
		e.records[ev.ID] = &eventRecord{PayloadHash: bodyHash, Status: "dead_lettered"}
		e.appendLedger(ledger.KindDeadLettered, ev.ID, bodyHash, ReasonHandlerExhausted+": "+lastErr.Error())
		e.deadLetter(ev.ID, bodyHash, ReasonHandlerExhausted, lastErr.Error(), attempts)
		return Outcome{HTTPStatus: 500, Result: "dead_lettered", Reason: ReasonHandlerExhausted, EventID: ev.ID, Attempts: attempts}
	}

	// 7. Applied: ledger first, then a signed receipt bound to the
	// applied entry's sequence and hash.
	applied := e.appendLedger(ledger.KindApplied, ev.ID, bodyHash, fmt.Sprintf("attempts=%d", attempts))
	rcpt := Receipt{
		EventID:     ev.ID,
		PayloadHash: bodyHash,
		LedgerSeq:   applied.Seq,
		LedgerHash:  applied.Hash,
		AppliedAt:   e.cfg.Now().UTC(),
	}
	rcpt.Signature = sign.HMACBase64(e.cfg.Secret, rcpt.Canonical())
	e.receipt[ev.ID] = rcpt
	e.records[ev.ID] = &eventRecord{PayloadHash: bodyHash, Status: "applied", ReceiptSeq: applied.Seq}
	e.stats.Applied++
	e.stats.ReceiptsIssued++
	e.appendLedger(ledger.KindReceiptIssued, ev.ID, bodyHash, fmt.Sprintf("receipt for seq %d", applied.Seq))
	if e.cfg.DataDir != "" {
		line, _ := json.Marshal(rcpt)
		f, err := os.OpenFile(filepath.Join(e.cfg.DataDir, "receipts.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err == nil {
			f.Write(append(line, '\n'))
			f.Close()
		}
		e.persistEffects()
	}
	return Outcome{HTTPStatus: 200, Result: "applied", EventID: ev.ID, Attempts: attempts}
}

// applyOnce performs a single handler attempt: the built-in store fold
// plus the optional business handler. The store fold happens only when
// the business handler succeeds, so a failed attempt leaves no partial
// effect. NOTE: with a real external handler the engine assumes the
// handler itself is retry-safe (documented contract).
func (e *Engine) applyOnce(ev store.Event) error {
	if e.cfg.Handler != nil {
		if err := e.cfg.Handler(ev); err != nil {
			return err
		}
	}
	e.store.Apply(ev)
	return nil
}

func (e *Engine) persistEffects() {
	if e.cfg.DataDir == "" {
		return
	}
	tmp := filepath.Join(e.cfg.DataDir, "effects.json.tmp")
	if err := os.WriteFile(tmp, e.store.Canonical(), 0o644); err == nil {
		os.Rename(tmp, filepath.Join(e.cfg.DataDir, "effects.json"))
	}
}

// PersistPayloads writes the retained payload store to payloads.jsonl so
// the replay tool can work from disk after the process exits.
func (e *Engine) PersistPayloads() error {
	if e.cfg.DataDir == "" {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	f, err := os.Create(filepath.Join(e.cfg.DataDir, "payloads.jsonl"))
	if err != nil {
		return err
	}
	defer f.Close()
	for hash, body := range e.payloads {
		line, _ := json.Marshal(map[string]string{"payload_hash": hash, "body": string(body)})
		if _, err := f.Write(append(line, '\n')); err != nil {
			return err
		}
	}
	return nil
}

// Handler returns the HTTP handler: POST /webhooks (receiver), and
// GET /receipts/{eventID} so a provider can fetch its receipt.
func (e *Engine) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhooks", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
		if err != nil {
			http.Error(w, "unreadable body", http.StatusBadRequest)
			return
		}
		out := e.Process(
			r.Header.Get("webhook-id"),
			r.Header.Get("webhook-timestamp"),
			r.Header.Get("webhook-signature"),
			body,
		)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(out.HTTPStatus)
		json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("GET /receipts/{eventID}", func(w http.ResponseWriter, r *http.Request) {
		rcpt, ok := e.ReceiptFor(r.PathValue("eventID"))
		if !ok {
			http.Error(w, "no receipt for event", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(rcpt)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	return mux
}
