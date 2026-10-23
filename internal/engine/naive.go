package engine

import (
	"time"

	"webhookledger/internal/sign"
	"webhookledger/internal/store"
)

// Naive is the honest contrast: a minimal competent receiver of the kind
// most teams actually ship. It verifies signatures (security is not the
// variable being tested) and parses events — but it has NO dedup, NO
// conflict detection, NO signature-replay guard, and NO internal retries:
// every verified delivery is applied, and a transient handler failure
// silently drops the event. The storm runs it under identical faults to
// measure what reconciliation actually buys.
type Naive struct {
	Secret       []byte
	Store        *store.Store
	Handler      Handler // same failure-injecting handler the engine gets
	Now          func() time.Time
	Applications int // total effect applications, including double-applies
	Dropped      int // events lost to unretried handler failures
	AppliedByID  map[string]int
}

// NewNaive builds a naive processor with an empty store.
func NewNaive(secret []byte, handler Handler) *Naive {
	return &Naive{
		Secret:      secret,
		Store:       store.New(),
		Handler:     handler,
		Now:         time.Now,
		AppliedByID: map[string]int{},
	}
}

// Process mirrors Engine.Process's signature so the storm can drive both
// with the same delivery script.
func (n *Naive) Process(headerID, tsHeader, sigHeader string, body []byte) Outcome {
	if err := sign.Verify(n.Secret, headerID, tsHeader, sigHeader, body, n.Now(), sign.DefaultTolerance); err != nil {
		return Outcome{HTTPStatus: 400, Result: "rejected", Reason: "verify_failed", EventID: headerID}
	}
	ev, err := store.ParseEvent(body)
	if err != nil {
		return Outcome{HTTPStatus: 400, Result: "rejected", Reason: ReasonMalformedJSON, EventID: headerID}
	}
	if n.Handler != nil {
		if err := n.Handler(ev); err != nil {
			n.Dropped++
			return Outcome{HTTPStatus: 500, Result: "dropped", Reason: "handler_failed_no_retry", EventID: ev.ID}
		}
	}
	n.Store.Apply(ev)
	n.Applications++
	n.AppliedByID[ev.ID]++
	return Outcome{HTTPStatus: 200, Result: "applied", EventID: ev.ID, Attempts: 1}
}

// DoubleApplies counts applications beyond the first per event ID — the
// measurable damage duplicates do to a non-idempotent receiver.
func (n *Naive) DoubleApplies() int {
	d := 0
	for _, c := range n.AppliedByID {
		if c > 1 {
			d += c - 1
		}
	}
	return d
}
