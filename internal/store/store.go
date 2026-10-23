// Package store holds the downstream effect state produced by applying
// webhook events, plus the wire event format. Effects are deliberately
// defined so that the final state is a function of the SET of applied
// events, not their arrival order: out-of-order delivery is a transport
// fault the engine must absorb, and the replay tool depends on
// order-insensitive re-derivation to compare byte-for-byte.
package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// Event types understood by the effect handler.
const (
	TypeOrderCreated  = "order.created"
	TypeOrderPaid     = "order.paid"
	TypeOrderRefunded = "order.refunded"
	TypeOrderCanceled = "order.canceled"
)

// Event is the provider payload. ID is the provider event ID: stable
// across every retry of the same event.
type Event struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Data struct {
		OrderID     string `json:"order_id"`
		AmountCents int64  `json:"amount_cents"`
		Item        string `json:"item,omitempty"`
	} `json:"data"`
}

// ParseEvent decodes a raw payload. It returns an error for malformed
// JSON and for structurally incomplete events.
func ParseEvent(body []byte) (Event, error) {
	var ev Event
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(&ev); err != nil {
		return Event{}, fmt.Errorf("malformed JSON: %w", err)
	}
	if ev.ID == "" {
		return Event{}, fmt.Errorf("event missing id")
	}
	if ev.Data.OrderID == "" {
		return Event{}, fmt.Errorf("event %s missing data.order_id", ev.ID)
	}
	switch ev.Type {
	case TypeOrderCreated, TypeOrderPaid, TypeOrderRefunded, TypeOrderCanceled:
	default:
		return Event{}, fmt.Errorf("event %s has unknown type %q", ev.ID, ev.Type)
	}
	return ev, nil
}

// PayloadHash is the SHA-256 of the exact raw body bytes. Providers retry
// byte-identical bodies; a different byte string under the same event ID
// is by definition a different payload, hence a conflict.
func PayloadHash(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// OrderState is the accumulated effect of all events for one order.
type OrderState struct {
	OrderID       string   `json:"order_id"`
	CreatedAmount int64    `json:"created_amount_cents"`
	Item          string   `json:"item,omitempty"`
	TotalPaid     int64    `json:"total_paid_cents"`
	TotalRefunded int64    `json:"total_refunded_cents"`
	Canceled      bool     `json:"canceled"`
	AppliedEvents []string `json:"applied_events"` // kept sorted
}

// Status derives the order status from accumulated facts.
func (o *OrderState) Status() string {
	switch {
	case o.Canceled:
		return "canceled"
	case o.TotalPaid > 0:
		return "paid"
	default:
		return "created"
	}
}

// Store is the effect store: order ID -> accumulated state.
type Store struct {
	Orders map[string]*OrderState `json:"orders"`
}

// New returns an empty store.
func New() *Store { return &Store{Orders: map[string]*OrderState{}} }

// Apply folds one event into the store. Applying the same event twice is
// the caller's bug (the engine dedups before calling); Apply itself is
// purely additive/set-based per event, so distinct events commute.
func (s *Store) Apply(ev Event) {
	o, ok := s.Orders[ev.Data.OrderID]
	if !ok {
		o = &OrderState{OrderID: ev.Data.OrderID}
		s.Orders[ev.Data.OrderID] = o
	}
	switch ev.Type {
	case TypeOrderCreated:
		o.CreatedAmount = ev.Data.AmountCents
		o.Item = ev.Data.Item
	case TypeOrderPaid:
		o.TotalPaid += ev.Data.AmountCents
	case TypeOrderRefunded:
		o.TotalRefunded += ev.Data.AmountCents
	case TypeOrderCanceled:
		o.Canceled = true
	}
	o.AppliedEvents = append(o.AppliedEvents, ev.ID)
	sort.Strings(o.AppliedEvents)
}

// Canonical renders the store deterministically: orders sorted by ID,
// fixed field order, no insignificant whitespace. Two stores with the
// same applied event set produce byte-identical output.
func (s *Store) Canonical() []byte {
	ids := make([]string, 0, len(s.Orders))
	for id := range s.Orders {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var buf bytes.Buffer
	buf.WriteString(`{"orders":{`)
	for i, id := range ids {
		if i > 0 {
			buf.WriteByte(',')
		}
		o := s.Orders[id]
		evs, _ := json.Marshal(o.AppliedEvents)
		if evs == nil {
			evs = []byte("[]")
		}
		item, _ := json.Marshal(o.Item)
		fmt.Fprintf(&buf, `"%s":{"order_id":"%s","status":"%s","created_amount_cents":%d,"item":%s,"total_paid_cents":%d,"total_refunded_cents":%d,"canceled":%t,"applied_events":%s}`,
			id, o.OrderID, o.Status(), o.CreatedAmount, item, o.TotalPaid, o.TotalRefunded, o.Canceled, evs)
	}
	buf.WriteString("}}")
	return buf.Bytes()
}

// EffectCount is the total number of applied events across all orders.
func (s *Store) EffectCount() int {
	n := 0
	for _, o := range s.Orders {
		n += len(o.AppliedEvents)
	}
	return n
}

// Diff describes how two stores differ, per order. An empty result means
// byte-identical canonical forms (given both were built by Apply).
func Diff(a, b *Store) []string {
	var out []string
	ids := map[string]bool{}
	for id := range a.Orders {
		ids[id] = true
	}
	for id := range b.Orders {
		ids[id] = true
	}
	sorted := make([]string, 0, len(ids))
	for id := range ids {
		sorted = append(sorted, id)
	}
	sort.Strings(sorted)
	for _, id := range sorted {
		oa, oka := a.Orders[id]
		ob, okb := b.Orders[id]
		switch {
		case !oka:
			out = append(out, fmt.Sprintf("order %s: missing in first store, present in second", id))
		case !okb:
			out = append(out, fmt.Sprintf("order %s: present in first store, missing in second", id))
		default:
			ja, _ := json.Marshal(oa)
			jb, _ := json.Marshal(ob)
			// AppliedEvents order is normalised by Apply; marshal compare is safe.
			if !bytes.Equal(ja, jb) {
				out = append(out, fmt.Sprintf("order %s: state differs: %s vs %s", id, ja, jb))
			}
		}
	}
	return out
}

// MarshalJSON makes Store serialise as its canonical form's structure for
// persistence (effects.json). Persistence uses Canonical bytes directly.
func (s *Store) MarshalJSON() ([]byte, error) {
	type plain Store
	return json.Marshal((*plain)(s))
}
