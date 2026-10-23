package store

import (
	"bytes"
	"testing"
)

func mkEvent(id, typ, order string, amount int64, item string) Event {
	var ev Event
	ev.ID = id
	ev.Type = typ
	ev.Data.OrderID = order
	ev.Data.AmountCents = amount
	ev.Data.Item = item
	return ev
}

func TestCanonicalOrderInsensitive(t *testing.T) {
	a := New()
	a.Apply(mkEvent("evt_1", TypeOrderCreated, "ord_1", 1000, "widget"))
	a.Apply(mkEvent("evt_2", TypeOrderPaid, "ord_1", 1000, ""))
	a.Apply(mkEvent("evt_3", TypeOrderPaid, "ord_1", 250, ""))

	b := New()
	b.Apply(mkEvent("evt_3", TypeOrderPaid, "ord_1", 250, ""))
	b.Apply(mkEvent("evt_1", TypeOrderCreated, "ord_1", 1000, "widget"))
	b.Apply(mkEvent("evt_2", TypeOrderPaid, "ord_1", 1000, ""))

	if !bytes.Equal(a.Canonical(), b.Canonical()) {
		t.Fatalf("same event set in different order must be byte-identical:\n%s\n%s", a.Canonical(), b.Canonical())
	}
	if diffs := Diff(a, b); len(diffs) != 0 {
		t.Fatalf("diff must be empty, got %v", diffs)
	}
	o := a.Orders["ord_1"]
	if o.TotalPaid != 1250 || o.Status() != "paid" || a.EffectCount() != 3 {
		t.Fatalf("unexpected state: %+v", o)
	}
}

func TestDiffDetectsDifference(t *testing.T) {
	a := New()
	a.Apply(mkEvent("evt_1", TypeOrderPaid, "ord_1", 100, ""))
	b := New()
	b.Apply(mkEvent("evt_1", TypeOrderPaid, "ord_1", 100, ""))
	b.Apply(mkEvent("evt_1", TypeOrderPaid, "ord_1", 100, "")) // double-applied
	if diffs := Diff(a, b); len(diffs) != 1 {
		t.Fatalf("double-apply must produce exactly 1 diff, got %v", diffs)
	}
	c := New()
	if diffs := Diff(a, c); len(diffs) != 1 {
		t.Fatalf("missing order must produce 1 diff, got %v", diffs)
	}
}

func TestParseEventValidation(t *testing.T) {
	if _, err := ParseEvent([]byte(`{"id":`)); err == nil {
		t.Fatal("truncated JSON must fail")
	}
	if _, err := ParseEvent([]byte(`{"type":"order.paid","data":{"order_id":"o1"}}`)); err == nil {
		t.Fatal("missing event id must fail")
	}
	if _, err := ParseEvent([]byte(`{"id":"e1","type":"order.exploded","data":{"order_id":"o1"}}`)); err == nil {
		t.Fatal("unknown type must fail")
	}
	ev, err := ParseEvent([]byte(`{"id":"e1","type":"order.paid","data":{"order_id":"o1","amount_cents":42}}`))
	if err != nil || ev.Data.AmountCents != 42 {
		t.Fatalf("valid event must parse: %v %+v", err, ev)
	}
}

func TestPayloadHashStable(t *testing.T) {
	h1 := PayloadHash([]byte(`{"a":1}`))
	h2 := PayloadHash([]byte(`{"a":1}`))
	h3 := PayloadHash([]byte(`{"a":2}`))
	if h1 != h2 || h1 == h3 {
		t.Fatal("payload hash must be content-determined")
	}
}
