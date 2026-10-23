package ledger

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendAndVerifyInMemory(t *testing.T) {
	l, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	if l.Head() != Genesis {
		t.Fatalf("empty head = %s, want genesis", l.Head())
	}
	for i, kind := range []string{KindReceived, KindVerified, KindApplied} {
		e, err := l.Append(kind, "evt_1", "hashabc", "")
		if err != nil {
			t.Fatal(err)
		}
		if e.Seq != int64(i) {
			t.Fatalf("seq = %d, want %d", e.Seq, i)
		}
	}
	if err := VerifyEntries(l.Entries()); err != nil {
		t.Fatalf("fresh chain must verify: %v", err)
	}
}

func TestTamperDetection(t *testing.T) {
	l, _ := Open("")
	l.Append(KindReceived, "evt_1", "hashabc", "")
	l.Append(KindApplied, "evt_1", "hashabc", "attempts=1")
	entries := l.Entries()
	entries[1].Detail = "attempts=999" // rewrite history
	if err := VerifyEntries(entries); !errors.Is(err, ErrHashValue) {
		t.Fatalf("edited entry: got %v, want ErrHashValue", err)
	}
}

func TestDeletionDetection(t *testing.T) {
	l, _ := Open("")
	l.Append(KindReceived, "evt_1", "h", "")
	l.Append(KindVerified, "evt_1", "h", "")
	l.Append(KindApplied, "evt_1", "h", "")
	entries := l.Entries()
	entries = append(entries[:1], entries[2:]...) // delete the middle
	if err := VerifyEntries(entries); err == nil {
		t.Fatal("deleted entry must break verification")
	}
}

func TestReorderDetection(t *testing.T) {
	l, _ := Open("")
	l.Append(KindReceived, "evt_1", "h", "")
	l.Append(KindApplied, "evt_1", "h", "")
	entries := l.Entries()
	entries[0], entries[1] = entries[1], entries[0]
	if err := VerifyEntries(entries); err == nil {
		t.Fatal("reordered entries must break verification")
	}
}

func TestFilePersistenceRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	l.Append(KindReceived, "evt_9", "ph", "")
	l.Append(KindApplied, "evt_9", "ph", "")
	l.Close()

	n, err := VerifyFile(path)
	if err != nil {
		t.Fatalf("persisted ledger must verify: %v", err)
	}
	if n != 2 {
		t.Fatalf("entries = %d, want 2", n)
	}
	// Reopening must load and continue the same chain.
	l2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	e, err := l2.Append(KindReceiptIssued, "evt_9", "ph", "")
	if err != nil {
		t.Fatal(err)
	}
	if e.Seq != 2 {
		t.Fatalf("continued seq = %d, want 2", e.Seq)
	}
	l2.Close()
	if _, err := VerifyFile(path); err != nil {
		t.Fatalf("continued chain must verify: %v", err)
	}
}

func TestOpenRejectsCorruptedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	l, _ := Open(path)
	l.Append(KindReceived, "evt_1", "h", "")
	l.Close()
	data, _ := os.ReadFile(path)
	// Flip a character inside the kind field.
	for i, b := range data {
		if b == 'r' && i > 20 {
			data[i] = 'x'
			break
		}
	}
	os.WriteFile(path, data, 0o644)
	if _, err := Open(path); err == nil {
		t.Fatal("corrupted ledger file must be rejected on open")
	}
}
