// Package ledger is an append-only, hash-chained record of every delivery
// state transition. Each entry commits to the hash of the previous entry,
// so any later edit, deletion, or reordering of history breaks the chain
// and is detected by Verify. Entries are persisted as JSON Lines.
package ledger

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// Entry kinds recorded in the ledger.
const (
	KindReceived            = "received"
	KindVerified            = "verified"
	KindRejected            = "rejected"
	KindApplied             = "applied"
	KindDuplicateSuppressed = "duplicate_suppressed"
	KindConflict            = "conflict"
	KindHandlerFailed       = "handler_failed"
	KindRetryScheduled      = "retry_scheduled"
	KindDeadLettered        = "dead_lettered"
	KindReceiptIssued       = "receipt_issued"
)

// Genesis is the prev-hash of the first entry.
const Genesis = "0000000000000000000000000000000000000000000000000000000000000000"

// Entry is one ledger record. Hash is computed over the canonical string
// seq|ts|kind|eventID|payloadHash|detail|prevHash.
type Entry struct {
	Seq         int64     `json:"seq"`
	TS          time.Time `json:"ts"`
	Kind        string    `json:"kind"`
	EventID     string    `json:"event_id"`
	PayloadHash string    `json:"payload_hash"`
	Detail      string    `json:"detail,omitempty"`
	PrevHash    string    `json:"prev_hash"`
	Hash        string    `json:"hash"`
}

func computeHash(e Entry) string {
	h := sha256.New()
	fmt.Fprintf(h, "%d|%s|%s|%s|%s|%s|%s",
		e.Seq, e.TS.UTC().Format(time.RFC3339Nano), e.Kind, e.EventID, e.PayloadHash, e.Detail, e.PrevHash)
	return hex.EncodeToString(h.Sum(nil))
}

// Ledger is a concurrency-safe append-only chain backed by a JSONL file.
// A nil file means in-memory only (tests, storm dry runs still persist via
// an explicit path in practice).
type Ledger struct {
	mu      sync.Mutex
	entries []Entry
	f       *os.File
	now     func() time.Time
}

// Open loads an existing ledger file (verifying the chain as it reads) or
// creates a new one. Pass "" for an in-memory ledger.
func Open(path string) (*Ledger, error) {
	l := &Ledger{now: time.Now}
	if path == "" {
		return l, nil
	}
	if existing, err := os.ReadFile(path); err == nil && len(existing) > 0 {
		entries, err := Parse(existing)
		if err != nil {
			return nil, fmt.Errorf("loading ledger %s: %w", path, err)
		}
		if err := VerifyEntries(entries); err != nil {
			return nil, fmt.Errorf("loading ledger %s: %w", path, err)
		}
		l.entries = entries
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	l.f = f
	return l, nil
}

// SetClock overrides the timestamp source (tests / storm determinism).
func (l *Ledger) SetClock(now func() time.Time) { l.now = now }

// Append adds one entry and returns it (with Seq, PrevHash and Hash set).
func (l *Ledger) Append(kind, eventID, payloadHash, detail string) (Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	prev := Genesis
	if n := len(l.entries); n > 0 {
		prev = l.entries[n-1].Hash
	}
	e := Entry{
		Seq:         int64(len(l.entries)),
		TS:          l.now().UTC(),
		Kind:        kind,
		EventID:     eventID,
		PayloadHash: payloadHash,
		Detail:      detail,
		PrevHash:    prev,
	}
	e.Hash = computeHash(e)
	if l.f != nil {
		line, err := json.Marshal(e)
		if err != nil {
			return Entry{}, err
		}
		if _, err := l.f.Write(append(line, '\n')); err != nil {
			return Entry{}, err
		}
		if err := l.f.Sync(); err != nil {
			return Entry{}, err
		}
	}
	l.entries = append(l.entries, e)
	return e, nil
}

// Entries returns a copy of the full chain.
func (l *Ledger) Entries() []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Entry, len(l.entries))
	copy(out, l.entries)
	return out
}

// Head returns the hash of the most recent entry, or Genesis when empty.
func (l *Ledger) Head() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) == 0 {
		return Genesis
	}
	return l.entries[len(l.entries)-1].Hash
}

// Close flushes and closes the backing file, if any.
func (l *Ledger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	return l.f.Close()
}

// Parse decodes JSONL ledger bytes.
func Parse(data []byte) ([]Entry, error) {
	var entries []Entry
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e Entry
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNo, err)
		}
		entries = append(entries, e)
	}
	return entries, sc.Err()
}

// VerifyFile loads a ledger file and verifies its chain.
func VerifyFile(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	entries, err := Parse(data)
	if err != nil {
		return 0, err
	}
	if err := VerifyEntries(entries); err != nil {
		return len(entries), err
	}
	return len(entries), nil
}

var (
	ErrSeqGap    = errors.New("ledger sequence gap")
	ErrPrevLink  = errors.New("ledger prev-hash link broken")
	ErrHashValue = errors.New("ledger entry hash mismatch (content edited)")
)

// VerifyEntries recomputes the whole chain: sequence continuity, prev-hash
// linkage, and each entry's hash over its content.
func VerifyEntries(entries []Entry) error {
	prev := Genesis
	for i, e := range entries {
		if e.Seq != int64(i) {
			return fmt.Errorf("entry %d: %w (seq=%d)", i, ErrSeqGap, e.Seq)
		}
		if e.PrevHash != prev {
			return fmt.Errorf("entry %d: %w", i, ErrPrevLink)
		}
		if computeHash(e) != e.Hash {
			return fmt.Errorf("entry %d: %w", i, ErrHashValue)
		}
		prev = e.Hash
	}
	return nil
}
