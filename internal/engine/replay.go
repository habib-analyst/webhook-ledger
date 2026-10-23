package engine

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	"webhookledger/internal/ledger"
	"webhookledger/internal/store"
)

// ReplayResult summarises an effect-replay run.
type ReplayResult struct {
	AppliedEntries int      `json:"applied_entries"`
	ReplayEffects  int      `json:"replay_effects"`
	LiveEffects    int      `json:"live_effects"`
	Identical      bool     `json:"identical"`
	Diffs          []string `json:"diffs,omitempty"`
}

// ReplayEffects rebuilds an effect store from ledger "applied" entries
// alone: for each, in sequence order, it looks up the retained payload by
// hash and folds it into a fresh store. If the ledger truly determines
// live state, the rebuilt store is byte-identical to the live one.
func ReplayEffects(entries []ledger.Entry, payloads map[string][]byte) (*store.Store, int, error) {
	fresh := store.New()
	applied := 0
	for _, ent := range entries {
		if ent.Kind != ledger.KindApplied {
			continue
		}
		body, ok := payloads[ent.PayloadHash]
		if !ok {
			return nil, applied, fmt.Errorf("ledger seq %d: payload %s not retained", ent.Seq, ent.PayloadHash)
		}
		ev, err := store.ParseEvent(body)
		if err != nil {
			return nil, applied, fmt.Errorf("ledger seq %d: %w", ent.Seq, err)
		}
		if ev.ID != ent.EventID {
			return nil, applied, fmt.Errorf("ledger seq %d: payload event %s != ledger event %s", ent.Seq, ev.ID, ent.EventID)
		}
		fresh.Apply(ev)
		applied++
	}
	return fresh, applied, nil
}

// DiffStores compares a replayed store against the live store.
func DiffStores(replayed, live *store.Store) ReplayResult {
	diffs := store.Diff(replayed, live)
	return ReplayResult{
		ReplayEffects: replayed.EffectCount(),
		LiveEffects:   live.EffectCount(),
		Identical:     len(diffs) == 0 && bytes.Equal(replayed.Canonical(), live.Canonical()),
		Diffs:         diffs,
	}
}

// VerifyAllReceipts checks every receipt three ways: HMAC signature under
// the shared secret; the referenced ledger entry exists, is an "applied"
// entry for the same event and payload; and its stored hash equals the
// hash the receipt commits to. Returns the count verified and any
// failures described.
func VerifyAllReceipts(secret []byte, receipts []Receipt, entries []ledger.Entry) (int, []string) {
	bySeq := map[int64]ledger.Entry{}
	for _, e := range entries {
		bySeq[e.Seq] = e
	}
	ok := 0
	var bad []string
	for _, r := range receipts {
		ent, exists := bySeq[r.LedgerSeq]
		switch {
		case !VerifyReceipt(secret, r):
			bad = append(bad, fmt.Sprintf("event %s: receipt signature invalid", r.EventID))
		case !exists:
			bad = append(bad, fmt.Sprintf("event %s: ledger seq %d not found", r.EventID, r.LedgerSeq))
		case ent.Kind != ledger.KindApplied || ent.EventID != r.EventID || ent.PayloadHash != r.PayloadHash:
			bad = append(bad, fmt.Sprintf("event %s: ledger seq %d does not record this application", r.EventID, r.LedgerSeq))
		case ent.Hash != r.LedgerHash:
			bad = append(bad, fmt.Sprintf("event %s: receipt ledger hash != ledger entry hash at seq %d", r.EventID, r.LedgerSeq))
		default:
			ok++
		}
	}
	return ok, bad
}

// LoadPayloadsFile reads a payloads.jsonl written by Engine.PersistPayloads.
func LoadPayloadsFile(path string) (map[string][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := map[string][]byte{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var rec struct {
			PayloadHash string `json:"payload_hash"`
			Body        string `json:"body"`
		}
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			return nil, err
		}
		out[rec.PayloadHash] = []byte(rec.Body)
	}
	return out, sc.Err()
}

// LoadReceiptsFile reads a receipts.jsonl written by the engine.
func LoadReceiptsFile(path string) ([]Receipt, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Receipt
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var r Receipt
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, sc.Err()
}
