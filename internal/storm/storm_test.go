package storm

import (
	"testing"
)

// TestStormSmall is the harness's own regression test: on a small storm
// the engine must reconcile perfectly (0 oracle mismatches, valid chain,
// identical replay) while the naive processor must visibly fail — if the
// naive processor ever "passes", the storm has stopped measuring
// anything.
func TestStormSmall(t *testing.T) {
	res, err := Run(Params{Seed: 7, UniqueEvents: 40, Orders: 10}, t.TempDir(), []byte("storm-test-secret"))
	if err != nil {
		t.Fatal(err)
	}
	if !res.EngineOracleIdentical || res.EngineOracleMismatch != 0 {
		t.Fatalf("engine must reconcile exactly: mismatches=%d diffs=%v", res.EngineOracleMismatch, res.EngineOracleDiffs)
	}
	if !res.ChainValid {
		t.Fatalf("chain must verify: %s", res.ChainError)
	}
	if !res.ReplayIdentical {
		t.Fatalf("replay must be identical: %v", res.ReplayDiffs)
	}
	if len(res.ReceiptFailures) != 0 {
		t.Fatalf("receipt failures: %v", res.ReceiptFailures)
	}
	if res.Engine.Applied != res.UniqueEvents-1 { // minus the doomed event
		t.Fatalf("applied = %d, want %d", res.Engine.Applied, res.UniqueEvents-1)
	}
	if res.Engine.DuplicatesSuppressed != res.DuplicateDeliverys {
		t.Fatalf("duplicates suppressed = %d, sent = %d", res.Engine.DuplicatesSuppressed, res.DuplicateDeliverys)
	}
	if res.Engine.Conflicts != res.PoisonByKind["conflict"] {
		t.Fatalf("conflicts caught = %d, sent = %d", res.Engine.Conflicts, res.PoisonByKind["conflict"])
	}
	if res.NaiveOracleMismatch == 0 {
		t.Fatal("naive processor reconciled perfectly — the storm is not measuring anything")
	}
	if res.NaiveDoubleApplies == 0 {
		t.Fatal("naive processor must double-apply under duplicate deliveries")
	}
}
