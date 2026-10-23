// webhook-ledger: an idempotent webhook reconciliation engine with a
// signed, hash-chained receipt ledger and effect-replay tooling.
//
// Commands:
//
//	serve          run the HTTP receiver (POST /webhooks, GET /receipts/{id})
//	storm          run the delivery-storm harness and write results.json
//	verify-ledger  re-check the hash chain of a data dir's ledger.jsonl
//	verify-receipts check every receipt in a data dir against secret + ledger
//	replay         rebuild effects from the ledger and diff against live state
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"webhookledger/internal/engine"
	"webhookledger/internal/ledger"
	"webhookledger/internal/sign"
	"webhookledger/internal/store"
	"webhookledger/internal/storm"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = cmdServe(os.Args[2:])
	case "storm":
		err = cmdStorm(os.Args[2:])
	case "verify-ledger":
		err = cmdVerifyLedger(os.Args[2:])
	case "verify-receipts":
		err = cmdVerifyReceipts(os.Args[2:])
	case "replay":
		err = cmdReplay(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: webhook-ledger <serve|storm|verify-ledger|verify-receipts|replay> [flags]")
}

func secretFlag(fs *flag.FlagSet) *string {
	return fs.String("secret", "", "shared webhook secret (or whsec_... form); falls back to WEBHOOK_SECRET env")
}

func resolveSecret(s string) []byte {
	if s == "" {
		s = os.Getenv("WEBHOOK_SECRET")
	}
	if s == "" {
		s = "whsec_dGVzdC1zZWNyZXQtdGVzdC1zZWNyZXQtdGVzdA==" // documented dev default, never for production
		fmt.Fprintln(os.Stderr, "warning: using the built-in dev secret; set -secret or WEBHOOK_SECRET")
	}
	return sign.SecretBytes(s)
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "listen address")
	dataDir := fs.String("data", "data", "data directory")
	sec := secretFlag(fs)
	fs.Parse(args)

	eng, err := engine.New(engine.Config{
		Secret:      resolveSecret(*sec),
		DataDir:     *dataDir,
		MaxAttempts: 5,
		Backoff:     engine.Backoff{Base: 100 * time.Millisecond, Cap: 30 * time.Second, Rand: rand.New(rand.NewSource(time.Now().UnixNano()))},
	})
	if err != nil {
		return err
	}
	fmt.Printf("webhook-ledger receiver on %s (data dir %s)\n", *addr, *dataDir)
	fmt.Println("POST /webhooks   GET /receipts/{eventID}   GET /healthz")
	return http.ListenAndServe(*addr, eng.Handler())
}

func cmdStorm(args []string) error {
	fs := flag.NewFlagSet("storm", flag.ExitOnError)
	seed := fs.Int64("seed", 42, "storm RNG seed")
	events := fs.Int("events", 240, "unique events")
	out := fs.String("out", "storm-out", "output data directory")
	sec := secretFlag(fs)
	fs.Parse(args)

	res, err := storm.Run(storm.Params{Seed: *seed, UniqueEvents: *events}, *out, resolveSecret(*sec))
	if err != nil {
		return err
	}
	blob, _ := json.MarshalIndent(res, "", "  ")
	if err := os.WriteFile(filepath.Join(*out, "results.json"), blob, 0o644); err != nil {
		return err
	}
	fmt.Println(string(blob))
	if !res.EngineOracleIdentical || !res.ChainValid || !res.ReplayIdentical || len(res.ReceiptFailures) > 0 {
		return fmt.Errorf("storm verification failed: see results.json")
	}
	return nil
}

func cmdVerifyLedger(args []string) error {
	fs := flag.NewFlagSet("verify-ledger", flag.ExitOnError)
	dataDir := fs.String("data", "data", "data directory")
	fs.Parse(args)
	n, err := ledger.VerifyFile(filepath.Join(*dataDir, "ledger.jsonl"))
	if err != nil {
		return fmt.Errorf("ledger verification FAILED after %d entries: %w", n, err)
	}
	fmt.Printf("ledger OK: %d entries, hash chain intact\n", n)
	return nil
}

func cmdVerifyReceipts(args []string) error {
	fs := flag.NewFlagSet("verify-receipts", flag.ExitOnError)
	dataDir := fs.String("data", "data", "data directory")
	sec := secretFlag(fs)
	fs.Parse(args)

	data, err := os.ReadFile(filepath.Join(*dataDir, "ledger.jsonl"))
	if err != nil {
		return err
	}
	entries, err := ledger.Parse(data)
	if err != nil {
		return err
	}
	if err := ledger.VerifyEntries(entries); err != nil {
		return fmt.Errorf("ledger chain invalid: %w", err)
	}
	receipts, err := engine.LoadReceiptsFile(filepath.Join(*dataDir, "receipts.jsonl"))
	if err != nil {
		return err
	}
	ok, bad := engine.VerifyAllReceipts(resolveSecret(*sec), receipts, entries)
	for _, b := range bad {
		fmt.Fprintln(os.Stderr, "FAIL:", b)
	}
	fmt.Printf("receipts: %d verified, %d failed (of %d)\n", ok, len(bad), len(receipts))
	if len(bad) > 0 {
		return fmt.Errorf("%d receipts failed verification", len(bad))
	}
	return nil
}

func cmdReplay(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	dataDir := fs.String("data", "data", "data directory")
	fs.Parse(args)

	data, err := os.ReadFile(filepath.Join(*dataDir, "ledger.jsonl"))
	if err != nil {
		return err
	}
	entries, err := ledger.Parse(data)
	if err != nil {
		return err
	}
	if err := ledger.VerifyEntries(entries); err != nil {
		return fmt.Errorf("ledger chain invalid: %w", err)
	}
	payloads, err := engine.LoadPayloadsFile(filepath.Join(*dataDir, "payloads.jsonl"))
	if err != nil {
		return err
	}
	replayed, applied, err := engine.ReplayEffects(entries, payloads)
	if err != nil {
		return err
	}
	liveBytes, err := os.ReadFile(filepath.Join(*dataDir, "effects.json"))
	if err != nil {
		return err
	}
	live := store.New()
	// effects.json is the canonical form; rebuild a comparable store by
	// re-applying the live store's own recorded applied events is not
	// possible from canonical bytes alone, so compare canonical bytes
	// directly: replayed canonical vs persisted canonical.
	_ = live
	if string(replayed.Canonical()) == string(liveBytes) {
		fmt.Printf("replay OK: %d applied entries re-derived, effect store byte-identical to live state\n", applied)
		return nil
	}
	return fmt.Errorf("replay MISMATCH: re-derived effects differ from persisted live state")
}
