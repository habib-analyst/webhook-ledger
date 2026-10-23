# webhook-ledger

**An inbound webhook reconciliation engine whose correctness is proven, not claimed — with a signed, hash-chained receipt ledger and effect-replay tooling.**

Every webhook receiver dedups "best effort" and logs to files the operator can rewrite. When a provider disputes *"you never processed event X"* — or your own audit asks *"did we apply event X twice, and which payload?"* — a log is an opinion. webhook-ledger makes it a proof:

- **Storm-proven reconciliation.** A built-in fault-injection harness delivers a scripted event set under duplicates, reordering, poison deliveries and handler failures, then diffs the live effect store against a ground-truth oracle. Measured result on the published storm: **0 mismatched orders**, where a naive no-dedup processor under the *identical* storm mismatches **54 of 60 orders** (76 double-applies, 101 dropped deliveries).
- **Signed receipt ledger.** Every delivery state transition is appended to a hash-chained ledger. Every applied event gets an HMAC-signed **receipt** binding event ID + payload hash + ledger sequence + ledger entry hash. Anyone holding the provider secret can verify exactly what was applied, in what order — and any later edit to the ledger breaks the chain.
- **Effect replay with byte-identical diff.** The replay tool rebuilds the downstream effect store from the ledger's `applied` entries alone and diffs it against live state. If the ledger truly determines your state, the two are byte-identical — checked, not assumed.

## Quickstart

```bash
go build -o webhook-ledger ./cmd/webhook-ledger

# Run the delivery storm (the measured core) and write results.json
./webhook-ledger storm -seed 42 -out storm-out

# Verify the evidence afterwards, offline
./webhook-ledger verify-ledger   -data storm-out
./webhook-ledger verify-receipts -data storm-out
./webhook-ledger replay          -data storm-out

# Run the receiver
WEBHOOK_SECRET='whsec_...' ./webhook-ledger serve -addr :8080 -data data
# POST /webhooks  (Standard Webhooks headers: webhook-id, webhook-timestamp, webhook-signature)
# GET  /receipts/{eventID}
```

Standard library only. No external modules, no database server — the ledger is a JSONL file with fsync-on-append.

## How it works

```
provider ──► POST /webhooks ──► ledger: received
                                 │
                    signature replay? ──► rejected (ledgered)
                    HMAC + timestamp tolerance? ──► rejected (ledgered)
                    malformed JSON? ──► rejected (ledgered)
                                 │ verified (ledgered)
                    known event ID?
                      ├─ same payload hash ──► duplicate_suppressed (ledgered), HTTP 200
                      └─ different payload ──► conflict (ledgered) + DLQ(payload_conflict), HTTP 409
                                 │ new event
                    handler with exponential backoff + jitter
                      ├─ success ──► applied (ledgered) + signed receipt (ledgered)
                      └─ exhausted ──► dead_lettered (ledgered) + DLQ(handler_exhausted)
```

**Semantics that matter:**

- **Idempotency key = event ID + payload hash.** Same ID, same bytes → duplicate, acked with 200 so the provider stops retrying. Same ID, *different bytes* → a provider-side bug or an attack: the first payload wins, the conflicting one is **never silently applied** — it is dead-lettered with reason `payload_conflict` for a human to reconcile.
- **Signature replays are not retries.** A legitimate retry re-signs with a fresh timestamp. A byte-identical signature presented twice is a captured-request replay and is rejected as such, separately from timestamp tolerance.
- **Everything is ledgered, including rejections.** The ledger is the evidence trail for what arrived, not a highlight reel of what succeeded.
- **Receipts are verifiable by the provider.** A receipt commits to the ledger entry that recorded the application. `verify-receipts` checks the HMAC, the referenced entry's kind/event/payload, and the entry hash — three ways, so a forged or re-pointed receipt fails.

## The storm (seed 42, published `results.json`)

| | Engine | Naive processor |
|---|---|---|
| Unique events / orders | 240 / 60 | 240 / 60 |
| Deliveries (30% duplicates, 5% poison) | 369 | 369 (identical script) |
| Applied | 239 (+1 dead-lettered by design) | 255 applications |
| Duplicates suppressed | **110 / 110** | 0 (76 double-applies) |
| Payload conflicts caught | **4 / 4** | 0 (silently applied) |
| Poison rejected (bad sig / malformed / replay / stale) | 7 / 4 / 2 / 2 | — |
| Handler attempts / retries | 363 / 123 | no retries (101 deliveries dropped) |
| **Orders mismatched vs oracle** | **0** | **54 / 60** |
| Receipt chain verified | 239 / 239 | n/a |
| Replay vs live state | byte-identical | n/a |

Faults injected: 30% duplicate deliveries, delivery order shuffled within a window of 24, 5% poison (bad signatures, malformed JSON, conflicting payloads under known IDs, exact-signature replays, stale timestamps), and transient handler failures (25% of events fail their first attempt, 10% fail their first two, one event fails forever and must dead-letter). Multi-hour provider retry delays are compressed to seconds — retries arrive re-signed with fresh timestamps, which is the only property a receiver can observe.

## Repository layout

```
cmd/webhook-ledger/   CLI: serve | storm | verify-ledger | verify-receipts | replay
internal/sign/        Standard Webhooks signing/verification (HMAC-SHA256, rotation, tolerance)
internal/ledger/      append-only hash-chained ledger (JSONL, fsync, verify)
internal/store/       event format + effect store (order-insensitive, canonical bytes)
internal/engine/      receiver + idempotent processor + backoff + DLQ + receipts + replay
internal/engine/      naive.go — the no-dedup contrast processor
internal/storm/       fault-injection harness + ground-truth oracle
results.json          measured output of the seed-42 storm
```

## Honest limits

- **Single node.** Processing is serialised by design (that total order is what makes receipts and replay well-defined). No clustering, no consensus.
- **HMAC receipts are symmetric.** They convince holders of the shared secret (you and the provider) — they are not public non-repudiation. The hash chain proves internal consistency and tamper-evidence; without an externally anchored head hash, a full-history rewrite is detectable only against receipts/head hashes shared beforehand.
- **Payload identity is byte identity.** Providers retry byte-identical bodies; a semantically equal but reformatted payload under the same ID is treated as a conflict. That is deliberate, and it is a choice, not a law of nature.
- **Simulated provider, synthetic effects.** The storm's provider and order effects are models. The reconciliation machinery is real; your handler semantics will differ.
- **Durability over throughput.** The ledger fsyncs every append; the 369-delivery storm takes ~40 s wall on a 2-core VM, dominated by fsync and injected backoff. This is a correctness artifact, not a throughput benchmark.

## Tests

`go test ./...` — 32 tests, also clean under `-race`: signature verification (tamper, rotation, stale/future, `whsec_` secrets), ledger tamper/deletion/reorder detection, dedup, conflict-to-DLQ, signature replay, backoff schedule, receipt verification and tampering, replay identity, HTTP end-to-end, and a small-storm regression test that fails if the naive processor ever starts passing.

See `PAPER.md` for the design write-up and `KILLGATE.md` for the prior-art analysis (Svix, Hookdeck, Convoy, Standard Webhooks) this project differentiates from.
