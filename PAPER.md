# webhook-ledger: Reconciliation You Can Prove

*An inbound webhook reconciliation engine with a signed, hash-chained receipt
ledger and effect-replay verification. Design, storm methodology, and measured
results (seed 42, this repository's `results.json`).*

## 1. The problem is evidence, not delivery

Webhook infrastructure has converged on reliable *transport*: Standard
Webhooks gives every delivery a stable ID and an HMAC signature; senders retry
with exponential backoff for ~27 hours; gateways queue durably. What has not
converged is the receiver's *evidence problem*:

1. Delivery is at-least-once, so duplicates are normal — yet the standard
   receiver guidance is "store the ID somewhere and check it", and the leading
   inbound gateway's dedup is explicitly best-effort inside a 1s–1h window,
   far shorter than the sender's retry horizon. Correctness silently rests on
   each handler re-implementing the same idea, untested, per endpoint.
2. The hard cases are not duplicates. They are: the same event ID arriving
   with a **different payload** (provider bug, key compromise, or a
   re-generated event); a **captured request replayed verbatim**; poison
   deliveries interleaved with legitimate retries; handler failures that must
   not lose the event *or* apply it twice.
3. When something goes wrong, both sides hold logs the other has no reason to
   trust. Logs are operator-controlled storage; as the signed-receipts
   literature puts it, logs are not evidence.

webhook-ledger is built around a single claim: **a receiver should be able to
prove — to the provider, to an auditor, to itself — exactly what it received,
what it applied, in what order, and that its live state is exactly the
consequence of those applications.** Everything else (the engine, the storm,
the receipts, the replay tool) exists to make that claim checkable.

## 2. Design

### 2.1 Idempotency keyed by event ID + payload hash

The dedup key is the pair *(provider event ID, SHA-256 of the raw body)*.
Three cases, three outcomes:

- **New ID** → process.
- **Known ID, same hash** → duplicate. Ack 200, apply nothing. (Erroring on
  duplicates would make the provider retry forever; silently dropping them
  without a ledger entry would erase the evidence that they arrived.)
- **Known ID, different hash** → **conflict**. The first payload wins; the
  conflicting delivery is dead-lettered with reason `payload_conflict` and
  *never applied*. Most receivers have no semantics for this case at all —
  key-only dedup (Stripe-style idempotency keys without fingerprints) applies
  whichever payload arrives second's *response* to the first's *request*, or
  overwrites. A different body under a known ID is precisely the case where
  "just apply it" corrupts state and "just drop it" destroys evidence.

Payload identity is byte identity, deliberately: providers retry
byte-identical bodies, so a reformatted body under the same ID is anomalous
by construction and deserves the conflict path.

### 2.2 Signature replays are not retries

Standard Webhooks retries re-sign with a fresh timestamp, so a retry's
signature differs from the original's. A *byte-identical* signature header
presented twice is therefore never a retry — it is a captured request being
replayed inside its validity window, where timestamp tolerance cannot help.
The engine records every verified signature and rejects exact repeats with
reason `signature_replay`, before any other processing. Timestamp tolerance
(±5 min) remains as the outer defence for captures replayed after the fact.

### 2.3 The ledger records everything, and receipts bind to it

Every delivery produces ledger entries for *every* state it passes through —
`received`, `verified`/`rejected`, `applied`, `duplicate_suppressed`,
`conflict`, `handler_failed`, `retry_scheduled`, `dead_lettered`,
`receipt_issued` — in an append-only file where each entry's hash commits to
its content and to the previous entry's hash (Haber–Stornetta chaining). Any
edit, deletion, or reordering of history invalidates the chain from that point
on; `verify-ledger` recomputes it in full.

On application, the engine issues a **receipt**: HMAC-SHA256, under the
provider's own shared secret, over `eventID | payloadHash | ledgerSeq |
ledgerHash | appliedAt`. The receipt is the provider-facing proof object: the
provider (who holds the secret) can verify that this receiver applied exactly
this payload, as the ledger entry at exactly this position — and the ledger
entry's hash is itself chained, so the receipt also anchors the surrounding
history that existed at issue time. Verification is three-way (HMAC validity,
referenced entry exists with matching kind/event/payload, entry hash matches),
so a receipt cannot be re-pointed at a different history.

### 2.4 Backoff, DLQ, and honest failure semantics

Handler failures are retried internally with exponential backoff and full
jitter (base × 2^n, capped), each attempt ledgered. Exhaustion dead-letters
with reason `handler_exhausted` and the attempt count; a later provider
redelivery of that event is a duplicate of a known event, not a second bite —
recovery from the DLQ is an operator action on recorded evidence, not an
accident of retry timing. DLQ reasons are a closed taxonomy
(`payload_conflict`, `handler_exhausted`, plus rejection reasons
`bad_signature`, `stale_timestamp`, `signature_replay`, `malformed_json`), so
"it failed" is never the whole story.

### 2.5 Replay: the ledger must determine live state

The replay tool discards the live store, folds the ledger's `applied` entries
(in sequence order, payloads retrieved by hash from the retained payload
store) into a fresh store, and compares canonical bytes. Byte-identity is a
strong, falsifiable claim: it fails if any application was lost, doubled,
applied from a payload the ledger doesn't hold, or if the effect semantics
depend on arrival order. Effect semantics are therefore defined to be
order-insensitive across distinct events (accumulated facts per order:
created amount/item set once, payments/refunds accumulate, cancellation is a
flag; status is derived). This is a real constraint on handler design, stated
plainly: handlers whose effects don't commute need an ordering discipline
this tool does not provide.

Processing is serialised under one mutex on one node. That total order is
what makes "applied at sequence N" meaningful and replay deterministic. It is
also the project's clearest scaling limit (§5).

## 3. Method: the delivery storm

Claims about reconciliation are usually argued from design. Here they are
measured. The harness (`internal/storm`) generates 240 unique order events
across 60 orders, builds a ground-truth oracle by applying each unique event
exactly once (excluding one designated *doomed* event whose handler never
succeeds — its true downstream effect is the DLQ), then delivers 369
deliveries through the real HTTP receiver:

- **Duplicates (110, 29.8%)**: provider retries — same body and ID, re-signed
  with fresh timestamps. Multi-hour retry delays are compressed to seconds;
  a fresh signature is the only property of a delayed retry a receiver can
  observe, so compression does not change what is being tested.
- **Reordering**: the delivery list is shuffled within a sliding window of 24.
- **Poison (19, 5.1%)**: 7 deliveries signed with the wrong secret; 4
  malformed JSON; 4 conflicting payloads under known IDs (validly signed —
  the provider-bug case); 2 exact-signature replays; 2 stale timestamps (−2 h).
- **Handler failures**: 25% of events fail their first attempt, 10% fail
  their first two, the doomed event fails all attempts.

The **naive processor** — signature verification and JSON parsing, but no
dedup, no conflict detection, no signature-replay guard, no retries — runs
the identical delivery script against the identical oracle. It is the honest
contrast: roughly what a competent team ships when reconciliation is a
middleware afterthought.

Verification after the storm: full chain re-verification; three-way receipt
verification for every issued receipt; effect replay diffed byte-for-byte
against live state; engine and naive stores diffed per-order against the
oracle.

## 4. Measured results (seed 42)

| Metric | Engine | Naive |
|---|---|---|
| Deliveries processed | 369 | 369 |
| Events applied | 239 | 255 applications |
| Duplicates suppressed | 110 / 110 | 0 |
| Double-applies | 0 | 76 |
| Deliveries dropped on handler failure | 0 | 101 |
| Payload conflicts caught → DLQ | 4 / 4 | 0 (applied silently) |
| Bad signatures rejected | 7 / 7 | 7 / 7 (it does verify) |
| Malformed rejected | 4 / 4 | 4 / 4 |
| Signature replays rejected | 2 / 2 | 0 (re-applied) |
| Stale timestamps rejected | 2 / 2 | 2 / 2 |
| Handler attempts | 363 (123 of them retries) | 356 (single attempt each, no retries) |
| Dead-lettered | 5 (4 conflict + 1 handler-exhausted) | n/a |
| Receipts issued & verified | 239 / 239 | n/a |
| Ledger entries / chain | 1,578 / valid | n/a |
| **Oracle mismatches (orders)** | **0 / 60** | **54 / 60** |
| Replay vs live state | byte-identical | n/a |

The engine's live store is byte-identical to the oracle: every duplicate
suppressed, every conflict quarantined, every transient failure recovered by
retry, the one unrecoverable event dead-lettered with its evidence intact.
The naive processor — under *identical* faults — corrupts 90% of orders,
through exactly the mechanisms the engine exists for: double-applied payments
and refunds (76), events silently lost to unretried failures (the doomed and
flaky events' deliveries), and conflicting payloads folded in as if true.
Unit/integration coverage: 32 tests, clean under the race detector,
including tamper/deletion/reorder detection on the chain and a small-storm
regression test that fails if the naive processor ever starts passing.

Wall time for the full storm is ~40 s on a 2-core VM, dominated by fsync on
every ledger append (1,578 appends) and injected backoff sleeps. Durability
was chosen over throughput throughout; treat this as a correctness artifact,
not a benchmark.

## 5. Honest limits and threats to validity

- **Single node, serialised processing.** The total order receipts rely on is
  a single mutex's order. A distributed receiver would need consensus or a
  partitioned-by-event-ID design; neither is attempted here.
- **HMAC is symmetric.** Receipts convince holders of the shared secret.
  They do not give the public non-repudiation; that would need asymmetric
  signatures (e.g. Ed25519 receipts), a deliberate non-goal for this version.
- **Hash chains prove consistency, not history.** Whoever holds the ledger
  can rewrite it wholesale and recompute a self-consistent chain. The defence
  is anchoring: receipts (held by the provider) commit to entry hashes, and a
  periodically published head hash would anchor the whole chain. Without an
  anchor shared *before* a dispute, the chain detects edits relative to
  issued receipts, not a fully colluding rewrite. This matches the known
  limits of hash chaining without transparency-log anchoring.
- **The storm is a model.** Its provider is synthetic, its effects are
  order-insensitive by construction, and its fault rates (30% duplicates, 5%
  poison) are chosen to stress, not sampled from production traffic. Real
  handlers with non-commuting effects, real provider quirks (ID reuse across
  event types, clock skew beyond tolerance), and crash-durability mid-append
  are outside what this measurement covers. The engine does fsync before
  acknowledging application, but a crash between effect application and
  ledger append is only safe because the built-in store is reconstructed
  from the same process's files on the happy path — external side-effecting
  handlers must be retry-safe, which is a contract, not a guarantee the
  engine can enforce.
- **Seen-signature set grows unboundedly** in this implementation (in-memory
  map). A production deployment needs TTL'd storage bounded by the tolerance
  window plus the provider's retry horizon.
- **Doomed-event semantics are a choice.** Treating post-DLQ redeliveries as
  duplicates (rather than reprocessing) favours exactly-once effects over
  self-healing; the DLQ + replay tooling is the intended recovery path.

## 6. Relation to prior art

Standard Webhooks standardises the wire (IDs, signatures, timestamps) and
advises dedup; webhook-ledger is a receiver that implements and *proves* the
advice. Svix optimises outbound delivery; Hookdeck and Convoy provide
gateways whose inbound dedup is best-effort and windowed and whose replay
re-sends bytes to an endpoint — webhook-ledger's replay instead re-derives
*effects* and diffs them, and its receipts give the *sender* a verification
capability no gateway log provides. The idempotent-consumer and inbox/outbox
patterns supply the folklore; this project supplies the conflict semantics,
the evidence chain, and the measurement. Full analysis in `KILLGATE.md`.

## 7. Reproduce

```bash
go test ./...                                  # 32 tests
go build -o webhook-ledger ./cmd/webhook-ledger
./webhook-ledger storm -seed 42 -out storm-out # writes results.json
./webhook-ledger verify-ledger   -data storm-out
./webhook-ledger verify-receipts -data storm-out
./webhook-ledger replay          -data storm-out
```

All numbers in §4 are read from the generated `results.json` of that exact
run (committed at the repository root), not transcribed from design notes.
