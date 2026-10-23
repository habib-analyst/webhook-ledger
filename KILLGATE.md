# KILLGATE — webhook-ledger

Date: 2026-10-23. Question: does an inbound webhook **reconciliation engine** with
(1) correctness *proven* by a fault storm against a ground-truth oracle,
(2) a signed, hash-chained **receipt ledger** that lets the receiver prove to the
provider exactly what was received and applied, in what order, and
(3) a replay tool that re-derives downstream effects from the ledger alone and
diffs them against live state — already exist as an available tool?

## What exists

| Tool / source | What it actually does | What it does NOT do |
|---|---|---|
| **Standard Webhooks spec** (Svix et al.) | Signing scheme (`webhook-id`, `webhook-timestamp`, `webhook-signature`, HMAC-SHA256 over `id.timestamp.body`), stable message ID across retries, retry schedule guidance | It is a wire format, not a receiver. No ledger, no receipts, no processing semantics beyond "deduplicate on the ID" advice |
| **Svix** | Outbound webhook *sending* platform: signing, retries with backoff, DLQ for failed *deliveries to customers*, delivery observability | Inbound side is documentation ("store the ID in Redis and dedupe"). No inbound reconciliation engine, no receiver-side receipts, no oracle-tested correctness claim |
| **Hookdeck Event Gateway** | Closest prior art on the inbound side: managed gateway that ingests webhooks, durable queueing, **best-effort deduplication inside a 1s–1h window**, filtering, forwarding, and *delivery* replay (re-send a stored delivery to your endpoint) | Dedup is explicitly best-effort and windowed (their own docs, via AgentMail's analysis), so correctness still rests on the handler. No hash-chained ledger, no signed per-event receipts a provider can verify, no replay that re-derives *downstream effects* and diffs them against live state, no published storm/oracle proof |
| **Convoy** (open source, Go) | Self-hostable webhook gateway, fan-out, both directions, retries | Delivery infrastructure. No receipt ledger, no effect-replay diff, no storm proof |
| **EventDock** | Managed inbound receiving: signature verification, DLQ with one-click replay, 24h dedup window | Same shape as Hookdeck: operational dedup window, delivery replay. No cryptographic receipts, no hash chain, no oracle diff |
| **Stripe webhook docs / Workbench** | Best practices (verify signatures, idempotent handlers, return 2xx fast) + source-side delivery history for Stripe's own events | Guidance and a sender-side log. Stripe never sees, and cannot prove, what *your* system applied |
| **Idempotent-consumer / inbox–outbox patterns** (blog posts, OWASP cheat sheet, AgentMail's write-ups) | The standard recipe: persist processed event IDs, unique constraint, return 200 for duplicates, release-on-failure TTL tricks | Patterns and snippets, not a shipped engine. None define conflict semantics for *same ID, different payload* beyond a fingerprint column; none issue receipts; none prove anything under a storm |
| **Traceseal / "signed execution receipts" literature** | Argues logs are not evidence; hash chaining + signed receipts as the end of the trust road for *execution* audit | A concept/primer about receipts for job execution, not a webhook receiver. No receiver, no dedup/conflict engine, no storm |

## The gap (the claim)

Every existing tool sits on one side of the line:

- **Senders/gateways** (Svix, Hookdeck, Convoy, EventDock) move deliveries reliably
  and keep *operational* logs. Their dedup is a window, their replay re-sends bytes,
  and when a provider disputes "you never processed event X" or a receiver disputes
  "you sent event X twice with different bodies", neither side can *prove* anything
  from those logs — the operator can rewrite them.
- **Receiver guidance** (specs, cheat sheets, blog recipes) tells you to dedup, but
  ships no semantics for the hard cases (payload conflict under a known ID, signature
  replays with fresh timestamps, poison deliveries interleaved with retries) and no
  evidence that an implementation survives them.

**webhook-ledger's differentiation, precisely:**

1. **Storm-proven reconciliation, not best-effort dedup.** The engine ships with a
   fault-injection harness (duplicates, windowed reordering, poison deliveries,
   handler-failure injection) and a ground-truth oracle. Correctness is a measured
   number — live effect store vs oracle mismatches, with a naive no-dedup processor
   run under the identical storm as the honest contrast — not a design claim.
2. **Signed, hash-chained receipt ledger.** Every state transition of every delivery
   is appended to a hash-chained ledger; every applied event gets an HMAC-signed
   receipt binding event ID + payload hash + ledger sequence + ledger entry hash.
   Anyone holding the provider secret can verify exactly what was applied, in what
   order, and detect any later edit to the ledger. This turns a he-said/she-said
   delivery dispute into a checkable proof. (Honest scope: HMAC is symmetric — the
   receipt convinces holders of the shared secret, not the general public; the hash
   chain proves internal consistency and tamper-evidence, not external anchoring.)
3. **Effect replay with byte-identical diff.** The replay tool rebuilds the
   downstream effect store from ledger `applied` entries alone and diffs it,
   byte-for-byte, against the live store. Hookdeck-style replay re-sends bytes to an
   endpoint and hopes; this replay *proves* the ledger fully determines live state.

Distinct from sibling projects in this programme: **orderstream** is an internal
event-sourced pipeline on NATS (our own events, our own bus); **idemflow** classifies
*why* dead-lettered jobs failed. webhook-ledger faces *inbound third-party*
deliveries, where the sender is outside our control, signatures and conflicts are
adversarial-shaped, and the deliverable is proof, not just processing.

## Verdict

**PROCEED.** The exact package — inbound reconciliation engine + storm/oracle
correctness proof + signed hash-chained receipt ledger + effect-replay diff — does
not exist as an available tool. The components exist separately as a wire spec
(Standard Webhooks), managed gateways (Hookdeck/Svix/Convoy), and patterns
(idempotent consumer). The composition, the conflict semantics, the receipts, and
the proof harness are the novel position.
