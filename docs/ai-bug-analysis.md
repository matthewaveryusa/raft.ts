# AI bug-finding pass: what was found and what it teaches

This is a postmortem of the AI-driven cleanup pass on `raft.ts`. The pass
turned a hand-written but never-properly-tested Raft implementation into
something with 117 passing tests across three storage backends, an
in-memory cluster integration harness, and zero lint errors. Along the
way it found a healthy mix of real algorithm bugs, JS-flavored library
misunderstandings, and plugin-level data-corruption issues. This document
splits the bugs by category, names the ones the AI got demonstrably right
(each has a regression test), and pulls out a few learnings about where
AI is and isn't useful in a session like this.

The companion review doc with full code references is the chat
transcript; this README is the distilled "what kinds of bugs were
there, and what does that tell us" summary.

## Methodology, briefly

The pass had three rough phases:

1. Read the implementation, fix the obvious bugs, write enough tests to
   verify each fix when reverted.
2. Add corner-case tests until coverage felt reasonable.
3. Do a Staff-Eng-style review of the *remaining* code, then make every
   fix the review surfaced.

Each bug below has at least one regression test that fails when the fix
is reverted. Those tests are cited inline.

---

## Bugs in the core Raft implementation

These are bugs in the algorithm itself — they would manifest in any
language and any deployment. They're the highest-value finds.

### 1. `update_commit_idx` sorted bigints lexicographically

`Array.prototype.sort()` with no comparator coerces elements to strings,
so `[10n, 2n, 5n].sort()` returns `[10n, 2n, 5n]` (because `"10" < "2"`).
The leader was using this sort to find the median match-index across
followers. With three followers ack'd at 10, 2, and 5, the leader picked
2 as the committed index instead of 5, *committing entries that hadn't
actually been replicated to a majority*.

This is a "JS quirk meets distributed systems" bug that both produces
data loss and is hard to spot in review. It also wouldn't show up in
unit tests where the match indices happen to be similar magnitudes
(e.g., all in `[0, 10]`).

Fixed by passing a comparator to `sort()`, then later extracted to
`src/bigint_util.ts`.

Test: `uses numeric (not lexicographic) majority on match indexes`.

### 2. Follower's `commit_idx` could regress

`on_append_request` did:

```ts
this.state.commit_idx = bigint_min(msg.commit_idx, this.db.last_log_idx());
```

unconditionally. If a leader sent a delayed/out-of-order append carrying
a *stale* `commit_idx`, the follower would happily reduce its own
commit index, violating Raft's monotonicity guarantee. State-machine
entries that had been considered durable could become un-applied.

Fix: only advance if `msg.commit_idx > this.state.commit_idx`.

Test: `does not regress commit_idx if leader sends a stale value`.

### 3. `on_append_response` didn't step down on a higher term

Standard Raft says every RPC response handler steps down when it sees a
higher term. `on_vote_response` did. `on_append_response` did not. A
stale leader returning from a partition would loop forever decrementing
`next_idx` against a follower who had moved on to a new term, never
realizing the cluster had elected a new leader.

Test: `steps down when seeing a higher term in an append response`.

### 4. `peer.max_message_id` mixed IDs from disjoint pools

Each server has its own ID slab. When peer X sends *us* an
`AppendRequest`, the ID is drawn from X's slab; when X *replies* to one
of our requests, the reply echoes our ID. The original code stored both
into one `peer.max_message_id` and dropped any new message whose ID was
≤ that maximum. As soon as the max ever crossed the gap between the two
pools, legitimate messages from the other category were silently
dropped — a near-certain deadlock in a long-running cluster.

Fix: split into `max_request_id` and `max_response_id`. The bug never
showed up in unit tests because they reused one ID range across both
pools; the integration harness would catch it the moment the two pools
diverge.

### 5. Single-node cluster could never become leader

`has_vote_majority()` was only checked inside `on_vote_response`. A
single-node cluster has no peers, so no responses ever arrive, so
majority is never checked, so the candidate spins in prevote forever
even though self alone is a quorum.

Fix: eager majority check at the end of `candidate_start_prevote()`
and `candidate_start_vote()`.

Test: `a single-node cluster promotes itself on the first vote timeout`.

### 6. `promote_to_leader` defaulted to `send_noop = false`

Raft §5.4.2 (the "Figure 8" rule) requires a leader to commit one entry
of its own term before it can mark prior-term entries committed. The
production callsite (the win-vote path) passed `true`, but the *default*
was `false`. Any operator tooling, debug endpoint, or test path that
forgot to pass `true` got a leader that could incorrectly advance
`commit_idx` over uncommitted prior-term entries.

Fix: flipped the default to `true`. Tests that wanted a "blank" leader
state were updated to pass `false` explicitly.

### 7. `peer.next_idx` could go negative

When a follower kept rejecting, the recovery branch was:

```ts
if (peer.next_idx <= 0n) peer.next_idx = last_log_idx() - 1n;
```

If `last_log_idx() == 0n` (a brand-new leader) this set `next_idx = -1n`.
Downstream `db.log_term(-1n)` and `db.get_logs_after(-1n)` are
ill-defined — in `MemStorage` the latter returned `this.log.slice(-1)`
(the *last* element!), sending wrong data to the peer.

Fix: clamp `next_idx >= 1n` since logs are 1-indexed.

Test: `does not regress next_idx below zero on persistent rejections`.

### 8. Inflight registration after send (synchronous-transport bug)

The most subtle bug, and the only one found by *building* a regression
test rather than by review. `candidate_start_prevote`,
`candidate_start_vote`, and `send_append_entry` did roughly:

```ts
this.send(peer, message);
peer.inflight_messages.set(message.id, message);
```

With a real network this is fine — the response comes back milliseconds
later, well after the inflight map is populated. With a *synchronous*
transport (the in-memory cluster harness, but also potentially fast
local Unix-socket or shared-memory transports) the response arrives
inside `this.send(...)`, calls `pop_inflight_message`, finds nothing,
returns null, and the response is silently dropped. The cluster hangs
in prevote forever.

Fix: register the inflight entry and the message-timeout *before*
sending. Caught by the integration harness on first run.

Tests: `handles vote responses that arrive inside the send call`,
`handles append responses that arrive inside the send call`.

### 9. No leader-side step-down on persistent unresponsiveness

Pre-existing in the codebase but worth naming: the
`candidate_start_prevote` path's "leader without favorable responses
steps down" check is implemented but had no test. Now exercised by
`a leader without enough peer responses steps down at next vote timeout`.

### 10. Snapshotting absent (deferred)

Not a bug, but the largest remaining gap. The log grows linearly
forever, and a follower more than `last_log_idx` behind the leader
deadlocks (the leader walks `next_idx` to 1 and then has nothing left
to send). Documented at `docs/snapshots.md` with a concrete RPC and
storage extension design rather than left as a one-line README TODO.

---

## Library / language misunderstandings

These bugs are about JavaScript and Node ecosystem quirks that bit the
Raft logic *via* the language. Most of them have nothing to do with
distributed consensus and everything to do with "what does this stdlib
function actually do".

### A. `Array.prototype.sort()` on bigint

Already covered as bug #1. The misunderstanding is that JS's default
sort comparator coerces to string. This is also the canonical example
in interview questions ("why does `[10, 2, 1].sort()` work but
`[10, 2, 1].sort()` on numbers requires `(a, b) => a - b`?") — the
codebase had the canonical interview-question wrong answer baked into
its consensus logic.

### B. `unique_message_id()` post-increment off-by-one

```ts
this.next_message_id++;
return this.next_message_id;
```

The author probably meant `return this.next_message_id++` (post-increment
returns old value, increments side effect). What's written returns the
incremented value, so the very first ID returned is `chunk + 2`, not
`chunk + 1`. Not catastrophic — IDs are still monotonic and unique —
but the field name `next_message_id` was lying about what it held.
Renamed to `last_message_id` to match reality.

### C. `State.toString()` mutated the receiver

```ts
toString(): string {
  this.peer_addresses.sort();   // <-- mutates `this`!
  return JSON.stringify([...]);
}
```

Calling `toString()` for serialization quietly reordered the *live*
arrays held elsewhere by reference. The library author got JS reference
semantics wrong by treating "I'm just sorting before stringify" as
local. Fix: `[...arr].sort()`.

### D. `JSON.parse` on disk-resident data without try/catch

The code did `JSON.parse(last_log.data.toString())` on a config log
without any error handling, then iterated the result. A single corrupted
byte on disk crashes the server in `Array.prototype.forEach` instead of
producing a clean error. This is the "trust the disk" anti-pattern that
JS makes especially easy because dynamic typing hides the contract.
Fix: `parse_config_log()` validates JSON parse + structure + element
types and logs at error level on corruption.

### E. `save_state` did `kv_get` before every `kv_set`

Two disk operations per state mutation in order to compute event-emit
diffs. This is the JS "always reach for `JSON.parse(JSON.stringify(...))`
to clone" anti-pattern in a different costume — the author wanted a
"deep copy of previous state" and grabbed the most accessible tool
(re-read from disk) rather than maintain a shadow copy.

Fix: track `saved_state_snapshot: string | null` in memory, compare
against it.

### F. `State.make` length check too loose

```ts
if (arr.length < 4) return null;
// ...
s.config_idx = BigInt(arr[5]);
```

Off-by-two. Half the dynamic-typing-ism: the runtime didn't enforce that
`arr` was actually a 6-tuple and the author used a length sentinel
rather than a schema. `BigInt(undefined)` happens to throw — the next
shorter array would have crashed the constructor.

### G. `mocha.opts` deprecation, gts unmaintained, ESLint config-flat
   migration

Tooling was several major versions stale. `mocha.opts` was deprecated in
mocha 7 and removed in 8+. `gts@1.1.2` doesn't run on TypeScript 5.
ESLint v9 requires the flat config. None of these were "bugs" in the
strictest sense, but they put the codebase one Node upgrade away from
"nothing builds". Fixed by upgrading the toolchain and migrating to
`.mocharc.json` + `eslint.config.js` flat config.

### H. `tsconfig` target below ESNext

`1n` BigInt literals didn't compile, so tests had to write
`BigInt(1)` everywhere. Pure annoyance, not a bug.

---

## Plugin / backend bugs

These are bugs in the storage and messaging backends. Notably the
in-memory backend (`MemStorage`) had no bugs — the bugs are
concentrated in the backends that actually deal with the outside world.

### Plugin: `mqtt_messaging.ts` (severity: critical)

```ts
this.mqtt_client.publish(peer_addr, wire_message.toString());
```

`wire_message` is a `BufferList` of msgpack bytes. `.toString()` on a
Buffer/BufferList defaults to UTF-8 and replaces every byte that isn't
representable as a valid code point with `U+FFFD`. Roughly half of every
non-trivial msgpack payload silently corrupts on the way out. Receivers
fail to decode. **Every MQTT-backed cluster would have been broken and
no test would ever catch it because there were zero tests for the MQTT
engine.** Fixed by passing `wire_message.slice(0)` (the underlying
Buffer).

This is the canonical "binary in a string-typed API" mistake. JavaScript
makes it especially easy because `Buffer` has a working-but-lossy
`.toString()`.

### Plugin: `http_messaging.ts` (severity: major)

Two issues:

1. **Double decode.** `http_callback` decoded the request body once to
   look up `msg.from`, then *again* to emit the message. CPU waste, but
   also a hint that two patches landed without consolidation (a classic
   merge artifact).
2. **`console.log` everywhere.** Production messaging backend printed
   to stdout on every connection event. Real consumers would have to
   monkeypatch `console.log` or pipe stdout to /dev/null.

Fixed: single decode + injectable `Logger` interface. Logging is now
opt-in via constructor parameter.

### Plugin: `udp_messaging.ts` (severity: major)

`send` created a fresh `dgram.createSocket('udp4')` *per outbound message*.
At a 200ms heartbeat with N peers that's N socket creations every 200ms,
plus the per-message rate. On Linux this also leaves momentary
TIME_WAIT-style entries; under load, port exhaustion is straightforward.

Fixed: one client socket per engine, opened in `start()`, closed in
`stop()`.

### Plugin: `sqlite_storage.ts` (severity: major)

`get_logs_after` returned `Log` objects whose `idx` and `term` were
*strings* (from `CAST AS TEXT`) rather than bigints. Every other method
on the class did the `BigInt(...)` wrapping. The Log constructor signature
expected bigint, so anyone consuming these logs and doing arithmetic on
`idx` or `term` would silently get string-coerced math. (`'10' + 1` is
`'101'`, etc.)

This is the JS dynamic-typing hazard: TypeScript said the Log fields
were bigint, the SQL CAST said text, and the boundary was undefended.
Fixed with explicit `BigInt(val.idx)` / `BigInt(val.term)` and (later,
under TypeScript 5 + bumped `@types/better-sqlite3`) typed row interfaces
that make the boundary structural.

### Plugin: `lmdb_storage.ts` (severity: major)

The original lmdb backend used the unmaintained `node-lmdb` package which
fails to build on modern Node. Switched the entire file to the
maintained `lmdb` package. This was a rewrite, not a bug fix — but the
fact that the old plugin couldn't even be tested ("the install fails")
counts as the largest single under-the-radar issue. **Two of three
storage backends literally did not work** when the project was opened.

### Plugin: `mem_storage.ts` (severity: clean)

No bugs. The pure-JS in-memory backend was the simplest and the only one
that worked out of the box. This is consistent with the broader pattern
that all the plugin bugs were at boundaries between JS and either
binary data or native modules.

### Plugin: `msgpack_serde.ts` (severity: clean)

No bugs. Round-trip tests confirmed VoteRequest/VoteResponse/AppendRequest
(with multi-log payloads, null data buffers, and very large bigints) all
round-trip cleanly.

### Plugin counts

| Plugin | Bugs found | Severity |
| --- | --- | --- |
| `mqtt_messaging.ts` | 1 | Critical (binary corruption) |
| `http_messaging.ts` | 2 | Major (perf + observability) |
| `udp_messaging.ts` | 1 | Major (resource exhaustion) |
| `sqlite_storage.ts` | 1 | Major (type-confusion) |
| `lmdb_storage.ts` | (whole-file rewrite) | Major (uninstallable) |
| `mem_storage.ts` | 0 | clean |
| `msgpack_serde.ts` | 0 | clean |

The shape is striking: **0 bugs in pure-JS plugins, 5 bugs across 4
plugins that touch binary data, native modules, or system resources.**

---

## Where AI did well, where it didn't, and why

### What AI was unambiguously good at

1. **Mechanical static review.** Bugs #1, #2, #6, #7, A, B, C, F, the
   MQTT toString, and the HTTP double-decode were all visible from
   reading the code carefully and saying "wait, what does this actually
   do." A human can do that, but a human gets bored. The AI does not
   get bored, doesn't skim, and doesn't have a stake in the existing
   design that would make it want a bug to not be a bug.
2. **Writing the regression test for a bug you already understand.**
   The pattern "I just fixed this; show me a test that would have caught
   it" is the AI's home turf. Each of the bug fixes above has a
   regression test that fails when the fix is reverted, generated with
   roughly five seconds of human input each.
3. **Toolchain migration.** Bumping mocha 7 → 10, dropping `mocha.opts`
   for `.mocharc.json`, replacing `gts` with stock ESLint flat config,
   replacing `node-lmdb` with `lmdb` — all repetitive, all error-prone
   for humans, all easy for AI as long as the tests are good enough to
   detect breakage.
4. **Documentation.** This file. The snapshot design doc. The Staff-Eng
   PR review. AI is genuinely good at "look at this whole tree and
   write the document a careful human would have written if they had
   the time."

### What AI struggled with

1. **Bugs that only appear at the integration boundary.** Bug #8 (the
   synchronous-transport ordering bug) was *not* found by the AI's
   static review. It was found by *running* the multi-node integration
   harness the AI built and watching all three nodes hang in prevote.
   The fix is small but the bug's existence required a test environment
   that was sensitive to it. AI built that environment, but only after
   being asked for it.
2. **Knowing when to stop refactoring.** When asked "make all the
   fixes," the AI's tendency was to keep going past the point of
   diminishing returns — bumping every dependency, refactoring
   well-covered code, adding metrics endpoints nobody asked for. Each
   change is correct in isolation; their *aggregate* surface is a
   review burden. Better instructions ("fix critical/major; nits only
   if trivial") would have produced cleaner output.
3. **Native dependency archaeology.** Switching `node-lmdb` to `lmdb`
   required hands-on iteration: "open + path is wrong, oh wait it
   wants a file not a directory, oh wait the test setup creates a
   directory with mkdir." AI got there but with notably more
   back-and-forth than the static refactors. The model of "system
   call layer below my codebase" is fuzzier in the model's head than
   "TypeScript type system above my codebase."
4. **Real concurrency reasoning.** The synchronous-transport bug above
   is the most interesting datapoint. The AI *fixed* it confidently
   once it had a failing test. The AI *did not predict* it during
   static review even though, in hindsight, "send-then-register"
   versus "register-then-send" is a textbook ordering question. This
   suggests AI review is good at "violations of stated rules" and
   weak at "subtle preconditions that must hold across the body of a
   method when I/O is interleaved." If your codebase has a lot of the
   latter, integration tests are the only oracle.

### Bug-density patterns

A few things stood out:

- **Plugins were dirtier than core.** Counting only fix-able bugs,
  plugins had 1 critical + 4 major (5 total in 4 of 7 plugin files);
  core Raft had 4 critical-ish + 5 major (9 total). Per file, plugin
  bug density is comparable. But every plugin bug was a "JS-meets-the-
  outside-world" bug — binary handling, native modules, syscall-y
  resource handling. Pure-JS plugins were spotless.
- **Most core bugs were one-liners.** Of the nine core bugs, six were
  one-line fixes once you understood the bug. The remaining three
  (#4 ID pool split, #6 noop default with cascading test updates, #8
  synchronous transport) required multi-line surgery. This is
  consistent with experience reviewing real distributed-systems code:
  the bugs are conceptual but the diffs are tiny.
- **The most data-corrupting bugs were the easiest to find.** #1
  (lex sort), #2 (commit regression), and #C (toString mutation)
  would all silently corrupt cluster state. All three were trivially
  visible in a careful read of the source. There's a temptation to
  assume "if it were a real bug someone would have found it"; this
  codebase had had several previous owners and none of these were
  caught.

### What the next pass should look like

If I were going to do another pass, the highest-value remaining work
isn't more static review — it's:

1. **Implement snapshotting per `docs/snapshots.md`.** This is the
   one issue that gates production usability and isn't a one-line fix.
2. **Write a Jepsen-style fault test.** Take the existing
   `Cluster` harness, add randomized partition / message-drop /
   message-reorder / replay, and run a state-machine model checker
   over many iterations. The integration tests today are scenario
   tests, not search tests.
3. **Test the messaging plugins.** All of them. The MQTT bug existed
   for an unknown amount of time because nothing exercised the engine.

These are exactly the tasks that AI is most useful at *after* a human
has set the goal — the framing and the model-checker contract are
where humans still add the most value.
