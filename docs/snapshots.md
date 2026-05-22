# Snapshotting (design notes)

> Status: **not implemented**. This document captures the intended design
> so the gap is concrete and reviewable rather than open-ended.

## Why we need it

Today the in-memory log grows without bound and the storage backends keep
every entry forever. Two concrete failure modes follow:

1. **Disk / RSS growth is linear in time.** A leader that has been up
   for weeks accumulates every client write with no way to release the
   tail. Even with the in-memory log only being read on append-replay,
   the storage layer keeps everything.
2. **Lagging follower deadlock.** When a follower falls further behind
   than the leader's earliest live log entry, the leader walks
   `peer.next_idx` down to 1 and tries to send the entire history. That
   works today only because we never trim the history. As soon as we
   trim, the leader has nothing to send the laggy peer and the cluster
   silently loses replication on that node.

A leader-installed snapshot fixes both: the leader can throw away log
entries up to the snapshot index, and a too-far-behind follower can be
caught up with one bulk transfer instead of a per-entry walk.

## Wire-level shape

Add one new RPC and one new log-style record:

- `InstallSnapshot` (RPC):
  - `term` — leader's term, used for the same step-down rules as the
    other RPCs.
  - `last_included_idx` — the log index that the snapshot covers up to.
  - `last_included_term` — its term (so the receiver can match it like
    a normal `prev_idx`/`prev_term` pair).
  - `last_config` — the cluster config at `last_included_idx`. Needed
    because the receiver may have to populate `peer_addresses` /
    `peer_addresses_old` from the snapshot before it has any logs.
  - `data` — opaque bytes produced by the application.
  - The response is a small ack with `term` and a `success` flag, same
    shape as `AppendResponse`.

- `LogType.snapshot` already exists in `src/messages.ts` but is unused.
  The intent is that storage backends store the snapshot at a known key
  and `last_log_idx()` returns `max(snapshot_idx, last log entry idx)`
  so the rest of the engine doesn't need to know whether an idx came
  from a log entry or a snapshot.

## Storage interface additions

Extend `AbstractStorageEngine`:

```ts
abstract install_snapshot(meta: SnapshotMeta, data: Buffer): void;
abstract latest_snapshot(): { meta: SnapshotMeta; data: Buffer } | null;
abstract trim_logs_up_to(idx: bigint): void;

interface SnapshotMeta {
  last_included_idx: bigint;
  last_included_term: bigint;
  last_config: { old_peers: string[]; new_peers: string[] };
}
```

`get_logs_after(idx)` MUST still work for `idx === snapshot.last_included_idx`
(returning the entries after the snapshot). For `idx <` snapshot index,
it should return null / empty and the caller is expected to use
`latest_snapshot()` instead.

## Server-side flow

1. **Triggering**: an embedder calls `server.compact_to(idx, snapshot_data)`.
   The server validates `idx <= state.commit_idx` and `idx >= snapshot_idx`,
   writes the snapshot via storage, then trims logs up to `idx`.
2. **Sending to a lagging peer**: in `on_append_response` the existing
   `next_idx` walk-back lands below the leader's earliest log. At that
   point we call `latest_snapshot()` and send `InstallSnapshot` instead
   of `AppendRequest`.
3. **Receiving on a follower**: validate term, write the snapshot, drop
   any logs at or before `last_included_idx`, update
   `state.commit_idx = max(commit_idx, last_included_idx)`, populate
   peer config from `meta.last_config`, save state, ack.

## Things this design intentionally punts

- **Streaming snapshots**. The first cut sends `data` in one message.
  Real workloads will eventually need chunking — that adds offsets,
  per-chunk acks, and rate-limit knobs. None of that is needed to fix
  the deadlock above.
- **Application semantics of `data`**. The library treats it as opaque.
  Producing and applying snapshot bytes is the embedder's job (we just
  ferry them between nodes).
- **Concurrent snapshot install while replicating**. The simple version
  refuses new appends from this peer until the snapshot is fully
  installed.

## Test plan

Mirror the existing partition / catch-up integration tests but with
log-trim turned on so the leader has actually thrown away entries the
follower needs:

1. Form a 3-node cluster, take a leader.
2. Partition n3, generate enough writes to trim past the index it
   stopped at.
3. Heal n3 and assert that it catches up via `InstallSnapshot` (not
   per-entry walk-back).
4. Crash + restart n3 with only the snapshot on disk; confirm it
   returns to steady state.
