# raft.ts

A reasonably-featured Raft consensus implementation in TypeScript covering
the subtler corners of the algorithm. Notably:

- **Two-phase voting (prevote)** so a flapping or recovering node cannot
  disrupt a healthy cluster's term progression.
- **Optimistic message delivery** on up-to-date followers — the leader does
  not wait for the previous append to ack before sending the next one.
- **Joint-consensus** dynamic cluster reconfiguration that can add and
  remove nodes safely.
- **Pluggable storage and messaging** behind small abstract interfaces.

Pluggable backends shipped in this repo:

| Concern | Implementations |
| ------- | --------------- |
| Storage | `MemStorage`, `SqliteStorageEngine` (better-sqlite3), `LmdbStorageEngine` (lmdb) |
| Messaging | `HttpMessagingEngine`, `UDPMessagingEngine`, `MqttMessagingEngine` |
| Serde | `MsgpackSerde` |
| Timeouts | `NodeTimeoutEngine` |

Tests are in `test/`; an integration harness with a deterministic in-memory
cluster lives at `test/cluster.ts`.

## Status

- **Algorithm**: covers leader election, log replication, joint-consensus
  reconfig, and the §5.4.2 noop-on-elect rule. 115+ unit and integration
  tests pass, including partition / re-election / catch-up scenarios.
- **Snapshotting**: not implemented. Currently the log grows without bound
  and a peer that falls more than the leader's full log behind cannot be
  re-onboarded — the leader will keep walking `next_idx` toward 1 instead
  of shipping a snapshot. This is the largest single gap before this
  library is appropriate for long-running production workloads. See
  [`docs/snapshots.md`](docs/snapshots.md) for design notes.
- **History**: an AI-driven cleanup pass found and fixed a number of bugs
  in the original implementation. See
  [`docs/ai-bug-analysis.md`](docs/ai-bug-analysis.md) for the categorized
  postmortem.

## Layout

```
src/             - library source
  server.ts        - core Raft engine
  state.ts         - persisted state
  messages.ts      - on-the-wire types
  interfaces.ts    - storage / messaging / timeout abstractions
  bigint_util.ts   - sort/min/max for bigint
  logger.ts        - injectable logger
  *_storage.ts     - storage backends
  *_messaging.ts   - messaging backends
  msgpack_serde.ts - msgpack codec
  node_timeout.ts  - real-clock timeout engine
test/            - mocha tests (unit + integration)
examples/        - runnable demos
```

## Development

Requires Node 20+ (see `.nvmrc`).

```
nvm use
npm install
npm test         # 115+ tests
npm run lint     # eslint
npm run typecheck
```
