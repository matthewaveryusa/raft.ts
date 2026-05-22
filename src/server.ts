import {
  AbstractMessagingEngine,
  AbstractStorageEngine,
  AbstractTimeoutEngine,
} from './interfaces';
import { bigint_cmp, bigint_min } from './bigint_util';
import { Logger, NOOP_LOGGER } from './logger';
import {
  AppendRequest,
  AppendResponse,
  Log,
  LogType,
  Message,
  MessageType,
  VoteRequest,
  VoteResponse,
} from './messages';
import { Role, State } from './state';
import { EventEmitter } from 'events';

// raft engine
export interface LogIdxTerm {
  idx: bigint;
  term: bigint;
}

export interface ClientError {
  error: string;
  leader: string | null;
}

export interface ServerOptions {
  // Reserve this many message IDs at a time on disk so a crash never reuses
  // an ID. Defaults to 1,000,000 — large enough that the disk write cost
  // amortizes to ~1 hit per million sends.
  id_slab_size?: bigint;
  // Maximum number of inflight (unacked) messages we'll keep in memory per
  // peer before refusing to enqueue more. A slow peer can otherwise
  // accumulate one entry per client request unbounded.
  max_inflight_per_peer?: number;
  // Optional logger; defaults to a no-op so unconfigured servers stay quiet.
  logger?: Logger;
}

// Public, read-only snapshot of a peer's replication state. Used by the
// `metrics()` accessor so external code does not need to reach into the
// internal `Peer` object.
export interface PeerMetrics {
  addr: string;
  match_idx: bigint;
  next_idx: bigint;
  inflight_count: number;
  is_in_old_config: boolean;
  is_in_new_config: boolean;
  is_responding_favorably: boolean;
  vote_granted: boolean;
}

export interface ServerMetrics {
  my_addr: string;
  role: Role;
  current_term: bigint;
  current_leader: string | null;
  commit_idx: bigint;
  config_idx: bigint;
  last_log_idx: bigint;
  voted_for: string | null;
  is_prevoting: boolean;
  peer_addresses: string[];
  peer_addresses_old: string[];
  peers: PeerMetrics[];
}

// Internal-only peer state. Kept un-exported so external consumers can't
// reach in and mutate replication state — use `metrics()` for read-only
// access or `__test_peer` for explicit test access.
class Peer {
  vote_granted = false;
  match_idx = BigInt(0);
  next_idx = BigInt(1);
  inflight_messages = new Map<bigint, Message>();
  // The largest *request-side* (peer-originated) ID we've processed.
  // Requests carry IDs from the peer's own slab.
  max_request_id = BigInt(0);
  // The largest *response-side* (us-originated) ID we've processed. Response
  // messages echo our own request ID, drawn from our slab. We track these
  // separately so an inbound request from peer's slab cannot mask a later
  // response from our slab (or vice versa) — see the bug fix referenced in
  // tests/raft.test.ts.
  max_response_id = BigInt(0);
  accepting_optimistic_appends = true;
  is_responding_favorably = false;

  constructor(
    public readonly addr: string,
    public is_in_old_config: boolean,
    public is_in_new_config: boolean
  ) {}
}

// Test-only access to internal peer state. Only use this from tests; the
// underscore prefix is a convention to make accidental production use
// obvious in code review.
export type TestPeerHandle = Peer;

export class Server extends EventEmitter {
  readonly my_addr: string;
  private timeout_engine: AbstractTimeoutEngine;
  private messaging_engine: AbstractMessagingEngine;
  private db: AbstractStorageEngine;
  private logger: Logger;

  private readonly timeout_ms: number;
  private readonly leader_heartbeat_ms: number;
  private readonly message_timeout_ms: number;
  private readonly id_slab_size: bigint;
  private readonly max_inflight_per_peer: number;

  private role: Role;
  private current_leader: string | null;
  private peers: Map<string, Peer>;
  // Largest message ID we have already returned. The field name is "last"
  // rather than "next" because `unique_message_id()` returns the
  // post-incremented value: the first ID handed out after construction is
  // `last_message_id + 1`.
  private last_message_id: bigint;
  // The chunk boundary (inclusive last ID) we have persisted to disk. We
  // can hand out IDs up through this value without any disk write.
  private message_id_chunk_end: bigint;
  private state: State;
  // Shadow copy of the last serialized state. Used so save_state can detect
  // changes and emit events without a per-call disk read.
  private saved_state_snapshot: string | null;
  private is_prevoting: boolean;

  constructor(
    address: string,
    peers_bootstrap: string[],
    db: AbstractStorageEngine,
    timeout_engine: AbstractTimeoutEngine,
    messaging_engine: AbstractMessagingEngine,
    options?: ServerOptions | bigint
  ) {
    super();
    this.timeout_engine = timeout_engine;
    this.messaging_engine = messaging_engine;
    this.db = db;
    // Backwards-compatible: older callers passed `id_slab_size` directly as
    // the 6th positional argument.
    const opts: ServerOptions =
      typeof options === 'bigint' ? { id_slab_size: options } : options ?? {};
    this.id_slab_size = opts.id_slab_size ?? BigInt(1000000);
    this.max_inflight_per_peer = opts.max_inflight_per_peer ?? 1024;
    this.logger = opts.logger ?? NOOP_LOGGER;
    this.is_prevoting = false;

    this.timeout_ms = 2000;
    this.leader_heartbeat_ms = this.timeout_ms / 10;
    this.message_timeout_ms = this.timeout_ms;
    this.my_addr = address;

    // volatile state
    this.role = Role.follower;
    this.emit('role_change', this.role);
    this.current_leader = null;
    this.peers = new Map();
    this.saved_state_snapshot = null;

    // Reserve a slab of message IDs on disk so a crash never reuses an ID.
    // We start at `chunk + 1` (the first ID the previous instance had not
    // yet handed out) and pre-claim a fresh slab on construction so callers
    // that issue a single message do not double-cost (read + write at
    // first send).
    const saved_chunk = this.db.kv_get('message_id_chunk');
    const chunk = saved_chunk ? BigInt(saved_chunk) : BigInt(0);
    this.last_message_id = chunk + BigInt(1);
    const new_chunk_end = chunk + this.id_slab_size;
    this.db.kv_set('message_id_chunk', new_chunk_end.toString());
    this.message_id_chunk_end = new_chunk_end;

    const state = this.load_state_safely();
    if (state) {
      this.state = state;
      const idx = this.db.last_log_idx();
      if (idx !== BigInt(0)) {
        const logs = this.db.get_logs_after(idx - BigInt(1));
        const last_log = logs[0];
        if (
          last_log &&
          last_log.type === LogType.config &&
          last_log.data !== null
        ) {
          const config = parse_config_log(last_log.data, this.logger);
          if (config) {
            // state is written after config is written to log; if a crash
            // occurs between the writes the state has an invalid peer set,
            // so we trust the log over the saved state.
            this.state.peer_addresses = config.new_peers;
            this.state.peer_addresses_old = config.old_peers;
            this.state.config_idx = last_log.idx;
            this.save_state();
          }
        }
      }
    } else {
      this.state = new State();
      peers_bootstrap.forEach(peer_addr =>
        this.state.peer_addresses.push(peer_addr.toString())
      );
      this.save_state();
    }
    this.reconcile_peers_with_state();
  }

  // Read and validate the persisted state. Returns null if no state has been
  // saved or if the saved state is obviously corrupt — corrupt-state bootstraps
  // are logged but not fatal so an operator can recover.
  private load_state_safely(): State | null {
    const raw = this.db.kv_get('state');
    if (!raw) return null;
    try {
      const s = State.make(raw);
      if (s) this.saved_state_snapshot = raw;
      return s;
    } catch (err) {
      this.logger.error?.('raft: persisted state failed to parse', { err });
      return null;
    }
  }

  // make this server think it's the leader.
  //
  // The default sends a no-op log entry: per Raft §5.4.2 a leader cannot
  // mark prior-term entries committed without first committing one of its
  // own term. Pass `false` only in tests that want to inspect the freshly
  // promoted leader before it appends anything.
  promote_to_leader(send_noop = true): void {
    this.reset_vote_timeout();
    if (this.role !== Role.leader) {
      this.role = Role.leader;
      this.emit('role_change', this.role);
    }
    if (this.current_leader !== this.my_addr) {
      this.current_leader = this.my_addr;
      this.emit('current_leader', this.my_addr);
    }
    this.reset_volatile_peer_state();
    for (const [, p] of this.peers) {
      this.reset_heartbeat(p);
    }
    if (send_noop) {
      this.leader_append_entry(Log.make_noop());
    }
  }

  start_server(): void {
    this.reset_vote_timeout();
    this.messaging_engine.on('message', (msg: Message) => this.on_message(msg));
    this.messaging_engine.start(this.my_addr);
  }

  on_client_request(data: Buffer | null): LogIdxTerm | ClientError {
    if (this.role !== Role.leader) {
      return { error: 'not_leader', leader: this.current_leader };
    }
    const log = Log.make_empty();
    log.data = data;
    log.type = LogType.message;
    this.leader_append_entry(log);
    return { idx: log.idx, term: log.term };
  }

  on_config_change_request(peers: string[]): LogIdxTerm | ClientError {
    if (this.role !== Role.leader) {
      return { error: 'not_leader', leader: this.current_leader };
    }
    let old_peers;
    if (this.state.peer_addresses_old.length) {
      old_peers = this.state.peer_addresses_old;
    } else {
      old_peers = this.state.peer_addresses;
    }

    this.state.peer_addresses_old = old_peers;
    this.state.peer_addresses = peers;

    this.reconcile_peers_with_state();

    const log = Log.make_empty();
    log.data = Buffer.from(JSON.stringify({ old_peers, new_peers: peers }));
    log.type = LogType.config;
    this.leader_append_entry(log);
    this.state.config_idx = log.idx;
    this.save_state();
    return { idx: log.idx, term: log.term };
  }

  reconcile_peers_with_state(): void {
    for (const [, peer] of this.peers) {
      peer.is_in_new_config = false;
      peer.is_in_old_config = false;
    }
    this.state.peer_addresses.forEach(peer_addr => {
      if (peer_addr === this.my_addr) {
        return;
      }
      const peer = this.peers.get(peer_addr);
      if (peer) {
        peer.is_in_new_config = true;
      } else {
        this.peers.set(peer_addr, new Peer(peer_addr, false, true));
      }
    });

    // Walk old peers; if they're already known as new peers, also flag them
    // as old; otherwise create a new Peer with only the old-config flag set.
    this.state.peer_addresses_old.forEach(peer_addr => {
      if (peer_addr === this.my_addr) {
        return;
      }
      const peer = this.peers.get(peer_addr);
      if (peer) {
        peer.is_in_old_config = true;
      } else {
        this.peers.set(peer_addr, new Peer(peer_addr, true, false));
      }
    });

    for (const [peer_name, peer] of this.peers) {
      if (peer.is_in_new_config === false && peer.is_in_old_config === false) {
        // remove peer from list of peers
        this.reset_inflight_messages(peer);
        this.timeout_engine.clear(`heartbeat_${peer_name}`);
        this.peers.delete(peer_name);
      }
    }
  }

  on_message(msg: Message) {
    if (msg.to !== this.my_addr) {
      return;
    }

    const peer = this.peers.get(msg.from);
    if (!peer) {
      return;
    }

    // Inbound requests are tagged with the peer's slab; responses to our
    // requests echo IDs from our slab. Track the two sides separately so
    // an ID from one pool cannot mask later messages from the other pool.
    const is_response =
      msg.type === MessageType.vote_response ||
      msg.type === MessageType.append_response;
    const max = is_response ? peer.max_response_id : peer.max_request_id;
    if (max >= msg.id) {
      // received out of order old message, ignoring
      return;
    }
    if (is_response) {
      peer.max_response_id = msg.id;
    } else {
      peer.max_request_id = msg.id;
    }

    switch (msg.type) {
      case MessageType.vote_request:
        this.on_vote_request(msg as VoteRequest, peer);
        break;
      case MessageType.vote_response:
        {
          const request = this.pop_inflight_message(
            peer,
            msg.id
          ) as VoteRequest;
          if (request === null) {
            return;
          }
          this.on_vote_response(request, msg as VoteResponse, peer);
        }
        break;
      case MessageType.append_request:
        this.on_append_request(msg as AppendRequest, peer);
        break;
      case MessageType.append_response:
        {
          const request = this.pop_inflight_message(
            peer,
            msg.id
          ) as AppendRequest;
          if (request === null) {
            return;
          }
          this.on_append_response(request, msg as AppendResponse, peer);
        }
        break;
      default:
        return;
    }
  }

  has_vote_majority(): boolean {
    // While in joint consensus we need a majority of BOTH the old and new
    // configurations independently (Raft §6).
    const num_old_peers = this.state.peer_addresses_old.length;
    if (num_old_peers !== 0) {
      // +1 is so the required number is rounded up; equivalent to
      // floor(N/2) + 1, the standard majority formula.
      const self_in_old = this.state.peer_addresses_old.includes(this.my_addr)
        ? 1
        : 0;
      const min_votes = Math.ceil((num_old_peers + 1) / 2);
      let count = self_in_old;
      for (const [, peer] of this.peers) {
        if (peer.vote_granted && peer.is_in_old_config) {
          count += 1;
        }
      }
      if (count < min_votes) {
        return false;
      }
    }

    const num_new_peers = this.state.peer_addresses.length;
    if (num_new_peers !== 0) {
      const self_in_new = this.state.peer_addresses.includes(this.my_addr)
        ? 1
        : 0;
      const min_votes = Math.ceil((num_new_peers + 1) / 2);
      let count = self_in_new;
      for (const [, peer] of this.peers) {
        if (peer.vote_granted && peer.is_in_new_config) {
          count += 1;
        }
      }
      if (count < min_votes) {
        return false;
      }
    }
    return true;
  }

  // Returns a freshly-loaded copy of the persisted state. Most callers
  // should use `metrics()` instead. We keep this around for backwards
  // compatibility with external consumers; internal save-state diffing uses
  // the in-memory `saved_state_snapshot` to avoid a disk round-trip on every
  // mutation.
  get_state(): State | null {
    const state = this.db.kv_get('state');
    if (!state) {
      return null;
    }
    return State.make(state);
  }

  save_state(): void {
    const serialized = this.state.toString();
    if (serialized === this.saved_state_snapshot) {
      // Nothing to do; the in-memory state hasn't changed since we last
      // wrote. Skip the disk write entirely.
      return;
    }

    // Compute event diffs against the in-memory snapshot rather than
    // re-reading from disk.
    const old =
      this.saved_state_snapshot !== null
        ? State.make(this.saved_state_snapshot)
        : null;

    this.db.kv_set('state', serialized);
    this.saved_state_snapshot = serialized;

    if (!old) {
      return;
    }
    if (old.current_term !== this.state.current_term) {
      this.emit('term', this.state.current_term);
    }
    if (old.commit_idx !== this.state.commit_idx) {
      this.emit('commit_idx', this.state.commit_idx);
    }
    if (old.voted_for !== this.state.voted_for) {
      this.emit('voted_for', this.state.voted_for);
    }
  }

  reset_vote_timeout() {
    this.timeout_engine.set_varied('vote', this.timeout_ms, () =>
      this.candidate_start_prevote()
    );
  }

  step_down(term: bigint): void {
    this.is_prevoting = false;
    this.state.current_term = term;
    this.state.voted_for = null;
    this.save_state();

    if (this.role !== Role.follower) {
      this.role = Role.follower;
      this.emit('role_change', this.role);
    }
    if (this.current_leader !== null) {
      this.current_leader = null;
      this.emit('current_leader', null);
    }
    this.reset_volatile_peer_state();
    this.reset_vote_timeout();
  }

  reset_inflight_messages(peer: Peer): void {
    for (const [id] of peer.inflight_messages) {
      this.timeout_engine.clear(id.toString());
    }
    peer.inflight_messages = new Map();
  }

  pop_inflight_message(peer: Peer, id: bigint): Message | null {
    const message = peer.inflight_messages.get(id);
    if (message) {
      peer.inflight_messages.delete(id);
      this.timeout_engine.clear(id.toString());
    } else {
      return null;
    }
    return message;
  }

  reset_volatile_peer_state(): void {
    for (const [peer_addr, peer] of this.peers) {
      this.timeout_engine.clear(`heartbeat_${peer_addr}`);

      // only address, port, and max msg id are not volatile
      peer.vote_granted = false;
      peer.match_idx = BigInt(0);
      peer.next_idx = this.db.last_log_idx() + BigInt(1);
      this.reset_inflight_messages(peer);
      peer.accepting_optimistic_appends = true;
      peer.is_responding_favorably = false;
    }
  }

  candidate_start_prevote(): void {
    this.timeout_engine.clear('vote');

    if (
      this.state.peer_addresses_old.length === 0 &&
      this.state.peer_addresses.indexOf(this.my_addr) === -1
    ) {
      // no longer in config! clear timeouts which will exit the program
      if (this.role === Role.leader) {
        this.step_down(this.state.current_term);
        this.timeout_engine.clear('vote');
        this.messaging_engine.stop();
        return;
      } else {
        this.reset_volatile_peer_state();
        this.messaging_engine.stop();
        return;
      }
    }
    if (this.role === Role.leader) {
      // Make sure we got some sort of message from a majority of the cluster
      // within the vote timeout. If we did, we continue to be the leader.
      // If we didn't, we step down to avoid perpetuating a split-brain and
      // try to win a vote afresh.
      let num_messages = 0;
      for (const [, peer] of this.peers) {
        if (peer.is_responding_favorably === true) {
          peer.is_responding_favorably = false;
          num_messages++;
        }
      }
      if (num_messages >= Math.floor((this.peers.size + 1) / 2)) {
        this.reset_vote_timeout();
        return;
      } else {
        this.step_down(this.state.current_term);
      }
    }

    this.is_prevoting = true;
    const idx = this.db.last_log_idx();
    const term = this.db.log_term(idx);

    for (const [peer_addr, peer] of this.peers) {
      peer.vote_granted = false;
      this.reset_inflight_messages(peer);
      const message_id = this.unique_message_id();
      const message = new VoteRequest(
        message_id,
        this.my_addr,
        peer_addr,
        this.state.current_term + BigInt(1),
        idx,
        term,
        true
      );
      // IMPORTANT: register the inflight message and the message-timeout
      // BEFORE sending. A synchronous transport (in-memory bus, fast local
      // socket, etc.) can deliver the response inside `this.send(...)`. If
      // we registered after sending, `pop_inflight_message` would return
      // null for that early response and we would silently drop it.
      this.timeout_engine.set(
        message_id.toString(),
        this.message_timeout_ms,
        () => {
          peer.inflight_messages.delete(message_id);
        }
      );
      peer.inflight_messages.set(message_id, message);
      this.send(peer, message);
    }
    this.reset_vote_timeout();

    // Single-node clusters (and any config where self alone constitutes a
    // majority) would otherwise wait forever for peer responses that will
    // never arrive. Self-vote is implicit, so check majority eagerly.
    if (this.has_vote_majority()) {
      this.is_prevoting = false;
      this.candidate_start_vote();
    }
  }

  candidate_start_vote(): void {
    this.timeout_engine.clear('vote');
    if (this.role === Role.leader) {
      return;
    }

    if (this.role !== Role.candidate) {
      this.role = Role.candidate;
      this.emit('role_change', this.role);
    }
    this.state.current_term++;
    this.state.voted_for = this.my_addr;

    this.save_state();

    const idx = this.db.last_log_idx();
    const term = this.db.log_term(idx);

    for (const [peer_addr, peer] of this.peers) {
      this.reset_inflight_messages(peer);
      peer.vote_granted = false;
      const message_id = this.unique_message_id();
      const message = new VoteRequest(
        message_id,
        this.my_addr,
        peer_addr,
        this.state.current_term,
        idx,
        term,
        false
      );
      // Register-before-send: see candidate_start_prevote for rationale.
      this.timeout_engine.set(
        message_id.toString(),
        this.message_timeout_ms,
        () => {
          peer.inflight_messages.delete(message_id);
        }
      );
      peer.inflight_messages.set(message_id, message);
      this.send(peer, message);
    }
    this.reset_vote_timeout();

    // Same eager-majority check as in prevote so a single-node cluster (or a
    // configuration where self alone is a quorum) can become leader without
    // waiting for nonexistent peer responses.
    if (this.has_vote_majority()) {
      this.promote_to_leader(true);
    }
  }

  unique_message_id(): bigint {
    // We've used up the persisted slab; reserve another one before handing
    // out the next ID so a crash never leads to ID reuse.
    if (this.last_message_id >= this.message_id_chunk_end) {
      const new_chunk_end = this.last_message_id + this.id_slab_size;
      this.db.kv_set('message_id_chunk', new_chunk_end.toString());
      this.message_id_chunk_end = new_chunk_end;
    }
    this.last_message_id = this.last_message_id + BigInt(1);
    return this.last_message_id;
  }

  send_append_entry(peer: Peer, message: AppendRequest) {
    // Soft cap: a slow peer can otherwise let us accumulate one inflight
    // entry per client request without bound. Drop the oldest so memory
    // stays bounded — the heartbeat will retry whatever the peer needs
    // anyway.
    while (peer.inflight_messages.size >= this.max_inflight_per_peer) {
      const oldest = peer.inflight_messages.keys().next().value;
      if (oldest === undefined) break;
      peer.inflight_messages.delete(oldest);
      this.timeout_engine.clear(oldest.toString());
    }

    this.reset_heartbeat(peer);

    // Register-before-send so a synchronous transport's response cannot beat
    // the inflight-tracking insert and get silently dropped by
    // pop_inflight_message. We capture the logs before clearing them off
    // the message because we mutate it in place to free the payload.
    const id = message.id;
    this.timeout_engine.set(id.toString(), this.message_timeout_ms, () => {
      peer.inflight_messages.delete(id);
      // We could retry sooner than the heartbeat (we know this peer has
      // missing logs) but the peer might also just be down, so we let the
      // heartbeat or the next append kickstart communication again.
    });
    // We keep a copy of the request without the log payload because logs can
    // be large and we don't want to retain them in memory longer than
    // needed; the inflight entry is only used to correlate the response.
    const tracked = new AppendRequest(
      message.id,
      message.from,
      message.to,
      message.term,
      message.prev_idx,
      message.prev_term,
      message.commit_idx,
      message.last_idx,
      []
    );
    peer.inflight_messages.set(message.id, tracked);

    this.send(peer, message);
  }

  leader_append_entry(log: Log): void {
    if (this.role !== Role.leader) {
      return;
    }
    const idx = this.db.last_log_idx();
    const term = this.db.log_term(idx);

    log.idx = idx + BigInt(1);
    log.term = this.state.current_term;

    this.db.add_log_to_storage(log);
    for (const [, peer] of this.peers) {
      if (
        peer.accepting_optimistic_appends ||
        peer.inflight_messages.size === 0
      ) {
        // 'this.my_addr' doubles as the leader_id field of the message
        const message = new AppendRequest(
          this.unique_message_id(),
          this.my_addr,
          peer.addr,
          this.state.current_term,
          idx,
          term,
          this.state.commit_idx,
          log.idx,
          [log]
        );
        this.send_append_entry(peer, message);
      }
    }
  }

  send(peer: Peer, message: Message): void {
    this.messaging_engine.send(peer.addr, message);
  }

  on_append_request(msg: AppendRequest, peer: Peer): void {
    let message: Message;
    if (this.state.current_term < msg.term) {
      this.step_down(msg.term);
    }

    //reject because I'm on a bigger term
    if (this.state.current_term > msg.term) {
      message = new AppendResponse(
        msg.id,
        this.my_addr,
        msg.from,
        this.state.current_term,
        false
      );
      this.messaging_engine.send(peer.addr, message);
      return;
    }

    //reject becaue I'm on a bigger index
    if (msg.prev_idx < this.state.commit_idx) {
      message = new AppendResponse(
        msg.id,
        this.my_addr,
        msg.from,
        this.state.current_term,
        false
      );
      this.reset_vote_timeout();
      this.send(peer, message);
      return;
    }

    const idx = this.db.last_log_idx();
    const term = this.db.log_term(idx);

    if (this.current_leader !== msg.from) {
      this.current_leader = msg.from;
      this.emit('current_leader', msg.from);
    }

    // if the prev index is zero, you have to have found a common log

    // if the prev index of the server is less or equal to the last log,
    // and the terms of the prev index match, you have found a common log

    // a common log implies everything before it is equal
    const found_common_log =
      (msg.prev_idx <= idx &&
        this.db.log_term(msg.prev_idx) === msg.prev_term) ||
      msg.prev_idx === BigInt(0);

    if (!found_common_log) {
      this.emit('no_common_log', msg.prev_term, msg.prev_idx, idx, term);
      const failure_message = new AppendResponse(
        msg.id,
        this.my_addr,
        msg.from,
        this.state.current_term,
        false
      );
      this.reset_vote_timeout();
      this.send(peer, failure_message);
      return;
    }

    if (msg.prev_idx !== idx) {
      //delete all logs AFTER msg.prev_idx
      this.emit('delete_dirty', msg.prev_idx);
      this.db.delete_invalid_logs_from_storage(msg.prev_idx);
    }

    let conf_log: Log = Log.make_empty();
    msg.logs.forEach(log => {
      this.db.add_log_to_storage(log);
      if (log.type === LogType.config) {
        conf_log = log;
      }
    });
    if (conf_log.type === LogType.config && conf_log.data) {
      const config = parse_config_log(conf_log.data, this.logger);
      if (config) {
        this.emit('peer_config_change', config);
        this.state.config_idx = conf_log.idx;
        this.state.peer_addresses = config.new_peers;
        this.state.peer_addresses_old = config.old_peers;
        this.reconcile_peers_with_state();
      }
    }
    const previous_commit_idx = this.state.commit_idx;
    // commit_idx must be monotonically increasing on a follower (Raft §5.3).
    // Only advance if the leader's commit_idx is strictly greater.
    if (msg.commit_idx > this.state.commit_idx) {
      this.state.commit_idx = bigint_min(
        msg.commit_idx,
        this.db.last_log_idx()
      );
    }
    if (
      previous_commit_idx !== this.state.commit_idx ||
      conf_log.type === LogType.config
    ) {
      this.save_state();
    }
    this.reset_vote_timeout();
    message = new AppendResponse(
      msg.id,
      this.my_addr,
      msg.from,
      this.state.current_term,
      true
    );
    this.send(peer, message);
  }

  update_commit_idx() {
    const last_log_idx = this.db.last_log_idx();
    const idxes = [];
    const idxes_old = [];

    if (this.state.peer_addresses.indexOf(this.my_addr) !== -1) {
      idxes.push(last_log_idx);
    }

    if (this.state.peer_addresses_old.indexOf(this.my_addr) !== -1) {
      idxes_old.push(last_log_idx);
    }

    for (const [, peer] of this.peers) {
      if (peer.is_in_old_config) {
        idxes_old.push(peer.match_idx);
      }
      if (peer.is_in_new_config) {
        idxes.push(peer.match_idx);
      }
    }

    // descending sort using a numeric comparator; the default Array.sort
    // coerces to strings which is wrong for bigints (e.g. "10" < "2").
    idxes.sort(bigint_cmp).reverse();
    idxes_old.sort(bigint_cmp).reverse();

    // 1 num = 1 idx = 0 : floor(1/2) = 0 rev_idx = 0
    // 2 num = 2 idx = 1 : floor(2/2) = 1 rev_idx = 0
    // 3 num = 2 idx = 1 : floor(3/2) = 1 rev_idx = 0
    // 4 num = 3 idx = 2 : floor(4/2) = 2 rev_idx = 1
    // 5 num = 3 idx = 2 : floor(5/2) = 2 rev_idx = 1
    const idx_new = Math.floor(idxes.length / 2);
    const new_idx_new = idxes[idx_new];

    let new_idx = new_idx_new;
    if (idxes_old.length) {
      const idx_old = Math.floor(idxes_old.length / 2);
      const new_idx_old = idxes_old[idx_old];
      new_idx = bigint_min(new_idx_new, new_idx_old);
    }

    if (new_idx > this.state.commit_idx) {
      /*
        // our implementation has new leaders send a no-op which guarantees
        // the latest log to be commited will have the current term
        // only update the commit index if it's part of the current leader term
        const term = this.db.log_term(new_idx)
        if (term !== this.state.current_term) {
          return false
        }
        */
      this.state.commit_idx = new_idx;
      if (
        this.state.peer_addresses_old.length !== 0 &&
        this.state.config_idx <= this.state.commit_idx
      ) {
        const log = Log.make_empty();
        log.data = Buffer.from(
          JSON.stringify({
            old_peers: [],
            new_peers: this.state.peer_addresses,
          })
        );
        log.type = LogType.config;
        this.leader_append_entry(log);
        this.state.peer_addresses_old = [];
        this.reconcile_peers_with_state();
        this.state.config_idx = log.idx;
      }
      this.save_state();
      return true;
    }
    return false;
  }

  on_append_response(
    request: AppendRequest,
    msg: AppendResponse,
    peer: Peer
  ): void {
    // If the responder is on a strictly higher term, we must step down.
    // Without this, a stale leader would keep retrying forever after a
    // partition heals.
    if (msg.term > this.state.current_term) {
      this.step_down(msg.term);
      return;
    }

    if (msg.success === false) {
      // We're still searching for a common log: even though the search is
      // ongoing the peer is responding in a favorable way.
      peer.is_responding_favorably = true;
      // Floor at 1 (logs are 1-indexed); decrementing past 1 lands on
      // prev_idx=0 which is the well-defined "before-the-log" sentinel.
      if (peer.next_idx > BigInt(1)) {
        peer.next_idx = peer.next_idx - BigInt(1);
      } else {
        peer.next_idx = BigInt(1);
      }
      // shutoff optimistic appends
      peer.accepting_optimistic_appends = false;
      // clear any inflight messages which may have optimistic appends which we'll ignore
      this.reset_inflight_messages(peer);
      // send a new append request
      const idx = peer.next_idx;
      const term = this.db.log_term(idx);

      const logs = this.db.get_logs_after(idx);
      const last_idx = this.db.last_log_idx();
      const message = new AppendRequest(
        this.unique_message_id(),
        this.my_addr,
        msg.from,
        this.state.current_term,
        idx,
        term,
        this.state.commit_idx,
        last_idx,
        logs
      );
      this.send_append_entry(peer, message);
    } else {
      // we've made progress
      peer.is_responding_favorably = true;
      peer.match_idx = request.last_idx;
      this.update_commit_idx();

      if (peer.accepting_optimistic_appends) {
        return;
      } else {
        peer.accepting_optimistic_appends = true;
        if (request.last_idx < this.db.last_log_idx()) {
          // new entries were appended since the request was made,
          // and optimistic appends was turned off, so we kick-off an append
          const prev_idx = request.last_idx;
          const prev_term = this.db.log_term(prev_idx);
          const logs = this.db.get_logs_after(request.last_idx);
          const last_idx = this.db.last_log_idx();
          const message = new AppendRequest(
            this.unique_message_id(),
            this.my_addr,
            msg.from,
            this.state.current_term,
            prev_idx,
            prev_term,
            this.state.commit_idx,
            last_idx,
            logs
          );
          this.send_append_entry(peer, message);
        }
      }
    }
  }

  reset_heartbeat(peer: Peer): void {
    this.timeout_engine.set(
      `heartbeat_${peer.addr}`,
      this.leader_heartbeat_ms,
      () => {
        const idx = this.db.last_log_idx();
        const term = this.db.log_term(idx);
        const message = new AppendRequest(
          this.unique_message_id(),
          this.my_addr,
          peer.addr,
          this.state.current_term,
          idx,
          term,
          this.state.commit_idx,
          idx,
          []
        );
        this.send_append_entry(peer, message);
      }
    );
  }

  on_vote_response(request: VoteRequest, msg: VoteResponse, peer: Peer): void {
    if (msg.term > this.state.current_term) {
      this.step_down(msg.term);
      return;
    }
    if (msg.vote_granted) {
      peer.vote_granted = true;
    }
    if (this.has_vote_majority()) {
      if (this.is_prevoting === true) {
        this.is_prevoting = false;
        this.candidate_start_vote();
      } else {
        this.promote_to_leader(true);
      }
    }
  }

  on_vote_request(msg: VoteRequest, peer: Peer): void {
    if (msg.is_test === false && this.state.current_term < msg.term) {
      this.step_down(msg.term);
    }
    let vote_granted = false;
    const idx = this.db.last_log_idx();
    const term = this.db.log_term(idx);
    // the most logic-intense section of the code
    const msg_log_not_behind =
      msg.last_term > term || (msg.last_term === term && msg.last_idx >= idx);

    if (msg_log_not_behind) {
      if (msg.is_test) {
        vote_granted = this.state.current_term < msg.term;
      } else {
        vote_granted =
          this.state.current_term === msg.term &&
          (this.state.voted_for === null || this.state.voted_for === msg.from);
      }
    }
    if (vote_granted && msg.is_test === false) {
      this.state.voted_for = msg.from;
      this.save_state();
      this.reset_vote_timeout();
    }
    const message = new VoteResponse(
      msg.id,
      this.my_addr,
      msg.from,
      this.state.current_term,
      vote_granted
    );
    this.messaging_engine.send(peer.addr, message);
  }

  /**
   * Read-only snapshot of the server's state suitable for metrics export
   * or operator dashboards. Pulls everything from in-memory state — no disk
   * read involved — so it is safe to call at high frequency.
   */
  metrics(): ServerMetrics {
    const peers: PeerMetrics[] = [];
    for (const [, peer] of this.peers) {
      peers.push({
        addr: peer.addr,
        match_idx: peer.match_idx,
        next_idx: peer.next_idx,
        inflight_count: peer.inflight_messages.size,
        is_in_old_config: peer.is_in_old_config,
        is_in_new_config: peer.is_in_new_config,
        is_responding_favorably: peer.is_responding_favorably,
        vote_granted: peer.vote_granted,
      });
    }
    return {
      my_addr: this.my_addr,
      role: this.role,
      current_term: this.state.current_term,
      current_leader: this.current_leader,
      commit_idx: this.state.commit_idx,
      config_idx: this.state.config_idx,
      last_log_idx: this.db.last_log_idx(),
      voted_for: this.state.voted_for,
      is_prevoting: this.is_prevoting,
      peer_addresses: [...this.state.peer_addresses],
      peer_addresses_old: [...this.state.peer_addresses_old],
      peers,
    };
  }
}

interface ParsedConfig {
  old_peers: string[];
  new_peers: string[];
}

// Validate that the bytes on disk really represent a `{old_peers, new_peers}`
// shape. Returns null on any error (parse, type, structure) and logs at
// error level so operators can see corruption rather than silently crashing
// later when we try to iterate the peers.
function parse_config_log(data: Buffer, logger: Logger): ParsedConfig | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(data.toString());
  } catch (err) {
    logger.error?.('raft: config log payload is not valid JSON', { err });
    return null;
  }
  if (
    !parsed ||
    typeof parsed !== 'object' ||
    !Array.isArray((parsed as ParsedConfig).old_peers) ||
    !Array.isArray((parsed as ParsedConfig).new_peers) ||
    (parsed as ParsedConfig).old_peers.some(p => typeof p !== 'string') ||
    (parsed as ParsedConfig).new_peers.some(p => typeof p !== 'string')
  ) {
    logger.error?.('raft: config log payload has unexpected shape', {
      parsed,
    });
    return null;
  }
  return parsed as ParsedConfig;
}
