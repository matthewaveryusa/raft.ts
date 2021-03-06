import {
  AbstractMessagingEngine,
  AbstractStorageEngine,
  AbstractTimeoutEngine,
} from './interfaces';
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
import { threadId } from 'worker_threads';

// raft engine
export interface LogIdxTerm {
  idx: bigint;
  term: bigint;
}

export interface ClientError {
  error: string;
  leader: string | null;
}

function bigint_min(lhs: bigint, rhs: bigint): bigint {
  if (lhs < rhs) {
    return lhs;
  }
  return rhs;
}

class Peer {
  addr: string;
  vote_granted: boolean;
  match_idx: bigint;
  next_idx: bigint;
  inflight_messages: Map<bigint, Message>;
  max_message_id: bigint;
  accepting_optimistic_appends: boolean;
  is_in_old_config: boolean;
  is_in_new_config: boolean;
  is_responding_favorably: boolean;

  constructor(
    addr: string,
    is_in_old_config: boolean,
    is_in_new_config: boolean
  ) {
    this.addr = addr;
    this.vote_granted = false;
    this.match_idx = BigInt(0);
    this.next_idx = BigInt(1);
    this.inflight_messages = new Map();
    this.max_message_id = BigInt(0);
    this.accepting_optimistic_appends = true;
    this.is_in_old_config = is_in_old_config;
    this.is_in_new_config = is_in_new_config;
    this.is_responding_favorably = false;
  }
}

export class Server extends EventEmitter {
  readonly my_addr: string;
  private timeout_engine: AbstractTimeoutEngine;
  private messaging_engine: AbstractMessagingEngine;
  private db: AbstractStorageEngine;

  private readonly timeout_ms: number;
  private readonly leader_heartbeat_ms: number;
  private readonly message_timeout_ms: number;
  private readonly id_slab_size: bigint;

  private role: Role;
  private current_leader: string | null;
  private peers: Map<string, Peer>;
  private next_message_id: bigint;
  private state: State;
  private is_prevoting: boolean;

  constructor(
    address: string,
    peers_bootstrap: string[],
    db: AbstractStorageEngine,
    timeout_engine: AbstractTimeoutEngine,
    messaging_engine: AbstractMessagingEngine,
    id_slab_size?: bigint
  ) {
    super();
    // there's a lot of state so the constructor is pretty sizeable
    this.timeout_engine = timeout_engine;
    this.messaging_engine = messaging_engine;
    this.db = db;
    this.id_slab_size = id_slab_size || BigInt(1000000);
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

    const next_message_id = this.db.kv_get('message_id_chunk');
    let chunk = BigInt(0);
    if (next_message_id) {
      chunk = BigInt(next_message_id);
      this.next_message_id = chunk + BigInt(1);
    } else {
      this.next_message_id = BigInt(1);
    }
    this.db.kv_set('message_id_chunk', (BigInt(1000000) + chunk).toString());

    const state = this.get_state();
    if (state) {
      this.state = state;
      const idx = this.db.last_log_idx();
      if (idx !== BigInt(0)) {
        const last_log = this.db.get_logs_after(idx - BigInt(1))[0];
        if (last_log.type === LogType.config && last_log.data !== null) {
          // state is written after config is written to log.
          // if crash occurs between the writes, state has an invalid peer set
          // so if the last log is a config, we populate the state with
          // the peers in the last log
          const config = JSON.parse(last_log.data.toString());
          this.state.peer_addresses = config.new_peers;
          this.state.peer_addresses_old = config.old_peers;
          this.state.config_idx = last_log.idx;
          this.save_state();
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

  // make this server think it's the leader
  // convenient for testing
  promote_to_leader(send_noop = false): void {
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
    for (const [peer_addr, p] of this.peers) {
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
    for (const [peer_name, peer] of this.peers) {
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

    // go through old peers and add them as old peers if they are not in the set
    // if they are part of the set, flip se the old config flag to true

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

    if (peer.max_message_id >= msg.id) {
      // received out of order old message, ignoring
      return;
    }

    peer.max_message_id = msg.id;

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
    // count old config vote
    const num_old_peers = this.state.peer_addresses_old.length;
    if (num_old_peers !== 0) {
      // +1 is so required number is rounded up
      // in case  peers+1 is even
      let self_in_old = 0;
      if (this.state.peer_addresses_old.indexOf(this.my_addr) !== -1) {
        self_in_old = 1;
      }
      const min_votes = Math.ceil((num_old_peers + 1) / 2);
      let count = self_in_old; // voting for self
      for (const [peer_name, peer] of this.peers) {
        if (peer.vote_granted && peer.is_in_old_config) {
          count += 1;
        }
      }
      if (count < min_votes) {
        return false;
      }
    }

    // count old config vote
    const num_new_peers = this.state.peer_addresses.length;
    if (num_new_peers !== 0) {
      // +1 is so required number is rounded up
      // in case  peers+1 is even
      let self_in_new = 0;
      if (this.state.peer_addresses.indexOf(this.my_addr) !== -1) {
        self_in_new = 1;
      }
      const min_votes = Math.ceil((num_new_peers + 1) / 2);
      let count = self_in_new; // voting for self
      for (const [peer_name, peer] of this.peers) {
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

  get_state(): State | null {
    const state = this.db.kv_get('state');
    if (!state) {
      return null;
    }
    return State.make(state);
  }

  save_state(): void {
    // for debug logging purposes only
    const old_state = this.get_state();

    this.db.kv_set('state', this.state.toString());

    if (!old_state) {
      return;
    }
    if (old_state.current_term !== this.state.current_term) {
      this.emit('term', this.state.current_term);
    }
    if (old_state.commit_idx !== this.state.commit_idx) {
      this.emit('commit_idx', this.state.commit_idx);
    }

    if (old_state.voted_for !== this.state.voted_for) {
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
    for (const [id, message] of peer.inflight_messages) {
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
      // make sure we got some sort of message from the majority of the cluster
      // within the vote timeout
      // if we did, we continue to be the
      // if we didn't, then we want to avoid perpetuating a split-brain, so we step down
      // and try to win a vote
      let num_messages = 0;
      for (const [peer_addr, peer] of this.peers) {
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
      this.send(peer, message);
      this.timeout_engine.set(
        message_id.toString(),
        this.message_timeout_ms,
        () => {
          peer.inflight_messages.delete(message_id);
        }
      );
      peer.inflight_messages.set(message_id, message);
    }
    this.reset_vote_timeout();
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
      this.send(peer, message);
      this.timeout_engine.set(
        message_id.toString(),
        this.message_timeout_ms,
        () => {
          peer.inflight_messages.delete(message_id);
        }
      );
      peer.inflight_messages.set(message_id, message);
    }
    this.reset_vote_timeout();
  }

  unique_message_id(): bigint {
    if (this.next_message_id % this.id_slab_size === BigInt(0)) {
      this.db.kv_set(
        'message_id_chunk',
        (this.next_message_id + this.id_slab_size).toString()
      );
    }
    this.next_message_id++;
    return this.next_message_id;
  }

  send_append_entry(peer: Peer, message: AppendRequest) {
    this.reset_heartbeat(peer);
    this.send(peer, message);

    this.timeout_engine.set(
      message.id.toString(),
      this.message_timeout_ms,
      () => {
        peer.inflight_messages.delete(message.id);
        // todo, maybe we should not wait for as long as the heartbeat since we know that this peer
        // has missing logs, but also the peer isn't responding, so it may be down.
        // for now we wait for the heartbeat, or a new append, to kickstart communication again
      }
    );

    // We delete the logs from the saved request as the logs can get large
    // so and we don't want to save them too long in-memory
    message.logs = [];
    peer.inflight_messages.set(message.id, message);
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
    for (const [peer_addr, peer] of this.peers) {
      if (
        peer.accepting_optimistic_appends ||
        peer.inflight_messages.size === 0
      ) {
        // from: 'this.me' doubles up as leader_id
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
      const config = JSON.parse(conf_log.data.toString());
      this.emit('peer_config_change', config);
      this.state.config_idx = conf_log.idx;
      this.state.peer_addresses = config.new_peers;
      this.state.peer_addresses_old = config.old_peers;
      this.reconcile_peers_with_state();
    }
    const previous_commit_idx = this.state.commit_idx;
    this.state.commit_idx = bigint_min(msg.commit_idx, this.db.last_log_idx());
    if (previous_commit_idx !== this.state.commit_idx || conf_log !== null) {
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

    for (const [peer_name, peer] of this.peers) {
      if (peer.is_in_old_config) {
        idxes_old.push(peer.match_idx);
      }
      if (peer.is_in_new_config) {
        idxes.push(peer.match_idx);
      }
    }

    idxes.sort().reverse();
    idxes_old.sort().reverse();

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
    // const peer_state = this.peers[msg.from]
    if (msg.success === false) {
      // step back one
      if (peer.next_idx <= BigInt(0)) {
        // The peer has rejected everything!
        // The least surprising thing to do is to retry, starting by sending the last
        // log on ther server?
        // TODO log this?
        peer.next_idx = this.db.last_log_idx() - BigInt(1);
      } else {
        // we're still searching for a common log,
        // even if the search is ongoing, the peer is
        // responding in a favoriable way
        peer.is_responding_favorably = true;
        peer.next_idx = peer.next_idx - BigInt(1);
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
}
