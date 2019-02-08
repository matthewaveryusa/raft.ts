import {AbstractMessagingEngine, AbstractStorageEngine, AbstractTimeoutEngine} from './interfaces'
import {AppendRequest, AppendResponse, Log, LogType, Message, MessageType, VoteRequest, VoteResponse} from './messages'
import {Role, State} from './state'

// raft engine
export interface ILogIdxTerm {
    idx: bigint
    term: bigint
}

export interface IClientError {
    error: string
    leader: string | null
}

function bigint_min(lhs: bigint, rhs: bigint): bigint {
    if (lhs < rhs) {
      return lhs
    }
    return rhs
  }

function bigint_max(lhs: bigint, rhs: bigint): bigint {
    if (lhs > rhs) {
      return lhs
    }
    return rhs
  }

function monotonic_ms(): number {
    const [seconds, nanoseconds] = process.hrtime()
    return seconds * 1000 + Math.round(nanoseconds / 1e6)
  }

class Peer {
    public addr: string
    public vote_granted: boolean
    public match_idx: bigint
    public next_idx: bigint
    public inflight_messages: Map<bigint, Message>
    public max_message_id: bigint
    public accepting_optimistic_appends: boolean

    constructor(addr: string) {
        this.addr = addr
        this.vote_granted = false
        this.match_idx = BigInt(0)
        this.next_idx = BigInt(1)
        this.inflight_messages = new Map()
        this.max_message_id = BigInt(0)
        this.accepting_optimistic_appends = true
    }

}

export class Server {
    public readonly my_addr: string
    private timeout_engine: AbstractTimeoutEngine
    private messaging_engine: AbstractMessagingEngine
    private db: AbstractStorageEngine

    private readonly timeout_ms: number
    private readonly leader_heartbeat_ms: number
    private readonly message_timeout_ms: number

    private role: Role
    private current_leader: string|null
    private peers: Map<string, Peer>
    private next_message_id: bigint
    private state: State
    private last_vote_granted: number

    constructor(address: string,
                peers_bootstrap: string[],
                db: AbstractStorageEngine,
                timeout_engine: AbstractTimeoutEngine,
                messaging_engine: AbstractMessagingEngine) {
      // there's a lot of state so the constructor is pretty sizeable

      this.timeout_engine = timeout_engine
      this.messaging_engine = messaging_engine
      this.db = db

      this.last_vote_granted = 0

      this.timeout_ms = 5000
      this.leader_heartbeat_ms = this.timeout_ms / 2
      this.message_timeout_ms = this.timeout_ms
      this.my_addr = address

      // volatile state
      this.role = Role.follower
      this.current_leader = null
      this.peers = new Map()

      const next_message_id = this.db.kv_get('message_id_chunk')
      let chunk = BigInt(0)
      if (next_message_id) {
        chunk = BigInt(next_message_id)
        this.next_message_id = chunk + BigInt(1)
      } else {
        this.next_message_id = BigInt(1)
      }
      this.db.kv_set('message_id_chunk', (BigInt(1000000) + chunk).toString())

      const state = this.get_state()
      if (state) {
          this.state = state
      } else {
        this.state = new State()
        peers_bootstrap.forEach((peer_addr) => this.state.peer_addresses.push(peer_addr.toString()))
        this.save_state()
      }

      this.state.peer_addresses.forEach((peer_addr) => {
        if (peer_addr === this.my_addr) {
          return
        }

        this.peers.set(peer_addr, new Peer(peer_addr))
      })

    }

    // make this server think it's the leader
    // convenient for testing
    public promote_to_leader(send_noop: boolean = false): void {
      this.timeout_engine.clear('vote')
      this.role = Role.leader
      this.log('promoted to leader')
      this.current_leader = this.my_addr
      this.reset_volatile_peer_state()
      for (const [peer_addr, p] of this.peers) {
        this.reset_heartbeat(p)
      }
      if (send_noop) {
        const log = new Log()
        log.type = LogType.noop
        log.data =  null
        this.leader_append_entry(log)
      }
    }

    public start_server(): void {
      this.reset_vote_timeout()
      this.messaging_engine.start(this.my_addr, (msg: Message) => this.on_message(msg) )
    }

    public on_client_request(data: Buffer | null): ILogIdxTerm | IClientError {
        if (this.role !== Role.leader) {
          return { error: 'not_leader', leader: this.current_leader }
        }
        const log = new Log()
        log.data = data
        log.type = LogType.message
        this.leader_append_entry(log)
        return { idx: log.idx, term: log.term }
    }

    public on_message(msg: Message) {
      const peer = this.peers.get(msg.from)
      if (!peer) {
        this.log('invalid peer', msg.from)
        return
      }

      if (peer.max_message_id >= msg.id) {
        // received out of order old message, ignoring
        this.log(`received out of order message, currently @ ${peer.max_message_id}`, msg)
        return
      }

      peer.max_message_id = msg.id

      switch (msg.type) {
        case MessageType.vote_request:
          this.on_vote_request(msg as VoteRequest)
          break
        case MessageType.vote_response:
          {
          const request = this.pop_inflight_message(peer, msg.id)
          if (request === null) { return }
          this.on_vote_response(msg as VoteResponse)
          }
          break
        case MessageType.append_request:
          this.on_append_request(msg as AppendRequest)
          break
        case MessageType.append_response:
          {
          const request = this.pop_inflight_message(peer, msg.id) as AppendRequest
          if (request === null) { return }
          this.on_append_response(request, msg as AppendResponse)
          }
          break
        default:
          this.log('unknown message type', msg.type)
      }
    }

    public has_vote_majority(): boolean {
      // first +1 is because self isn't in peers list
      // second +1 is so required number is rounded up
      // in case  peers+1 is even
      const min_votes = Math.ceil((this.peers.size + 1 + 1) / 2)
      let count = 1 // voting for self
      for (const [peer_name, peer] of this.peers) {
        if (peer.vote_granted) {
          count += 1
        }
      }
      return count >= min_votes
    }

    public log(...args: any): void {
      console.log(new Date().toISOString(), this.my_addr, this.state.current_term, ...args)
    }

    public get_state(): State|null {
      const state = this.db.kv_get('state')
      if (!state) {
        return null
      }
      return State.make(state)
    }

    public save_state(): void {
      // for debug logging purposes only
      const old_state = this.get_state()

      this.db.kv_set('state', this.state.toString())

      const log = []
      if (!old_state) {
        return
      }
      if (old_state.current_term !== this.state.current_term) {
        log.push(`term: ${old_state.current_term} -> ${this.state.current_term}`)
      }
      if (old_state.commit_idx !== this.state.commit_idx) {
        log.push(`commit_index: ${old_state.commit_idx} -> ${this.state.commit_idx}`)
      }

      if (old_state.voted_for !== this.state.voted_for) {
        log.push(`voted_for ${old_state.voted_for} -> ${this.state.voted_for}`)
      }
      if (log.length) {
        this.log('state updated', log.join(','))
      }
    }

    public reset_vote_timeout() {
      this.timeout_engine.set_varied('vote', this.timeout_ms, () => this.candidate_start_vote())
    }

    public step_down(term: bigint): void {
      this.log(`stepping down, was ${this.state.current_term}, now ${term}`)
      this.state.current_term = term
      this.state.voted_for = null
      this.save_state()

      this.role = Role.follower
      this.log('follower')
      this.current_leader = null
      this.reset_volatile_peer_state()
      this.reset_vote_timeout()
    }

    public reset_inflight_messages(peer: Peer): void {
      for (const [id, message] of peer.inflight_messages) {
        this.timeout_engine.clear(id.toString())
        this.log(`removed inflight message ${id}`)
      }
      peer.inflight_messages = new Map()
    }

    public pop_inflight_message(peer: Peer, id: bigint): Message|null {
      const message = peer.inflight_messages.get(id)
      if (message) {
        peer.inflight_messages.delete(id)
        this.timeout_engine.clear(id.toString())
      } else {
        return null
      }
      return message
    }

    public reset_volatile_peer_state(): void {
      for (const [peer_addr, peer] of this.peers) {
        this.timeout_engine.clear(`heartbeat_${peer_addr}`)

        // only address and port are not volatile
        peer.vote_granted = false
        peer.match_idx = BigInt(0)
        peer.next_idx = this.db.last_log_idx() + BigInt(1)
        this.reset_inflight_messages(peer)
        peer.accepting_optimistic_appends = true
        peer.max_message_id = BigInt(0)
      }
    }

    public candidate_start_vote(): void {
      this.timeout_engine.clear('vote')
      if (this.role === Role.leader) {
        throw Error(`I'm a leader, I can't start a vote`)
      }
      this.log('candidate')

      this.role = Role.candidate
      this.state.current_term++
      this.state.voted_for = this.my_addr

      this.save_state()

      const idx = this.db.last_log_idx()
      const term = this.db.log_term(idx)

      for (const [peer_addr, peer] of this.peers) {
        this.reset_inflight_messages(peer)
        const message_id = this.unique_message_id()
        const message = new VoteRequest(message_id, this.my_addr, peer_addr,
          this.state.current_term, idx, term)
        this.send(peer, message)
        this.timeout_engine.set(message_id.toString(), this.message_timeout_ms, () => {
          if (peer.inflight_messages.delete(message_id) !== true) {
            throw Error('clearing vote message but already cleared')
          }
        })
        peer.inflight_messages.set(message_id, message)
      }
      this.reset_vote_timeout()
    }

    public unique_message_id(): bigint {
      if (this.next_message_id % BigInt(1000000) === BigInt(0)) {
        this.db.kv_set('message_id_chunk', (this.next_message_id + BigInt(1000000)).toString())
      }
      this.next_message_id++
      return this.next_message_id
    }

    public send_append_entry(peer: Peer, message: AppendRequest) {
      this.reset_heartbeat(peer)
      this.send(peer, message)

      this.timeout_engine.set(message.id.toString(), this.message_timeout_ms, () => {
        if (peer.inflight_messages.delete(message.id) !== true) {
          throw Error('clearing append_entry message but already cleared')
        }

        // todo, maybe we should not wait for as long as the heartbeat since we know that this peer
        // has missing logs, but also the peer isn't responding, so it may be down.
        // for now we wait for th heartbeat, or a new append, to kickstart communication again
      })

      peer.inflight_messages.set(message.id, message)
    }

    public leader_append_entry(log: Log): void {
      if (this.role !== Role.leader) {
        throw Error('error, appending log but not leader')
      }
      const idx = this.db.last_log_idx()
      const term = this.db.log_term(idx)

      log.idx = idx + BigInt(1)
      log.term = this.state.current_term

      this.db.add_log_to_storage(log)
      for (const [peer_addr, peer] of this.peers) {
        if (peer.accepting_optimistic_appends || peer.inflight_messages.size === 0) {
          // from: 'this.me' doubles up as leader_id
          const message = new AppendRequest(this.unique_message_id(),
            this.my_addr,
            peer.addr,
            this.state.current_term,
            idx, term,
            this.state.commit_idx,
            log.idx,
            [log],
          )
          this.send_append_entry(peer, message)
        }
      }
    }

    public send(peer: Peer, message: Message): void {
      this.messaging_engine.send(peer.addr, message)
    }

    public on_append_request(msg: AppendRequest): void {
      if (this.state.current_term < msg.term) {
        this.step_down(msg.term)
      }

      const peer = this.peers.get(msg.from)
      if (!peer) {
          return
      }

      if (this.state.current_term > msg.term) {
        this.log(`rejecting append request from ${msg.from}, term ${this.state.current_term} > ${msg.term}`)
        const response = new AppendResponse(msg.id, this.my_addr, msg.from, this.state.current_term, false)
        this.messaging_engine.send(peer.addr, response)
        return
      }

      const idx = this.db.last_log_idx()
      const term = this.db.log_term(idx)

      if (this.current_leader !== msg.from) {
        this.current_leader = msg.from
      }

      // if the prev index is zero, you have to have found a common log

      // if the prev index of the server is less or equal to the last log,
      // and the terms of the prev index match, you have found a common log

      // a common log implies everything before it is equal
      const found_common_log = (msg.prev_idx <= idx && this.db.log_term(msg.prev_idx) === msg.prev_term) ||
        msg.prev_idx === BigInt(0)

      if (!found_common_log) {
        this.log(`rejecting append request from ${msg.from}, no common log for prev ${msg.prev_idx}`)
        const message = new AppendResponse(msg.id, this.my_addr, msg.from, this.state.current_term, false)
        this.reset_vote_timeout()
        this.send(peer, message)
        return
      }

      if (msg.last_idx === msg.prev_idx) {
        // this is a heartbeat
        // msg.logs.length should be 0
        const previous_commit_idx = this.state.commit_idx
        this.state.commit_idx = bigint_min(msg.commit_idx, this.db.last_log_idx())
        if (previous_commit_idx !== this.state.commit_idx) {
          this.save_state()
        }
        this.reset_vote_timeout()
        const message = new AppendResponse(msg.id, this.my_addr, msg.from, this.state.current_term, true)
        this.send(peer, message)
        return
      }

      if (msg.logs.length !== 0) {
        if (msg.prev_idx !== idx) {
          this.db.delete_invalid_logs_from_storage(msg.prev_idx)
        }
        msg.logs.forEach((log) => {
          this.db.add_log_to_storage(log)
        })
        this.state.commit_idx = bigint_min(msg.commit_idx, this.db.last_log_idx())
        this.save_state()
        this.reset_vote_timeout()
        const message = new AppendResponse(msg.id, this.my_addr, msg.from, this.state.current_term, true)
        this.send(peer, message)
        return
      }

      // logs were not sent so we need to get them out of band
      // TODO
      throw Error('Out of band fetch not implemented')
    }

    public update_commit_idx() {
      const idxes = [this.db.last_log_idx()]
      for (const [peer_name, peer] of this.peers) {
        idxes.push(peer.match_idx)
      }

      idxes.sort().reverse()
      // 1 num = 1 idx = 0 : floor(1/2) = 0 rev_idx = 0
      // 2 num = 2 idx = 1 : floor(2/2) = 1 rev_idx = 0
      // 3 num = 2 idx = 1 : floor(3/2) = 1 rev_idx = 0
      // 4 num = 3 idx = 2 : floor(4/2) = 2 rev_idx = 1
      // 5 num = 3 idx = 2 : floor(5/2) = 2 rev_idx = 1
      const idx = Math.floor((this.peers.size + 1) / 2)
      const new_idx = idxes[idx]
      if (new_idx > this.state.commit_idx) {
        this.state.commit_idx = new_idx
        this.save_state()
        return true
      }
      return false
    }

    public on_append_response(request: AppendRequest, msg: AppendResponse): void {
      this.log(`append from ${msg.from} success ${msg.success} term ${msg.term} index ${request.last_idx}` +
      ` pi ${request.prev_idx} pt ${request.prev_term}`)
      if (this.role !== Role.leader) { return }
      const peer = this.peers.get(msg.from)
      if (!peer) { return }
      // const peer_state = this.peers[msg.from]
      if (msg.success === false) {
        // step back one
        if (peer.next_idx === BigInt(0)) {
          throw Error(`${msg.from} failed message, but it was the very first one in the stream`)
        }
        peer.next_idx = bigint_max(BigInt(0), peer.next_idx - BigInt(1))
        // shutoff optimistic appends
        peer.accepting_optimistic_appends = false
        // clear any inflight messages which may have optimistic appends which we'll ignore
        this.reset_inflight_messages(peer)
        // send a new append request
        const idx = peer.next_idx
        const term = this.db.log_term(idx)

        const logs = this.db.get_logs_after(idx)
        if (logs.length) {
          this.log(`append response, sending logs after idx ${idx}, got ${logs.length} logs, ` +
          `idx ${logs[0].idx}-${logs[logs.length - 1].idx}`)
        } else {
          this.log(`append response, sending logs after idx ${idx}, none found`)
        }
        const last_idx = this.db.last_log_idx()
        const message = new AppendRequest(this.unique_message_id(), this.my_addr, msg.from, this.state.current_term,
          idx, term, this.state.commit_idx, last_idx, logs)
        this.send_append_entry(peer, message)
      } else {
        // we've made progress
        peer.match_idx = request.last_idx
        this.update_commit_idx()

        if (peer.accepting_optimistic_appends) {
          return
        } else {
          peer.accepting_optimistic_appends = true
          if (request.last_idx < this.db.last_log_idx()) {
            // new entries were appended since the request was made,
            // and optimistic appends was turned off, so we kick-off an append
            const prev_idx = request.last_idx
            const prev_term = this.db.log_term(prev_idx)
            const logs = this.db.get_logs_after(request.last_idx)
            const last_idx = this.db.last_log_idx()
            const message = new AppendRequest(this.unique_message_id(), this.my_addr, msg.from, this.state.current_term,
             prev_idx, prev_term, this.state.commit_idx, last_idx, logs )
            this.send_append_entry(peer, message)
          }
        }
      }
    }

    public reset_heartbeat(peer: Peer): void {
      this.timeout_engine.set(`heartbeat_${peer.addr}`, this.leader_heartbeat_ms, () => {
        const idx = this.db.last_log_idx()
        const term = this.db.log_term(idx)
        const message = new AppendRequest(this.unique_message_id(), this.my_addr, peer.addr, this.state.current_term,
         idx, term, this.state.commit_idx, idx, [])
        this.send_append_entry(peer, message)
      })
    }

    public on_vote_response(msg: VoteResponse): void {
      this.log(`vote from ${msg.from} granted ${msg.vote_granted} term ${msg.term}`)
      if (this.role !== Role.candidate) { return }
      if (msg.term > this.state.current_term) {
        this.step_down(msg.term)
        return
      }
      const peer = this.peers.get(msg.from)
      if (!peer) { return }
      if (msg.vote_granted) {
        peer.vote_granted = true
      }
      if (this.has_vote_majority()) {
        this.promote_to_leader(true)
      }
    }

    public on_vote_request(msg: VoteRequest): void {
      if (monotonic_ms() - this.last_vote_granted < this.timeout_ms) {
        this.log(`granted vote too recently so ignoring this vote request`)
        return
      }
      const peer =  this.peers.get(msg.from)
      if (!peer) {
          return
      }

      if (this.state.current_term < msg.term) {
        this.step_down(msg.term)
      }
      let vote_granted = false
      const idx = this.db.last_log_idx()
      const term = this.db.log_term(idx)
      const msg_log_not_behind = (msg.last_term > term) || (msg.last_term === term && msg.last_idx >= idx)
      if (this.state.current_term === msg.term &&
        (this.state.voted_for === null || this.state.voted_for === msg.from) &&
        msg_log_not_behind
      ) {
        vote_granted = true
        this.last_vote_granted = monotonic_ms()
        this.state.voted_for = msg.from
        this.save_state()
        this.reset_vote_timeout()
      }
      const message = new VoteResponse(msg.id, this.my_addr, msg.from, this.state.current_term, vote_granted)
      this.messaging_engine.send(peer.addr, message)
    }
}
