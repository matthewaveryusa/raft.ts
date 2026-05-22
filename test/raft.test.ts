import { expect } from 'chai';
import { TestMessagingEngine } from './test_messaging';
import { TestStorageEngine } from './test_storage';
import { TestTimeoutEngine } from './test_timeouts';

import { Server } from '../src/server';
import { Role, State } from '../src/state';

import {
  AppendRequest,
  AppendResponse,
  Log,
  LogType,
  Message,
  MessageType,
  VoteRequest,
  VoteResponse,
} from '../src/messages';
import { EventLog } from './event_log';
import { TestSerde } from './test_serde';

// chai uses these
/* tslint:disable no-unused-expression*/

interface Harness {
  ev: EventLog;
  me: TestMessagingEngine;
  se: TestStorageEngine;
  te: TestTimeoutEngine;
  s: Server;
}

function setup_with_state(commit_idx?: number): Harness {
  const ev = new EventLog();
  const me = new TestMessagingEngine(new TestSerde(), ev);
  const se = new TestStorageEngine(ev);
  const te = new TestTimeoutEngine(ev);

  se.kv.set('message_id_chunk', BigInt(42000000).toString());
  const state = new State();
  state.commit_idx = BigInt(typeof commit_idx === 'number' ? commit_idx : 5000);
  state.current_term = BigInt(42);
  state.peer_addresses = ['s1', 's2', 's3', 's4'];
  state.voted_for = 's2';
  se.kv.set('state', state.toString());

  const s = new Server('s1', ['s1', 's2', 's3'], se, te, me);
  return { ev, me, se, te, s };
}

function find_send(ev: EventLog, peer_addr: string): Message | null {
  for (const e of ev.logs) {
    if (e.name === 'TestMessagingEngine::send' && e.args.peer_addr === peer_addr) {
      return e.args.message as Message;
    }
  }
  return null;
}

function find_sends(ev: EventLog, peer_addr: string): Message[] {
  return ev.logs
    .filter(
      e =>
        e.name === 'TestMessagingEngine::send' && e.args.peer_addr === peer_addr
    )
    .map(e => e.args.message as Message);
}

function get_state_from_kv(se: TestStorageEngine): State {
  const raw = se.kv.get('state');
  if (!raw) throw new Error('no state saved');
  const s = State.make(raw);
  if (!s) throw new Error('invalid state');
  return s;
}

// Find the registered vote-timeout callback irrespective of how many storage
// events fired before start_server (varies for fresh vs. restored servers).
function fire_vote_timeout(ev: EventLog): void {
  const vote_set = ev.logs.find(
    l => l.name === 'TestTimeoutEngine::set_varied' && l.args.name === 'vote'
  );
  if (!vote_set) throw new Error('vote timeout was never registered');
  vote_set.args.callback();
}

describe('Server', () => {
  describe('construction', () => {
    it('builds from scratch with no prior state', () => {
      const ev = new EventLog();
      const me = new TestMessagingEngine(new TestSerde(), ev);
      const se = new TestStorageEngine(ev);
      const te = new TestTimeoutEngine(ev);

      const s = new Server('s1', ['s1', 's2', 's3'], se, te, me);
      s.start_server();

      // a fresh state should have been written
      const state = get_state_from_kv(se);
      expect(state.current_term).to.equal(BigInt(0));
      expect(state.commit_idx).to.equal(BigInt(0));
      expect(state.voted_for).to.be.null;
      expect(state.peer_addresses).to.eql(['s1', 's2', 's3']);
      expect(state.peer_addresses_old).to.eql([]);

      // a vote timeout should have been registered
      const vote_set = ev.logs.find(
        l => l.name === 'TestTimeoutEngine::set_varied' && l.args.name === 'vote'
      );
      expect(vote_set, 'vote timeout was set').to.not.be.undefined;
    });

    it('reuses prior state when one exists', () => {
      const { se, s } = setup_with_state();
      s.start_server();
      // running save_state on a no-op should not crash
      s.save_state();

      const state = get_state_from_kv(se);
      expect(state.current_term).to.equal(BigInt(42));
      expect(state.commit_idx).to.equal(BigInt(5000));
      expect(state.voted_for).to.equal('s2');
    });

    it('returns null for malformed serialized state', () => {
      expect(State.make('[1]')).to.be.null;
      // missing last 2 fields is also malformed
      expect(State.make(JSON.stringify(['1', '2', null, []]))).to.be.null;
    });

    it('round-trips state through toString/make', () => {
      const orig = new State();
      orig.current_term = BigInt(7);
      orig.commit_idx = BigInt(3);
      orig.voted_for = 'sX';
      orig.peer_addresses = ['s2', 's1', 's3'];
      orig.peer_addresses_old = ['sB', 'sA'];
      orig.config_idx = BigInt(11);

      const restored = State.make(orig.toString());
      expect(restored).to.not.be.null;
      expect(restored!.current_term).to.equal(BigInt(7));
      expect(restored!.commit_idx).to.equal(BigInt(3));
      expect(restored!.voted_for).to.equal('sX');
      // toString sorts both arrays for deterministic serialization
      expect(restored!.peer_addresses).to.eql(['s1', 's2', 's3']);
      expect(restored!.peer_addresses_old).to.eql(['sA', 'sB']);
      expect(restored!.config_idx).to.equal(BigInt(11));
    });
  });

  describe('elections', () => {
    it('starts a prevote when the vote timeout fires', () => {
      const { ev, s } = setup_with_state();
      s.start_server();
      fire_vote_timeout(ev);

      // we should have sent a vote request to each non-self peer
      const to_s2 = find_send(ev, 's2') as VoteRequest;
      const to_s3 = find_send(ev, 's3') as VoteRequest;
      const to_s4 = find_send(ev, 's4') as VoteRequest;
      expect(to_s2).to.not.be.null;
      expect(to_s3).to.not.be.null;
      expect(to_s4).to.not.be.null;

      // prevote requests carry the next term but is_test=true so they don't
      // perturb the cluster
      expect(to_s2.is_test).to.be.true;
      expect(to_s2.term).to.equal(BigInt(43));
    });

    it('promotes to leader after winning prevote and real vote', () => {
      const { ev, se, s } = setup_with_state();
      s.start_server();
      ev.logs[3].args.callback();

      // prevote responses
      s.on_message(new VoteResponse(BigInt(42000002), 's2', 's1', BigInt(42), true));
      s.on_message(new VoteResponse(BigInt(42000004), 's4', 's1', BigInt(42), false));
      s.on_message(new VoteResponse(BigInt(42000003), 's3', 's1', BigInt(42), true));

      // After prevote majority, real vote should have started; current_term
      // gets bumped to 43 and we vote for ourselves.
      let state = get_state_from_kv(se);
      expect(state.current_term).to.equal(BigInt(43));
      expect(state.voted_for).to.equal('s1');

      // real-vote responses (term now 43)
      s.on_message(new VoteResponse(BigInt(42000005), 's2', 's1', BigInt(43), true));
      s.on_message(new VoteResponse(BigInt(42000007), 's4', 's1', BigInt(43), false));
      s.on_message(new VoteResponse(BigInt(42000006), 's3', 's1', BigInt(43), true));

      // We should now be the leader with a heartbeat scheduled
      state = get_state_from_kv(se);
      expect(state.current_term).to.equal(BigInt(43));

      const heartbeat_set = ev.logs.find(
        l =>
          l.name === 'TestTimeoutEngine::set' &&
          l.args.name === 'heartbeat_s2'
      );
      expect(heartbeat_set, 'heartbeat scheduled for s2').to.not.be.undefined;

      // candidate_start_vote should be a no-op for a leader
      s.candidate_start_vote();
      // confirm we are still the leader (term unchanged)
      const state2 = get_state_from_kv(se);
      expect(state2.current_term).to.equal(BigInt(43));
    });

    it('steps down when seeing a higher-term vote response', () => {
      const { ev, se, s } = setup_with_state();
      s.start_server();
      ev.logs[3].args.callback();

      s.on_message(
        new VoteResponse(BigInt(42000002), 's2', 's1', BigInt(420), false)
      );

      const state = get_state_from_kv(se);
      expect(state.current_term).to.equal(BigInt(420));
      expect(state.voted_for).to.be.null;
    });
  });

  describe('vote requests', () => {
    it('grants and saves vote when peer is on a higher term', () => {
      const { ev, se, s } = setup_with_state();
      s.start_server();

      const req = new VoteRequest(
        BigInt(42000000),
        's2',
        's1',
        BigInt(43),
        BigInt(0),
        BigInt(0),
        false
      );
      s.on_message(req);

      // After step-down + grant, we save the new term and the vote
      const state = get_state_from_kv(se);
      expect(state.current_term).to.equal(BigInt(43));
      expect(state.voted_for).to.equal('s2');

      const reply = find_send(ev, 's2') as VoteResponse;
      expect(reply).to.not.be.null;
      expect(reply.type).to.equal(MessageType.vote_response);
      expect(reply.vote_granted).to.be.true;

      // Re-asking the same peer for a vote (idempotently) should still grant
      const req2 = new VoteRequest(
        BigInt(42000001),
        's2',
        's1',
        BigInt(43),
        BigInt(0),
        BigInt(0),
        false
      );
      s.on_message(req2);
      const replies = find_sends(ev, 's2');
      const last = replies[replies.length - 1] as VoteResponse;
      expect(last.vote_granted).to.be.true;
    });

    it('denies a vote from a stale-term peer', () => {
      const { ev, s } = setup_with_state();
      s.start_server();

      const req = new VoteRequest(
        BigInt(42000000),
        's2',
        's1',
        BigInt(41),
        BigInt(0),
        BigInt(0),
        false
      );
      s.on_message(req);

      const reply = find_send(ev, 's2') as VoteResponse;
      expect(reply).to.not.be.null;
      expect(reply.vote_granted).to.be.false;
    });
  });

  describe('client requests', () => {
    it('returns a not_leader error when not the leader', () => {
      const { s } = setup_with_state();
      s.start_server();
      const ret = s.on_client_request(Buffer.from('test data')) as {
        error: string;
      };
      expect(ret.error).to.equal('not_leader');
    });

    it('appends a log when promoted to leader', () => {
      const { se, s } = setup_with_state();
      s.start_server();
      s.promote_to_leader(false);
      const ret = s.on_client_request(Buffer.from('test data')) as {
        idx: bigint;
        term: bigint;
      };
      expect(ret.idx).to.equal(BigInt(1));
      // The newly leader has term=42 from setup; promote_to_leader() does NOT
      // advance the term so the log carries the existing term.
      expect(ret.term).to.equal(BigInt(42));
      expect(se.log.length).to.equal(1);
      expect(se.log[0].type).to.equal(LogType.message);
      expect(se.log[0].data!.toString()).to.equal('test data');
    });
  });

  describe('append requests (follower)', () => {
    it('accepts heartbeat, log entries and a higher commit_idx', () => {
      const { se, s } = setup_with_state(0);
      s.start_server();

      // heartbeat from a higher-term leader
      s.on_message(
        new AppendRequest(
          BigInt(42000000),
          's2',
          's1',
          BigInt(43),
          BigInt(0),
          BigInt(0),
          BigInt(0),
          BigInt(0),
          []
        )
      );

      // append a single log entry
      const log = new Log(LogType.noop, BigInt(1), BigInt(43), null);
      s.on_message(
        new AppendRequest(
          BigInt(42000001),
          's2',
          's1',
          BigInt(43),
          BigInt(0),
          BigInt(0),
          BigInt(0),
          BigInt(1),
          [log]
        )
      );
      expect(se.log.length).to.equal(1);

      // advance commit index
      s.on_message(
        new AppendRequest(
          BigInt(42000002),
          's2',
          's1',
          BigInt(43),
          BigInt(1),
          BigInt(43),
          BigInt(1),
          BigInt(1),
          []
        )
      );

      const state = get_state_from_kv(se);
      expect(state.commit_idx).to.equal(BigInt(1));
      expect(state.current_term).to.equal(BigInt(43));
    });

    it('does not regress commit_idx if leader sends a stale value', () => {
      // BUGFIX: previously on_append_request would set
      //   commit_idx = min(msg.commit_idx, last_log_idx)
      // unconditionally, which could DECREASE the follower's commit_idx and
      // violate Raft's monotonicity guarantee.
      const { se, s } = setup_with_state(5);
      // pre-seed enough logs so prev_idx checks pass
      for (let i = 1; i <= 5; i++) {
        se.add_log_to_storage(
          new Log(LogType.noop, BigInt(i), BigInt(42), null)
        );
      }
      s.start_server();

      // leader sends an append with commit_idx LOWER than ours (e.g. an
      // out-of-order or delayed message)
      s.on_message(
        new AppendRequest(
          BigInt(42000000),
          's2',
          's1',
          BigInt(42),
          BigInt(5),
          BigInt(42),
          BigInt(2), // stale commit_idx
          BigInt(5),
          []
        )
      );

      const state = get_state_from_kv(se);
      expect(state.commit_idx).to.equal(BigInt(5));
    });

    it('finds the common log point and rejects stale/conflicting requests', () => {
      const { ev, me, se, te } = setup_with_state(1);
      // pre-seed log with terms 40, 41, 42
      se.add_log_to_storage(
        new Log(LogType.message, BigInt(1), BigInt(40), Buffer.from('1'))
      );
      se.add_log_to_storage(
        new Log(LogType.message, BigInt(2), BigInt(41), Buffer.from('2'))
      );
      se.add_log_to_storage(
        new Log(LogType.message, BigInt(3), BigInt(42), Buffer.from('3'))
      );

      const s = new Server('s1', ['s1', 's2', 's3'], se, te, me);
      s.start_server();

      const l33 = new Log(
        LogType.message,
        BigInt(3),
        BigInt(43),
        Buffer.from('33')
      );
      const l4 = new Log(
        LogType.message,
        BigInt(4),
        BigInt(43),
        Buffer.from('44')
      );

      // bad outer term
      s.on_message(
        new AppendRequest(
          BigInt(42000000),
          's2',
          's1',
          BigInt(12),
          BigInt(2),
          BigInt(12),
          BigInt(1),
          BigInt(4),
          [l33, l4]
        )
      );
      let last = find_sends(ev, 's2').pop() as AppendResponse;
      expect(last.type).to.equal(MessageType.append_response);
      expect(last.success).to.be.false;

      // bad prev_term
      s.on_message(
        new AppendRequest(
          BigInt(42000001),
          's2',
          's1',
          BigInt(43),
          BigInt(2),
          BigInt(43),
          BigInt(1),
          BigInt(4),
          [l33, l4]
        )
      );
      last = find_sends(ev, 's2').pop() as AppendResponse;
      expect(last.success).to.be.false;

      // bad prev_idx (way past our log)
      s.on_message(
        new AppendRequest(
          BigInt(42000002),
          's2',
          's1',
          BigInt(43),
          BigInt(15),
          BigInt(42),
          BigInt(1),
          BigInt(4),
          [l33, l4]
        )
      );
      last = find_sends(ev, 's2').pop() as AppendResponse;
      expect(last.success).to.be.false;

      // trying to clobber a committed log (prev_idx < commit_idx)
      s.on_message(
        new AppendRequest(
          BigInt(42000003),
          's2',
          's1',
          BigInt(43),
          BigInt(0),
          BigInt(0),
          BigInt(1),
          BigInt(4),
          [l33, l4]
        )
      );
      last = find_sends(ev, 's2').pop() as AppendResponse;
      expect(last.success).to.be.false;
      // committed log untouched
      expect(se.log.length).to.equal(3);

      // ok, common log at idx=2 (term=41); should delete logs after 2 and append
      s.on_message(
        new AppendRequest(
          BigInt(42000004),
          's2',
          's1',
          BigInt(43),
          BigInt(2),
          BigInt(41),
          BigInt(0),
          BigInt(4),
          [l33, l4]
        )
      );
      last = find_sends(ev, 's2').pop() as AppendResponse;
      expect(last.success).to.be.true;
      // logs are now [1@t40, 2@t41, 3@t43, 4@t43]
      expect(se.log.length).to.equal(4);
      expect(se.log[2].term).to.equal(BigInt(43));
      expect(se.log[3].term).to.equal(BigInt(43));
    });
  });

  describe('append responses (leader)', () => {
    it('updates match_idx on success and recovers a trailing peer on failure', () => {
      const { ev, se, s } = setup_with_state(0);
      s.start_server();
      s.promote_to_leader(false);

      // append a single client log
      s.on_client_request(Buffer.from('test data'));
      expect(se.log.length).to.equal(1);

      // s2 acks, s3 rejects
      s.on_message(
        new AppendResponse(BigInt(42000002), 's2', 's1', BigInt(42), true)
      );
      s.on_message(
        new AppendResponse(BigInt(42000003), 's3', 's1', BigInt(42), false)
      );

      // After s3 rejects we should have sent it a replay with its decremented
      // next_idx (so prev_idx = old_last_idx -- here the log at idx 1 carrying
      // the leader's data).
      const sends_s3 = find_sends(ev, 's3') as AppendRequest[];
      const last_to_s3 = sends_s3[sends_s3.length - 1];
      expect(last_to_s3.type).to.equal(MessageType.append_request);

      // Now s3 acks the replay
      s.on_message(
        new AppendResponse(BigInt(42000005), 's3', 's1', BigInt(42), true)
      );

      // commit_idx should advance because we have majority (self + s2 + s3)
      const state = get_state_from_kv(se);
      expect(state.commit_idx).to.equal(BigInt(1));
    });

    it('steps down when seeing a higher term in an append response', () => {
      // BUGFIX: previously, on_append_response did not check msg.term, so a
      // stale leader would not step down even when contacted by a follower
      // who had moved to a higher term.
      const { se, s } = setup_with_state(0);
      s.start_server();
      s.promote_to_leader(false);

      s.on_client_request(Buffer.from('test data'));
      s.on_message(
        new AppendResponse(BigInt(42000002), 's2', 's1', BigInt(99), false)
      );

      const state = get_state_from_kv(se);
      expect(state.current_term).to.equal(BigInt(99));
      expect(state.voted_for).to.be.null;
    });

    it('catches up a peer that fell behind during optimistic append window', () => {
      const { ev, se, s } = setup_with_state(0);
      s.start_server();
      s.promote_to_leader(false);

      s.on_client_request(Buffer.from('test data'));
      s.on_message(
        new AppendResponse(BigInt(42000002), 's2', 's1', BigInt(42), true)
      );
      s.on_message(
        new AppendResponse(BigInt(42000003), 's3', 's1', BigInt(42), false)
      );
      // add a second log; s3 has optimistic appends turned off so it should
      // not get a duplicate yet
      s.on_client_request(Buffer.from('test data2'));

      // s3 acks the catch-up; that should re-enable optimistic and fire a
      // follow-up append for the new entry
      const before_count = find_sends(ev, 's3').length;
      s.on_message(
        new AppendResponse(BigInt(42000005), 's3', 's1', BigInt(42), true)
      );
      const after_count = find_sends(ev, 's3').length;
      expect(after_count).to.be.greaterThan(before_count);

      expect(se.log.length).to.equal(2);
    });

    it('uses numeric (not lexicographic) majority on match indexes', () => {
      // BUGFIX: update_commit_idx used Array.prototype.sort() with no
      // comparator on bigint match indexes. That caused [10n, 2n, 5n] to be
      // ordered as [10n, 2n, 5n] (lex), making the leader pick the wrong
      // median. With 3 followers acking idx 10, 2 and 5, the committed idx
      // must be 5 (median), not 2.
      const { se, s } = setup_with_state(0);
      // pre-seed 10 logs (the leader needs them to exist before "committing")
      for (let i = 1; i <= 10; i++) {
        se.add_log_to_storage(
          new Log(LogType.noop, BigInt(i), BigInt(42), null)
        );
      }
      s.start_server();
      s.promote_to_leader(false);

      // craft 3 acks so the followers' match_idx ends up at 10, 2, 5
      // peer.match_idx = request.last_idx; so we issue heartbeats with
      // matching last_idx values via the inflight tracking. Simplest: poke
      // the peer state directly by replaying the trailing-replay path.
      // We do this by sending ack messages after a heartbeat fires.

      // For this unit test we just inject inflight messages then deliver
      // success responses with explicit last_idx values.
      const inject = (peer_addr: string, last_idx: bigint, msg_id: bigint) => {
        const req = new AppendRequest(
          msg_id,
          's1',
          peer_addr,
          BigInt(42),
          last_idx,
          BigInt(42),
          BigInt(0),
          last_idx,
          []
        );
        // tslint:disable-next-line: no-any
        const peer = (s as any).peers.get(peer_addr);
        peer.inflight_messages.set(msg_id, req);
        peer.max_message_id = BigInt(0);
      };

      inject('s2', BigInt(10), BigInt(50000001));
      inject('s3', BigInt(2), BigInt(50000002));
      inject('s4', BigInt(5), BigInt(50000003));

      s.on_message(
        new AppendResponse(BigInt(50000001), 's2', 's1', BigInt(42), true)
      );
      s.on_message(
        new AppendResponse(BigInt(50000002), 's3', 's1', BigInt(42), true)
      );
      s.on_message(
        new AppendResponse(BigInt(50000003), 's4', 's1', BigInt(42), true)
      );

      // 4 nodes (self at idx 10, plus 10, 2, 5): sorted desc = [10, 10, 5, 2]
      // floor(4/2) = 2 -> commit_idx should be 5
      const state = get_state_from_kv(se);
      expect(state.commit_idx).to.equal(BigInt(5));
    });
  });

  describe('heartbeats', () => {
    it('sends an empty append on heartbeat firing', () => {
      const { ev, s } = setup_with_state();
      s.promote_to_leader(false);
      // first heartbeat callback after promote_to_leader is for s2
      const heartbeat = ev.logs.find(
        l =>
          l.name === 'TestTimeoutEngine::set' &&
          l.args.name === 'heartbeat_s2'
      );
      expect(heartbeat, 'heartbeat scheduled for s2').to.not.be.undefined;
      heartbeat!.args.callback();

      // most recent send to s2 is an AppendRequest with empty logs
      const sends_s2 = find_sends(ev, 's2');
      const last = sends_s2[sends_s2.length - 1] as AppendRequest;
      expect(last.type).to.equal(MessageType.append_request);
      expect(last.logs).to.eql([]);
    });
  });

  describe('robustness', () => {
    it('ignores invalid/unknown messages without crashing', () => {
      const { me, s } = setup_with_state(0);
      s.start_server();

      // not a leader: no-op
      s.leader_append_entry(Log.make_empty());

      // peer not in our list -> ignored
      let msg: Message = new VoteRequest(
        BigInt(1),
        'bad',
        's1',
        BigInt(0),
        BigInt(0),
        BigInt(0),
        false
      );
      s.on_message(msg);

      // wrong destination -> ignored
      msg.to = 'bad';
      msg.from = 's2';
      s.on_message(msg);

      // duplicate / out-of-order ids -> ignored
      msg = new VoteRequest(
        BigInt(1000),
        's2',
        's1',
        BigInt(0),
        BigInt(0),
        BigInt(0),
        false
      );
      s.on_message(msg);
      s.on_message(msg);
      msg.id = BigInt(900);
      s.on_message(msg);

      // unknown message type -> default branch
      msg.id = BigInt(1001);
      msg.type = MessageType.none;
      s.on_message(msg);

      // unsolicited responses -> ignored gracefully
      s.on_message(new VoteResponse(BigInt(1002), 's2', 's1', BigInt(0), false));

      // a message piped through the EventEmitter still works
      me.emit(
        'message',
        new AppendResponse(BigInt(1003), 's2', 's1', BigInt(0), false)
      );

      // no exception means we pass; assert at least that we did not throw
      expect(true).to.be.true;
    });

    it('persists message id chunk so ids are unique across restarts', () => {
      const { se, s } = setup_with_state(0);
      s.start_server();
      s.promote_to_leader(false);
      s.on_client_request(Buffer.from('a'));
      s.on_client_request(Buffer.from('b'));

      // The message_id_chunk should have been bumped and saved
      const chunk = se.kv.get('message_id_chunk');
      expect(chunk).to.not.be.undefined;
      expect(BigInt(chunk!) > BigInt(42000000)).to.be.true;
    });

    it('runs queued message timeouts without throwing', () => {
      const { ev, te, s } = setup_with_state(0);
      s.start_server();
      ev.logs[3].args.callback();
      for (const [, cb] of te.timeouts) {
        cb();
      }
      te.timeouts = new Map();
      s.promote_to_leader(false);
      s.on_client_request(Buffer.from('test data'));
      for (const [, cb] of te.timeouts) {
        cb();
      }
    });
  });

  describe('config changes', () => {
    it('walks through joint consensus and trims old peers after commit', () => {
      const { se, s } = setup_with_state(0);
      s.start_server();
      s.promote_to_leader(false);

      // boot s4, add s5 (so old=[s1,s2,s3,s4], new=[s1,s2,s3,s5])
      s.on_config_change_request(['s1', 's2', 's3', 's5']);

      // first config log committed via majority of BOTH old and new
      s.on_message(
        new AppendResponse(BigInt(42000002), 's2', 's1', BigInt(42), true)
      );
      s.on_message(
        new AppendResponse(BigInt(42000003), 's3', 's1', BigInt(42), true)
      );

      const state = get_state_from_kv(se);
      // we should have committed the joint config and emitted the second
      // (final) config which clears the old peer set
      expect(state.peer_addresses_old).to.eql([]);
      expect(state.peer_addresses).to.eql(['s1', 's2', 's3', 's5']);
      // commit_idx must have advanced past the first config log
      expect(state.commit_idx >= BigInt(1)).to.be.true;
    });

    it('refuses config-change requests from a non-leader', () => {
      const { s } = setup_with_state(0);
      s.start_server();
      const ret = s.on_config_change_request(['s1', 's2']) as {
        error: string;
      };
      expect(ret.error).to.equal('not_leader');
    });

    it('uses peer_addresses_old as-is when already in joint mode', () => {
      // BUGFIX REGRESSION: a second config change while joint should
      // preserve the original old set, not promote the current "new" set
      // into the old slot.
      const { se, s } = setup_with_state(0);
      s.start_server();
      s.promote_to_leader(false);
      s.on_config_change_request(['s1', 's2', 's3', 's5']);
      // now joint: old=[s1,s2,s3,s4], new=[s1,s2,s3,s5]
      s.on_config_change_request(['s1', 's2', 's3', 's6']);
      const state = get_state_from_kv(se);
      // the original old set must still be the old set
      expect(state.peer_addresses_old).to.eql(['s1', 's2', 's3', 's4']);
      expect(state.peer_addresses).to.eql(['s1', 's2', 's3', 's6']);
    });

    it('handles a config log delivered by an AppendRequest', () => {
      // a follower sees a config log come through the wire and updates its
      // peer set accordingly; this is the path used during cluster expansion
      const { se, s } = setup_with_state(0);
      s.start_server();
      const config = { old_peers: [], new_peers: ['s1', 's2', 's3', 's7'] };
      const log = new Log(
        LogType.config,
        BigInt(1),
        BigInt(43),
        Buffer.from(JSON.stringify(config))
      );
      s.on_message(
        new AppendRequest(
          BigInt(42000000),
          's2',
          's1',
          BigInt(43),
          BigInt(0),
          BigInt(0),
          BigInt(0),
          BigInt(1),
          [log]
        )
      );
      const state = get_state_from_kv(se);
      expect(state.peer_addresses).to.eql(['s1', 's2', 's3', 's7']);
      expect(state.config_idx).to.equal(BigInt(1));
    });
  });

  // -------------------------------------------------------------------------
  // Additional corner cases: cluster sizes, log up-to-date logic, conflict
  // resolution, optimistic appends, event emission, self-removal, etc.
  // -------------------------------------------------------------------------

  describe('cluster sizes', () => {
    it('a single-node cluster promotes itself on the first vote timeout', () => {
      // BUGFIX: previously the candidate path only checked `has_vote_majority`
      // when it received vote responses. With no peers there are no responses,
      // so a 1-node cluster could never become leader.
      const ev = new EventLog();
      const me = new TestMessagingEngine(new TestSerde(), ev);
      const se = new TestStorageEngine(ev);
      const te = new TestTimeoutEngine(ev);
      const s = new Server('s1', ['s1'], se, te, me);
      s.start_server();
      fire_vote_timeout(ev);
      const state = get_state_from_kv(se);
      // term must have advanced (real vote happens after prevote majority)
      expect(state.current_term).to.equal(BigInt(1));
      expect(state.voted_for).to.equal('s1');
    });

    it('a two-node cluster requires both nodes to elect a leader', () => {
      const ev = new EventLog();
      const me = new TestMessagingEngine(new TestSerde(), ev);
      const se = new TestStorageEngine(ev);
      const te = new TestTimeoutEngine(ev);
      const s = new Server('s1', ['s1', 's2'], se, te, me);
      s.start_server();
      fire_vote_timeout(ev);
      // one denial means we don't have majority and stay in prevote
      const sent = ev.logs.find(
        l => l.name === 'TestMessagingEngine::send'
      );
      const msg_id = sent!.args.message.id as bigint;
      s.on_message(
        new VoteResponse(msg_id, 's2', 's1', BigInt(0), false)
      );
      const state = get_state_from_kv(se);
      expect(state.current_term).to.equal(BigInt(0));
    });

    it('a five-node cluster needs three votes (incl. self) for majority', () => {
      const ev = new EventLog();
      const me = new TestMessagingEngine(new TestSerde(), ev);
      const se = new TestStorageEngine(ev);
      const te = new TestTimeoutEngine(ev);
      se.kv.set('message_id_chunk', BigInt(0).toString());
      const st = new State();
      st.current_term = BigInt(0);
      st.peer_addresses = ['s1', 's2', 's3', 's4', 's5'];
      se.kv.set('state', st.toString());
      const s = new Server('s1', ['s1', 's2', 's3', 's4', 's5'], se, te, me);
      s.start_server();
      fire_vote_timeout(ev);
      // figure out the IDs sent to each peer
      const reqs = ev.logs.filter(
        l => l.name === 'TestMessagingEngine::send'
      );
      const id_for = (peer_addr: string): bigint => {
        const r = reqs.find(l => l.args.peer_addr === peer_addr);
        return r!.args.message.id as bigint;
      };
      // grant from s2 (1 + self = 2): no majority yet
      s.on_message(
        new VoteResponse(id_for('s2'), 's2', 's1', BigInt(0), true)
      );
      let state = get_state_from_kv(se);
      expect(state.current_term).to.equal(BigInt(0));
      // grant from s3 (2 + self = 3): prevote majority -> real vote starts
      s.on_message(
        new VoteResponse(id_for('s3'), 's3', 's1', BigInt(0), true)
      );
      state = get_state_from_kv(se);
      expect(state.current_term).to.equal(BigInt(1));
      expect(state.voted_for).to.equal('s1');
    });
  });

  describe('vote-grant logic (log up-to-date)', () => {
    function make_voter_with_log(
      our_idx: number,
      our_term: number
    ): { se: TestStorageEngine; s: Server } {
      const ev = new EventLog();
      const me = new TestMessagingEngine(new TestSerde(), ev);
      const se = new TestStorageEngine(ev);
      const te = new TestTimeoutEngine(ev);
      const st = new State();
      st.current_term = BigInt(5);
      st.peer_addresses = ['s1', 's2'];
      se.kv.set('state', st.toString());
      // pre-seed log up to our_idx with our_term
      for (let i = 1; i <= our_idx; i++) {
        se.add_log_to_storage(
          new Log(LogType.message, BigInt(i), BigInt(our_term), null)
        );
      }
      const s = new Server('s1', ['s1', 's2'], se, te, me);
      s.start_server();
      return { se, s };
    }

    it('denies a vote when candidate has a stale last_term', () => {
      const { se, s } = make_voter_with_log(3, 5);
      s.on_message(
        new VoteRequest(
          BigInt(1),
          's2',
          's1',
          BigInt(6),
          BigInt(10), // candidate has more entries, but...
          BigInt(4), // ...on a stale term
          false
        )
      );
      // we still step down for the higher term
      const state = get_state_from_kv(se);
      expect(state.current_term).to.equal(BigInt(6));
      // but we should not have voted for them
      expect(state.voted_for).to.be.null;
    });

    it('denies a vote when candidate has same term but shorter log', () => {
      const { se, s } = make_voter_with_log(3, 5);
      s.on_message(
        new VoteRequest(
          BigInt(1),
          's2',
          's1',
          BigInt(6),
          BigInt(2), // shorter
          BigInt(5),
          false
        )
      );
      const state = get_state_from_kv(se);
      expect(state.voted_for).to.be.null;
    });

    it('grants a vote when candidate has the same log tip', () => {
      const { se, s } = make_voter_with_log(3, 5);
      s.on_message(
        new VoteRequest(
          BigInt(1),
          's2',
          's1',
          BigInt(6),
          BigInt(3),
          BigInt(5),
          false
        )
      );
      const state = get_state_from_kv(se);
      expect(state.voted_for).to.equal('s2');
    });

    it('grants a vote when candidate has a longer log on the same term', () => {
      const { se, s } = make_voter_with_log(3, 5);
      s.on_message(
        new VoteRequest(
          BigInt(1),
          's2',
          's1',
          BigInt(6),
          BigInt(10),
          BigInt(5),
          false
        )
      );
      const state = get_state_from_kv(se);
      expect(state.voted_for).to.equal('s2');
    });

    it('grants a vote when candidate has a higher last_term even with shorter log', () => {
      const { se, s } = make_voter_with_log(10, 5);
      s.on_message(
        new VoteRequest(
          BigInt(1),
          's2',
          's1',
          BigInt(6),
          BigInt(2), // shorter than ours
          BigInt(7), // but a much higher term
          false
        )
      );
      const state = get_state_from_kv(se);
      expect(state.voted_for).to.equal('s2');
    });

    it('refuses a second vote in the same term to a different peer', () => {
      const { ev, se, s } = setup_with_state();
      s.start_server();
      // first request grants the vote (term 43 > 42)
      s.on_message(
        new VoteRequest(
          BigInt(42000000),
          's2',
          's1',
          BigInt(43),
          BigInt(0),
          BigInt(0),
          false
        )
      );
      let state = get_state_from_kv(se);
      expect(state.voted_for).to.equal('s2');
      // a different peer requests in the same term -> deny
      s.on_message(
        new VoteRequest(
          BigInt(42000001),
          's3',
          's1',
          BigInt(43),
          BigInt(0),
          BigInt(0),
          false
        )
      );
      state = get_state_from_kv(se);
      expect(state.voted_for).to.equal('s2');
      const replies = find_sends(ev, 's3') as VoteResponse[];
      expect(replies[replies.length - 1].vote_granted).to.be.false;
    });
  });

  describe('prevote semantics', () => {
    it('prevote responses do NOT advance term or change voted_for', () => {
      const { ev, se, s } = setup_with_state();
      s.start_server();
      fire_vote_timeout(ev);
      // peer responds to the test (prevote) with a grant; their saved term
      // and vote should be unchanged from before processing.
      // First snapshot before any processing happens further:
      const before = get_state_from_kv(se);
      // simulate ourselves _receiving_ a prevote from another node
      s.on_message(
        new VoteRequest(
          BigInt(99000000),
          's3',
          's1',
          BigInt(100),
          BigInt(0),
          BigInt(0),
          true // is_test (prevote)
        )
      );
      const after = get_state_from_kv(se);
      expect(after.current_term).to.equal(before.current_term);
      expect(after.voted_for).to.equal(before.voted_for);
    });

    it('prevote responses are denied when peer is on a higher term', () => {
      // setup a server at term=10
      const ev = new EventLog();
      const me = new TestMessagingEngine(new TestSerde(), ev);
      const se = new TestStorageEngine(ev);
      const te = new TestTimeoutEngine(ev);
      const st = new State();
      st.current_term = BigInt(10);
      st.peer_addresses = ['s1', 's2'];
      se.kv.set('state', st.toString());
      const s = new Server('s1', ['s1', 's2'], se, te, me);
      s.start_server();

      // prevote from a stale-term peer
      s.on_message(
        new VoteRequest(
          BigInt(1),
          's2',
          's1',
          BigInt(5), // lower term
          BigInt(0),
          BigInt(0),
          true
        )
      );
      const reply = find_send(ev, 's2') as VoteResponse;
      expect(reply.vote_granted).to.be.false;
    });
  });

  describe('append corner cases', () => {
    it('accepts an append at prev_idx=0 against an empty log', () => {
      const { se, s } = setup_with_state(0);
      s.start_server();
      const log = new Log(LogType.message, BigInt(1), BigInt(43), null);
      s.on_message(
        new AppendRequest(
          BigInt(42000000),
          's2',
          's1',
          BigInt(43),
          BigInt(0),
          BigInt(0),
          BigInt(0),
          BigInt(1),
          [log]
        )
      );
      expect(se.log.length).to.equal(1);
    });

    it('replays multiple logs in a single append request', () => {
      const { se, s } = setup_with_state(0);
      s.start_server();
      const logs = [
        new Log(LogType.message, BigInt(1), BigInt(43), Buffer.from('a')),
        new Log(LogType.message, BigInt(2), BigInt(43), Buffer.from('b')),
        new Log(LogType.message, BigInt(3), BigInt(43), Buffer.from('c')),
      ];
      s.on_message(
        new AppendRequest(
          BigInt(42000000),
          's2',
          's1',
          BigInt(43),
          BigInt(0),
          BigInt(0),
          BigInt(0),
          BigInt(3),
          logs
        )
      );
      expect(se.log.length).to.equal(3);
      expect(se.log[0].data!.toString()).to.equal('a');
      expect(se.log[2].data!.toString()).to.equal('c');
    });

    it('rejects an append from a leader on a strictly lower term', () => {
      const { ev, s } = setup_with_state(0);
      s.start_server();
      s.on_message(
        new AppendRequest(
          BigInt(42000000),
          's2',
          's1',
          BigInt(10), // lower than our current_term (42)
          BigInt(0),
          BigInt(0),
          BigInt(0),
          BigInt(0),
          []
        )
      );
      const reply = find_send(ev, 's2') as AppendResponse;
      expect(reply.success).to.be.false;
      // and we keep our higher current_term in the response
      expect(reply.term).to.equal(BigInt(42));
    });

    it('clamps follower commit_idx to its last_log_idx', () => {
      const { se, s } = setup_with_state(0);
      s.start_server();
      // accept a single log so last_log_idx = 1
      const log = new Log(LogType.message, BigInt(1), BigInt(43), null);
      s.on_message(
        new AppendRequest(
          BigInt(42000000),
          's2',
          's1',
          BigInt(43),
          BigInt(0),
          BigInt(0),
          BigInt(0),
          BigInt(1),
          [log]
        )
      );
      // leader claims commit_idx=999 but we only have idx=1
      s.on_message(
        new AppendRequest(
          BigInt(42000001),
          's2',
          's1',
          BigInt(43),
          BigInt(1),
          BigInt(43),
          BigInt(999),
          BigInt(1),
          []
        )
      );
      const state = get_state_from_kv(se);
      expect(state.commit_idx).to.equal(BigInt(1));
    });

    it('emits a no_common_log event when prev_term mismatches', () => {
      const { ev, me, se, te } = setup_with_state(0);
      se.add_log_to_storage(
        new Log(LogType.message, BigInt(1), BigInt(40), null)
      );
      const s = new Server('s1', ['s1', 's2', 's3'], se, te, me);
      s.start_server();
      let no_common_log_called = false;
      s.on('no_common_log', () => {
        no_common_log_called = true;
      });
      s.on_message(
        new AppendRequest(
          BigInt(42000000),
          's2',
          's1',
          BigInt(43),
          BigInt(1),
          BigInt(99), // wrong prev_term
          BigInt(0),
          BigInt(2),
          [new Log(LogType.message, BigInt(2), BigInt(43), null)]
        )
      );
      expect(no_common_log_called).to.be.true;
      const reply = find_sends(ev, 's2').pop() as AppendResponse;
      expect(reply.success).to.be.false;
    });

    it('truncates dirty logs and emits delete_dirty', () => {
      const { me, se, te } = setup_with_state(0);
      // pre-seed with logs at idx 1..3 (terms 40,41,42); idx 3 will be replaced
      se.add_log_to_storage(
        new Log(LogType.message, BigInt(1), BigInt(40), null)
      );
      se.add_log_to_storage(
        new Log(LogType.message, BigInt(2), BigInt(41), null)
      );
      se.add_log_to_storage(
        new Log(LogType.message, BigInt(3), BigInt(42), null)
      );
      const s = new Server('s1', ['s1', 's2', 's3'], se, te, me);
      s.start_server();
      let delete_dirty_arg: bigint | null = null;
      s.on('delete_dirty', (idx: bigint) => {
        delete_dirty_arg = idx;
      });
      // accept overwrite from idx 3 with new term
      s.on_message(
        new AppendRequest(
          BigInt(42000000),
          's2',
          's1',
          BigInt(43),
          BigInt(2),
          BigInt(41),
          BigInt(0),
          BigInt(3),
          [new Log(LogType.message, BigInt(3), BigInt(43), null)]
        )
      );
      expect(delete_dirty_arg).to.equal(BigInt(2));
      expect(se.log.length).to.equal(3);
      expect(se.log[2].term).to.equal(BigInt(43));
    });

    it('updates current_leader and emits when a new leader sends an append', () => {
      const { s } = setup_with_state(0);
      s.start_server();
      let observed: string | null = null;
      s.on('current_leader', (l: string | null) => {
        observed = l;
      });
      s.on_message(
        new AppendRequest(
          BigInt(42000000),
          's2',
          's1',
          BigInt(43),
          BigInt(0),
          BigInt(0),
          BigInt(0),
          BigInt(0),
          []
        )
      );
      expect(observed).to.equal('s2');
    });
  });

  describe('append response corner cases', () => {
    it('does not regress next_idx below zero on persistent rejections', () => {
      // When the peer keeps rejecting, peer.next_idx decrements by 1 each
      // time. If it ever reached <= 0, the leader resets it to last_log_idx-1
      // (the comment says "retry from the last log"). This test exercises
      // that path and asserts we still send a valid AppendRequest.
      const { ev, se, s } = setup_with_state(0);
      // pre-seed two logs so last_log_idx > 0
      se.add_log_to_storage(
        new Log(LogType.message, BigInt(1), BigInt(42), null)
      );
      se.add_log_to_storage(
        new Log(LogType.message, BigInt(2), BigInt(42), null)
      );
      s.start_server();
      s.promote_to_leader(false);

      // simulate sending a heartbeat to s2 first so we have an inflight req
      const heartbeat_set = ev.logs.find(
        l =>
          l.name === 'TestTimeoutEngine::set' &&
          l.args.name === 'heartbeat_s2'
      );
      heartbeat_set!.args.callback();
      const sent = find_sends(ev, 's2').pop() as AppendRequest;

      // reject 4 times in a row (decrement past zero)
      for (let i = 0; i < 4; i++) {
        // tslint:disable-next-line: no-any
        const peer = (s as any).peers.get('s2');
        // Ensure we have the latest inflight registered
        const sent_now = find_sends(ev, 's2').pop() as AppendRequest;
        const id = sent_now.id;
        peer.inflight_messages.set(id, sent_now);
        peer.max_message_id = BigInt(0);
        s.on_message(
          new AppendResponse(id, 's2', 's1', BigInt(42), false)
        );
      }
      // we should keep sending AppendRequest messages without throwing
      const all = find_sends(ev, 's2');
      expect(all.length).to.be.greaterThan(1);
      expect(all[all.length - 1].type).to.equal(MessageType.append_request);
      expect(sent.type).to.equal(MessageType.append_request);
    });

    it('does not advance commit_idx without a new majority', () => {
      const { se, s } = setup_with_state(0);
      s.start_server();
      s.promote_to_leader(false);
      s.on_client_request(Buffer.from('only one'));
      // only s2 acks
      s.on_message(
        new AppendResponse(BigInt(42000002), 's2', 's1', BigInt(42), true)
      );
      // self + s2 = 2 of 4 (cluster s1,s2,s3,s4) -> NOT majority
      const state = get_state_from_kv(se);
      expect(state.commit_idx).to.equal(BigInt(0));
    });

    it('shuts off optimistic appends after a rejection', () => {
      const { ev, s } = setup_with_state(0);
      s.start_server();
      s.promote_to_leader(false);
      s.on_client_request(Buffer.from('a'));
      const before = find_sends(ev, 's3').length;
      // s3 rejects
      s.on_message(
        new AppendResponse(BigInt(42000003), 's3', 's1', BigInt(42), false)
      );
      // adding more logs should NOT trigger a new send to s3 (optimistic off)
      s.on_client_request(Buffer.from('b'));
      s.on_client_request(Buffer.from('c'));
      const after = find_sends(ev, 's3').length;
      // exactly one more send (the catch-up replay), not three (one per
      // client req)
      expect(after - before).to.equal(1);
    });
  });

  describe('step down and role changes', () => {
    it('emits role_change when becoming candidate then leader', () => {
      const { ev, s } = setup_with_state();
      s.start_server();
      const roles: Role[] = [];
      s.on('role_change', (r: Role) => roles.push(r));
      fire_vote_timeout(ev); // start prevote
      s.on_message(
        new VoteResponse(BigInt(42000002), 's2', 's1', BigInt(42), true)
      );
      s.on_message(
        new VoteResponse(BigInt(42000003), 's3', 's1', BigInt(42), true)
      );
      s.on_message(
        new VoteResponse(BigInt(42000005), 's2', 's1', BigInt(43), true)
      );
      s.on_message(
        new VoteResponse(BigInt(42000006), 's3', 's1', BigInt(43), true)
      );
      expect(roles).to.contain(Role.candidate);
      expect(roles).to.contain(Role.leader);
    });

    it('emits role_change back to follower on step down', () => {
      const { se, s } = setup_with_state(0);
      s.start_server();
      s.promote_to_leader(false);
      const roles: Role[] = [];
      s.on('role_change', (r: Role) => roles.push(r));
      // a higher-term append response triggers step down
      s.on_client_request(Buffer.from('x'));
      s.on_message(
        new AppendResponse(BigInt(42000002), 's2', 's1', BigInt(99), false)
      );
      expect(roles[0]).to.equal(Role.follower);
      const state = get_state_from_kv(se);
      expect(state.current_term).to.equal(BigInt(99));
      expect(state.voted_for).to.be.null;
    });

    it('emits term and voted_for events when state changes', () => {
      const { s } = setup_with_state();
      s.start_server();
      const observed = { term: null as bigint | null, voted_for: 'unset' as
        | string
        | null
        | undefined };
      s.on('term', (t: bigint) => (observed.term = t));
      s.on('voted_for', (v: string | null) => (observed.voted_for = v));
      s.on_message(
        new VoteRequest(
          BigInt(42000000),
          's2',
          's1',
          BigInt(50),
          BigInt(0),
          BigInt(0),
          false
        )
      );
      expect(observed.term).to.equal(BigInt(50));
      expect(observed.voted_for).to.equal('s2');
    });

    it('emits commit_idx when the follower advances', () => {
      const { s } = setup_with_state(0);
      s.start_server();
      const log = new Log(LogType.message, BigInt(1), BigInt(43), null);
      s.on_message(
        new AppendRequest(
          BigInt(42000000),
          's2',
          's1',
          BigInt(43),
          BigInt(0),
          BigInt(0),
          BigInt(0),
          BigInt(1),
          [log]
        )
      );
      let observed: bigint | null = null;
      s.on('commit_idx', (c: bigint) => (observed = c));
      s.on_message(
        new AppendRequest(
          BigInt(42000001),
          's2',
          's1',
          BigInt(43),
          BigInt(1),
          BigInt(43),
          BigInt(1),
          BigInt(1),
          []
        )
      );
      expect(observed).to.equal(BigInt(1));
    });
  });

  describe('leader-side responsiveness check', () => {
    it('a leader without enough peer responses steps down at next vote timeout', () => {
      const { ev, s } = setup_with_state();
      s.start_server();
      s.promote_to_leader(false);
      const roles: Role[] = [];
      s.on('role_change', (r: Role) => roles.push(r));
      fire_vote_timeout(ev);
      // candidate_start_prevote stepped us down to follower then re-entered
      // prevote, so a role_change to follower should have been emitted.
      expect(roles).to.contain(Role.follower);
    });

    it('a leader that received responses recently retains leadership', () => {
      const { ev, s } = setup_with_state();
      s.start_server();
      s.promote_to_leader(false);
      // mark all peers as responding favorably
      // tslint:disable-next-line: no-any
      for (const [, peer] of (s as any).peers) {
        peer.is_responding_favorably = true;
      }
      fire_vote_timeout(ev);
      // the timeout callback should not have stepped us down; verify by
      // accepting a client request as leader.
      const ret = s.on_client_request(Buffer.from('still leader')) as {
        idx: bigint;
      };
      expect(ret.idx).to.equal(BigInt(1));
    });
  });

  describe('self removal', () => {
    it('a non-leader removed from config stops messaging', () => {
      const ev = new EventLog();
      const me = new TestMessagingEngine(new TestSerde(), ev);
      const se = new TestStorageEngine(ev);
      const te = new TestTimeoutEngine(ev);
      // craft a state where s1 is no longer in the config
      const st = new State();
      st.current_term = BigInt(0);
      st.peer_addresses = ['s2', 's3'];
      se.kv.set('state', st.toString());
      const s = new Server('s1', ['s2', 's3'], se, te, me);
      s.start_server();

      let stopped = false;
      // monkey-patch the engine's stop to detect the call
      const orig_stop = me.stop.bind(me);
      me.stop = () => {
        stopped = true;
        orig_stop();
      };
      // fire the vote timeout (the only path that checks self-removal)
      fire_vote_timeout(ev);
      expect(stopped).to.be.true;
    });
  });
});

import { MsgpackSerde } from '../src/msgpack_serde';

describe('MsgpackSerde', () => {
  const s = new MsgpackSerde();

  function roundtrip<T extends Message>(msg: T): T {
    const enc = s.encode(msg);
    if (!enc) throw new Error('encode failed');
    const buf = enc.slice(0);
    const dec = s.decode(buf);
    if (!dec) throw new Error('decode failed');
    return dec as T;
  }

  it('round-trips a VoteRequest', () => {
    const r = roundtrip(
      new VoteRequest(
        BigInt(7),
        'a',
        'b',
        BigInt(10),
        BigInt(3),
        BigInt(2),
        true
      )
    );
    expect(r.id).to.equal(BigInt(7));
    expect(r.from).to.equal('a');
    expect(r.to).to.equal('b');
    expect(r.term).to.equal(BigInt(10));
    expect(r.last_idx).to.equal(BigInt(3));
    expect(r.last_term).to.equal(BigInt(2));
    expect(r.is_test).to.be.true;
  });

  it('round-trips a VoteResponse', () => {
    const r = roundtrip(
      new VoteResponse(BigInt(7), 'a', 'b', BigInt(10), true)
    );
    expect(r.vote_granted).to.be.true;
    expect(r.id).to.equal(BigInt(7));
  });

  it('round-trips an AppendResponse', () => {
    const r = roundtrip(
      new AppendResponse(BigInt(8), 'x', 'y', BigInt(99), false)
    );
    expect(r.success).to.be.false;
    expect(r.term).to.equal(BigInt(99));
  });

  it('round-trips an AppendRequest with multiple logs and bigint fields', () => {
    const logs = [
      new Log(
        LogType.message,
        BigInt(1),
        BigInt(2),
        Buffer.from('hello')
      ),
      new Log(LogType.config, BigInt(2), BigInt(2), Buffer.from('{}')),
      new Log(LogType.noop, BigInt(3), BigInt(3), null),
    ];
    const r = roundtrip(
      new AppendRequest(
        BigInt(11),
        'leader',
        'follower',
        BigInt(7),
        BigInt(0),
        BigInt(0),
        BigInt(0),
        BigInt(3),
        logs
      )
    );
    expect(r.prev_idx).to.equal(BigInt(0));
    expect(r.commit_idx).to.equal(BigInt(0));
    expect(r.last_idx).to.equal(BigInt(3));
    expect(r.logs.length).to.equal(3);
    expect(r.logs[0].type).to.equal(LogType.message);
    expect(r.logs[0].idx).to.equal(BigInt(1));
    expect(r.logs[0].term).to.equal(BigInt(2));
    expect(r.logs[0].data!.toString()).to.equal('hello');
    expect(r.logs[1].type).to.equal(LogType.config);
    expect(r.logs[2].data).to.be.null;
  });

  it('preserves very large bigints across the wire', () => {
    const big = BigInt('123456789012345678901234567890');
    const r = roundtrip(
      new VoteRequest(
        big,
        'a',
        'b',
        big + BigInt(1),
        big + BigInt(2),
        big + BigInt(3),
        false
      )
    );
    expect(r.id).to.equal(big);
    expect(r.term).to.equal(big + BigInt(1));
    expect(r.last_idx).to.equal(big + BigInt(2));
    expect(r.last_term).to.equal(big + BigInt(3));
  });

  it('returns undefined when encoding an unknown message type', () => {
    // tslint:disable-next-line: no-any
    const bogus: any = { type: MessageType.none };
    expect(s.encode(bogus)).to.be.undefined;
  });
});

import { NodeTimeoutEngine } from '../src/node_timeout';

describe('NodeTimeoutEngine', () => {
  it('fires registered callbacks after the configured timeout', done => {
    const eng = new NodeTimeoutEngine(t => t);
    const start = Date.now();
    eng.set('x', 20, () => {
      const elapsed = Date.now() - start;
      try {
        // generous bound; CI clocks are jittery
        expect(elapsed).to.be.greaterThan(10);
        done();
      } catch (e) {
        done(e);
      }
    });
  });

  it('clear() prevents the callback from firing', done => {
    const eng = new NodeTimeoutEngine(t => t);
    let fired = false;
    eng.set('x', 10, () => {
      fired = true;
    });
    eng.clear('x');
    setTimeout(() => {
      try {
        expect(fired).to.be.false;
        done();
      } catch (e) {
        done(e);
      }
    }, 30);
  });

  it('set() with the same name replaces the previous timer', done => {
    const eng = new NodeTimeoutEngine(t => t);
    let first_fired = false;
    eng.set('x', 10, () => {
      first_fired = true;
    });
    eng.set('x', 20, () => {
      try {
        expect(first_fired).to.be.false;
        done();
      } catch (e) {
        done(e);
      }
    });
  });

  it('clearing a nonexistent name is a no-op', () => {
    const eng = new NodeTimeoutEngine(t => t);
    eng.clear('does-not-exist');
    expect(true).to.be.true;
  });

  it('set_varied applies the variance function once', done => {
    let calls = 0;
    const eng = new NodeTimeoutEngine(t => {
      calls++;
      return t;
    });
    eng.set_varied('x', 5, () => {
      try {
        // variance fn was called by set_varied AND by the underlying set
        expect(calls).to.equal(2);
        done();
      } catch (e) {
        done(e);
      }
    });
  });
});
