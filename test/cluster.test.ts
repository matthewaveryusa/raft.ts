// Multi-node integration tests. These run a real `Server` on every node
// and route messages between them through an in-memory bus, with a virtual
// clock for deterministic timeout / heartbeat behavior. The harness lets
// us partition links, drop messages, and inspect committed state across
// the cluster.

import { expect } from 'chai';
import { Role } from '../src/state';
import { LogType } from '../src/messages';
import { Cluster } from './cluster';

/* tslint:disable no-unused-expression */

describe('Cluster integration', () => {
  describe('elections', () => {
    it('elects a leader within a few election timeouts', () => {
      const cluster = new Cluster(['n1', 'n2', 'n3']);
      const leader = cluster.await_leader();
      expect(leader, 'a leader should emerge').to.not.be.null;
      expect(leader!.server.metrics().role).to.equal(Role.leader);
      // followers see the leader
      for (const n of cluster.nodes.values()) {
        if (n === leader) continue;
        expect(n.server.metrics().current_leader).to.equal(leader!.addr);
      }
    });

    it('only one leader exists per term', () => {
      const cluster = new Cluster(['n1', 'n2', 'n3', 'n4', 'n5']);
      cluster.await_leader();
      // A second leader at the same term would throw; the property is
      // already enforced by Cluster.leader(). Sanity check terms agree.
      const terms = new Set<bigint>();
      for (const n of cluster.nodes.values()) {
        terms.add(n.server.metrics().current_term);
      }
      // Followers may briefly be on an older term right after election;
      // tick a bit more to let the noop append propagate.
      cluster.tick(2000);
      const final_terms = new Set<bigint>();
      for (const n of cluster.nodes.values()) {
        final_terms.add(n.server.metrics().current_term);
      }
      expect(final_terms.size).to.equal(1);
    });

    it('re-elects when the current leader is partitioned away', () => {
      const cluster = new Cluster(['n1', 'n2', 'n3', 'n4', 'n5']);
      const original = cluster.await_leader();
      expect(original).to.not.be.null;
      const original_term = original!.server.metrics().current_term;

      // Isolate the leader.
      const others = ['n1', 'n2', 'n3', 'n4', 'n5'].filter(
        a => a !== original!.addr
      );
      cluster.bus.partition([original!.addr]);

      // Wait long enough for the followers to time out and elect a new
      // leader from the majority side.
      cluster.tick_until(() => {
        const leaders = others
          .map(a => cluster.get(a).server.metrics())
          .filter(m => m.role === Role.leader);
        return leaders.length === 1;
      }, 30_000);

      const new_leader_addr = others.find(
        a => cluster.get(a).server.metrics().role === Role.leader
      );
      expect(new_leader_addr, 'new leader emerged').to.not.be.undefined;
      expect(new_leader_addr).to.not.equal(original!.addr);
      expect(
        cluster.get(new_leader_addr!).server.metrics().current_term >
          original_term
      ).to.be.true;
    });
  });

  describe('replication', () => {
    it('commits a client request on a quorum', () => {
      const cluster = new Cluster(['n1', 'n2', 'n3']);
      const leader = cluster.await_leader();
      expect(leader).to.not.be.null;
      // Let the noop log commit first.
      cluster.tick(2000);
      const before = leader!.server.metrics().commit_idx;

      const ret = leader!.server.on_client_request(Buffer.from('hello'));
      expect('idx' in ret, 'leader accepted client request').to.be.true;

      cluster.tick_until(
        () => leader!.server.metrics().commit_idx > before,
        10_000
      );
      expect(leader!.server.metrics().commit_idx > before).to.be.true;

      // Followers also reach the same commit_idx
      cluster.tick(1000);
      const leader_commit = leader!.server.metrics().commit_idx;
      for (const n of cluster.nodes.values()) {
        expect(
          n.server.metrics().commit_idx,
          `node ${n.addr} commit_idx`
        ).to.equal(leader_commit);
      }
    });

    it('catches up a node that was partitioned and rejoins', () => {
      const cluster = new Cluster(['n1', 'n2', 'n3']);
      const leader = cluster.await_leader();
      cluster.tick(2000);

      const partitioned = ['n1', 'n2', 'n3'].find(a => a !== leader!.addr)!;
      cluster.bus.partition([partitioned]);

      // Generate some new entries while the partition is up.
      for (let i = 0; i < 5; i++) {
        leader!.server.on_client_request(Buffer.from(`msg${i}`));
      }
      cluster.tick(2000);
      const leader_commit = leader!.server.metrics().commit_idx;
      const partitioned_commit = cluster.get(partitioned).server.metrics()
        .commit_idx;
      expect(partitioned_commit < leader_commit).to.be.true;

      // Heal and let the partitioned node catch up.
      cluster.bus.heal();
      cluster.tick_until(
        () =>
          cluster.get(partitioned).server.metrics().commit_idx >=
          leader_commit,
        20_000
      );
      expect(cluster.get(partitioned).server.metrics().commit_idx).to.equal(
        leader_commit
      );
    });

    it('does not commit when the leader has only itself', () => {
      const cluster = new Cluster(['n1', 'n2', 'n3']);
      const leader = cluster.await_leader();
      cluster.tick(2000);

      // Isolate the leader so no follower can ack.
      cluster.bus.partition([leader!.addr]);
      const before = leader!.server.metrics().commit_idx;

      // The leader will keep trying but should not advance commit_idx
      // because it cannot reach a majority. Note: it will eventually step
      // down at the responsiveness check; for this test we just check
      // that commit_idx never moves until it does.
      leader!.server.on_client_request(Buffer.from('lonely'));
      cluster.tick(500); // less than the responsiveness window
      expect(leader!.server.metrics().commit_idx).to.equal(before);
    });
  });

  describe('split brain prevention', () => {
    it('a 2-of-5 minority partition cannot elect a leader', () => {
      const cluster = new Cluster(['n1', 'n2', 'n3', 'n4', 'n5']);
      cluster.await_leader();
      cluster.tick(2000);

      // Isolate exactly two nodes — they hold 2/5 votes total, no majority.
      cluster.bus.partition(['n4', 'n5']);
      cluster.tick(20_000);

      expect(cluster.get('n4').server.metrics().role).to.not.equal(
        Role.leader
      );
      expect(cluster.get('n5').server.metrics().role).to.not.equal(
        Role.leader
      );
      // Some node in the majority {n1,n2,n3} should still be leader.
      const majority_leader = ['n1', 'n2', 'n3']
        .map(a => cluster.get(a))
        .find(n => n.server.metrics().role === Role.leader);
      expect(majority_leader, 'majority side has a leader').to.not.be
        .undefined;
    });
  });

  describe('persistence + replay', () => {
    it('a follower applies the leader noop at the start of a term', () => {
      // Verifies the §5.4.2 fix: promote_to_leader sends a noop by default.
      const cluster = new Cluster(['n1', 'n2', 'n3']);
      const leader = cluster.await_leader();
      cluster.tick(2000);
      const log_idx = leader!.server.metrics().last_log_idx;
      expect(log_idx > BigInt(0)).to.be.true;

      // The first log on every replica should be a noop with the leader's
      // term, regardless of which node won the election.
      for (const n of cluster.nodes.values()) {
        const stored = (n.storage as unknown as { log: { type: LogType }[] })
          .log;
        expect(stored.length).to.be.greaterThan(0);
        expect(stored[0].type).to.equal(LogType.noop);
      }
    });
  });

  describe('synchronous-transport regression', () => {
    it('handles vote responses that arrive inside the send call', () => {
      // BUGFIX: the production code used to register the inflight vote
      // request AFTER calling send. With a synchronous transport (this
      // harness, fast local Unix sockets, in-process testing harnesses)
      // the response arrives during the send and pop_inflight_message
      // returns null because the entry isn't there yet — so the response
      // is silently dropped and the cluster hangs in prevote forever.
      // Now inflight is registered before send.
      const cluster = new Cluster(['n1', 'n2', 'n3']);
      const leader = cluster.await_leader(5_000);
      expect(leader, 'leader emerged via synchronous bus').to.not.be.null;
    });

    it('handles append responses that arrive inside the send call', () => {
      const cluster = new Cluster(['n1', 'n2', 'n3']);
      const leader = cluster.await_leader();
      cluster.tick(2000);
      // Issue several client requests; if synchronous response handling
      // were broken, optimistic-mode peers would never have their match_idx
      // advance and commit_idx would stall.
      for (let i = 0; i < 20; i++) {
        leader!.server.on_client_request(Buffer.from(`msg${i}`));
      }
      cluster.tick(5000);
      const commit = leader!.server.metrics().commit_idx;
      // 20 client logs + 1 noop = at least 21 committed entries
      expect(commit >= BigInt(20)).to.be.true;
      for (const n of cluster.nodes.values()) {
        expect(n.server.metrics().commit_idx).to.equal(commit);
      }
    });
  });
});
