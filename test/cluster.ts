// In-memory deterministic cluster harness for integration tests.
//
// The transport is a shared bus that delivers messages synchronously by
// default but can be told to drop, delay, or duplicate messages on
// individual links so tests can simulate partitions, jitter, and reordering.
// The timeout engine is virtual so vote / heartbeat fires happen exactly
// when tests advance the clock.

import { EventEmitter } from 'events';
import {
  AbstractMessagingEngine,
  AbstractSerde,
  AbstractStorageEngine,
  AbstractTimeoutEngine,
} from '../src/interfaces';
import { MemStorage } from '../src/mem_storage';
import { Message } from '../src/messages';
import { Server, ServerOptions } from '../src/server';
import { Role } from '../src/state';

// A no-op serde — the harness doesn't go through bytes.
class IdentitySerde extends AbstractSerde {
  encode(_msg: Message) {
    return undefined;
  }
  decode(_data: Buffer) {
    return undefined;
  }
}

interface PendingTimeout {
  fires_at: number;
  callback: () => void;
  // Monotonic seq used to break ties in fire order so tests are
  // deterministic when two timeouts fire at the same logical instant.
  seq: number;
}

export class VirtualClock {
  private now_ms = 0;
  private seq = 0;
  // name -> pending timeout
  private timeouts = new Map<string, PendingTimeout>();

  set(name: string, timeout_ms: number, callback: () => void): void {
    this.timeouts.set(name, {
      fires_at: this.now_ms + timeout_ms,
      callback,
      seq: this.seq++,
    });
  }

  clear(name: string): void {
    this.timeouts.delete(name);
  }

  now(): number {
    return this.now_ms;
  }

  /**
   * Advance virtual time by `ms` milliseconds. Any timeouts whose fires_at
   * is at or before the new time fire in fires_at-then-seq order. Timeouts
   * scheduled by the firing callbacks themselves are processed in the same
   * advance call if they still fall before the target time, which mimics
   * the "drain the queue" behavior of real timer wheels.
   */
  advance(ms: number): void {
    const target = this.now_ms + ms;
    while (true) {
      let next: { name: string; t: PendingTimeout } | null = null;
      for (const [name, t] of this.timeouts) {
        if (t.fires_at > target) continue;
        if (
          !next ||
          t.fires_at < next.t.fires_at ||
          (t.fires_at === next.t.fires_at && t.seq < next.t.seq)
        ) {
          next = { name, t };
        }
      }
      if (!next) break;
      this.timeouts.delete(next.name);
      this.now_ms = next.t.fires_at;
      next.t.callback();
    }
    this.now_ms = target;
  }
}

class VirtualTimeoutEngine extends AbstractTimeoutEngine {
  // Per-engine variance offset used to break exact-tie firing across nodes
  // so the cluster doesn't deadlock with every node prevoting at the same
  // virtual instant. Tests want determinism, so this is a fixed integer
  // injected at construction time, not a random number.
  constructor(
    private clock: VirtualClock,
    private namespace: string,
    private variance_offset_ms: number
  ) {
    super(t => t);
  }
  private key(name: string): string {
    // Namespace timeout names so each node's "vote" / "heartbeat_*" entries
    // don't collide on the shared virtual clock.
    return `${this.namespace}::${name}`;
  }
  set(name: string, timeout_ms: number, callback: () => void): void {
    this.clock.set(this.key(name), timeout_ms, callback);
  }
  set_varied(name: string, timeout_ms: number, callback: () => void): void {
    // Apply the deterministic per-node offset to break ties.
    this.clock.set(
      this.key(name),
      timeout_ms + this.variance_offset_ms,
      callback
    );
  }
  clear(name: string): void {
    this.clock.clear(this.key(name));
  }
}

/**
 * Shared in-memory transport between cluster servers. Tests can install
 * link policies (drop, delay, dedupe) per from→to pair to simulate
 * partitions and message-level faults.
 */
export class VirtualBus {
  private engines = new Map<string, ClusterMessagingEngine>();
  // from -> to -> "pass" | "drop"
  private links = new Map<string, Map<string, 'pass' | 'drop'>>();

  register(addr: string, engine: ClusterMessagingEngine): void {
    this.engines.set(addr, engine);
  }

  link(from: string, to: string, policy: 'pass' | 'drop'): void {
    let m = this.links.get(from);
    if (!m) {
      m = new Map();
      this.links.set(from, m);
    }
    m.set(to, policy);
  }

  partition(addrs: string[]): void {
    // Drop all links into and out of this partition so the inside cannot
    // talk to the outside.
    for (const a of addrs) {
      for (const [b] of this.engines) {
        if (addrs.includes(b)) continue;
        this.link(a, b, 'drop');
        this.link(b, a, 'drop');
      }
    }
  }

  heal(): void {
    this.links.clear();
  }

  deliver(from: string, to: string, msg: Message): void {
    const policy = this.links.get(from)?.get(to);
    if (policy === 'drop') return;
    const engine = this.engines.get(to);
    if (!engine) return;
    // Synchronous delivery — tests advance time explicitly.
    engine.emit('message', msg);
  }
}

class ClusterMessagingEngine extends AbstractMessagingEngine {
  constructor(
    serde: AbstractSerde,
    private bus: VirtualBus,
    private my_addr: string
  ) {
    super(serde);
  }
  start(_address: string): void {
    // Nothing to do — registration happens at Cluster construction time.
  }
  send(peer_addr: string, message: Message): void {
    this.bus.deliver(this.my_addr, peer_addr, message);
  }
  stop(): void {
    // No-op for the in-memory bus.
  }
}

export interface ClusterNode {
  addr: string;
  server: Server;
  storage: AbstractStorageEngine;
  messaging: ClusterMessagingEngine;
}

/**
 * A small in-memory Raft cluster. All servers share a single VirtualClock
 * and VirtualBus so tests are deterministic.
 */
export class Cluster extends EventEmitter {
  readonly clock: VirtualClock;
  readonly bus: VirtualBus;
  readonly nodes = new Map<string, ClusterNode>();

  constructor(addrs: string[], options?: ServerOptions) {
    super();
    this.clock = new VirtualClock();
    this.bus = new VirtualBus();
    let variance_offset = 0;
    for (const addr of addrs) {
      const storage = new MemStorage();
      // Stagger each node's vote/heartbeat by a small deterministic offset
      // so they don't all fire at the same virtual instant. 100ms is well
      // under the 2000ms timeout but enough to ensure ordering.
      const timeouts = new VirtualTimeoutEngine(
        this.clock,
        addr,
        variance_offset
      );
      variance_offset += 100;
      const messaging = new ClusterMessagingEngine(
        new IdentitySerde(),
        this.bus,
        addr
      );
      this.bus.register(addr, messaging);
      const server = new Server(addr, addrs, storage, timeouts, messaging, {
        ...options,
      });
      this.nodes.set(addr, { addr, server, storage, messaging });
      server.start_server();
    }
  }

  get(addr: string): ClusterNode {
    const n = this.nodes.get(addr);
    if (!n) throw new Error(`unknown node ${addr}`);
    return n;
  }

  /** Advance virtual time and let any pending timeouts fire. */
  tick(ms: number): void {
    this.clock.advance(ms);
  }

  /** Find the (single) current leader, if any. */
  leader(): ClusterNode | null {
    let leader: ClusterNode | null = null;
    for (const n of this.nodes.values()) {
      if (n.server.metrics().role === Role.leader) {
        if (leader) {
          throw new Error(
            `multiple leaders: ${leader.addr} and ${n.addr}`
          );
        }
        leader = n;
      }
    }
    return leader;
  }

  /**
   * Drive ticks repeatedly until the predicate becomes true or we run
   * out of budget. Returns whether the predicate was satisfied.
   */
  tick_until(predicate: () => boolean, budget_ms = 60_000, step_ms = 50): boolean {
    let elapsed = 0;
    while (elapsed < budget_ms) {
      if (predicate()) return true;
      this.tick(step_ms);
      elapsed += step_ms;
    }
    return predicate();
  }

  /** Tick until exactly one leader emerges (or budget elapses). */
  await_leader(budget_ms = 60_000): ClusterNode | null {
    this.tick_until(() => this.leader() !== null, budget_ms);
    return this.leader();
  }
}
