import { AbstractStorageEngine } from '../src/interfaces';
import { Log } from '../src/messages';
import { EventLog } from './event_log';

export class TestStorageEngine extends AbstractStorageEngine {
  kv: Map<string, string>;
  log: Log[];
  events: EventLog;

  constructor(eventlog: EventLog) {
    super();
    this.kv = new Map();
    this.log = [];
    this.events = eventlog;
  }

  kv_get(key: string): string | null {
    const ret = this.kv.get(key) || null;
    this.events.add('TestStorageEngine::kv_get', { key }, ret);
    return ret;
  }

  kv_set(key: string, value: string): void {
    this.events.add('TestStorageEngine::kv_set', { key, value }, null);
    this.kv.set(key, value);
  }

  get_logs_after(idx: bigint): Log[] {
    let logs: Log[] = [];
    if (idx === BigInt(0)) {
      logs = this.log;
    } else if (idx < this.log.length) {
      logs = this.log.slice(Number(idx.toString()));
    }
    this.events.add('TestStorageEngine::get_logs_after', { idx }, logs);
    return logs;
  }

  log_term(idx: bigint): bigint {
    const log_idx = idx - BigInt(1);
    let ret = BigInt(0);
    if (idx === BigInt(0)) {
      ret = BigInt(0);
    } else if (log_idx <= this.log.length - 1) {
      ret = this.log[Number(log_idx.toString())].term;
    }
    this.events.add('TestStorageEngine::log_term', { idx }, ret);
    return ret;
  }

  last_log_idx(): bigint {
    if (this.log.length === 0) {
      return BigInt(0);
    } else {
      return this.log[this.log.length - 1].idx;
    }
  }

  add_log_to_storage(log: Log): void {
    this.events.add('TestStorageEngine::add_log_to_storage', { log }, null);
    this.log.push(log);
  }

  delete_invalid_logs_from_storage(idx: bigint): void {
    const log_idx = idx;
    if (idx === BigInt(0)) {
      this.log = [];
    } else if (log_idx <= this.log.length - 1) {
      this.log = this.log.slice(0, Number(log_idx.toString()));
    }
    this.events.add(
      'TestStorageEngine::delete_invalid_logs_from_storage',
      { idx },
      null
    );
  }

  latest_config_before_or_at(idx: bigint): Log | null {
    return null;
  }
}
