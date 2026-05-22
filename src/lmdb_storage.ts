import { open, RootDatabase } from 'lmdb';
import * as path from 'path';
import { AbstractStorageEngine } from './interfaces';
import { Log } from './messages';

// LMDB-backed storage. Uses two named sub-databases under one root: a flat
// key/value store and a numerically-keyed log store.
//
// Keys for the log store are zero-padded decimal strings. LMDB sorts these
// lexicographically, which matches numeric order because the padding width
// is fixed.
const LOG_KEY_WIDTH = 20;

function pad(idx: bigint): string {
  return idx.toString().padStart(LOG_KEY_WIDTH, '0');
}

interface SerializedLog {
  type: string;
  term: string;
  data: Buffer | null;
}

export class LmdbStorageEngine extends AbstractStorageEngine {
  private cached_log_idx: bigint | null;
  private root: RootDatabase;
  private kvdb: ReturnType<RootDatabase['openDB']>;
  private logdb: ReturnType<RootDatabase['openDB']>;

  constructor(db_name: string, db_size_bytes: number) {
    super();
    this.cached_log_idx = null;
    this.root = open({
      path: path.join(db_name),
      mapSize: db_size_bytes,
      maxDbs: 2,
      compression: false,
    });
    this.kvdb = this.root.openDB({ name: 'key' });
    this.logdb = this.root.openDB({
      name: 'log',
      // structured-clone-friendly: we serialize logs to a small object
      encoding: 'msgpack',
    });
  }

  kv_get(key: string): string | null {
    const v = this.kvdb.get(key);
    return typeof v === 'string' ? v : v == null ? null : String(v);
  }

  kv_set(key: string, value: string): void {
    this.kvdb.putSync(key, value);
  }

  get_logs_after(idx: bigint): Log[] {
    const logs: Log[] = [];
    const start = pad(idx + BigInt(1));
    for (const { key, value } of this.logdb.getRange({ start })) {
      const row = value as SerializedLog;
      logs.push(
        new Log(
          row.type as Log['type'],
          BigInt(key as string),
          BigInt(row.term),
          row.data ?? null
        )
      );
    }
    return logs;
  }

  log_term(idx: bigint): bigint {
    if (idx === BigInt(0)) {
      return BigInt(0);
    }
    const v = this.logdb.get(pad(idx)) as SerializedLog | undefined;
    return v ? BigInt(v.term) : BigInt(0);
  }

  last_log_idx(): bigint {
    if (this.cached_log_idx === null) {
      // getRange with reverse:true and limit:1 gives us the highest key.
      const iter = this.logdb.getRange({ reverse: true, limit: 1 });
      let last: bigint = BigInt(0);
      for (const { key } of iter) {
        last = BigInt(key as string);
      }
      this.cached_log_idx = last;
    }
    return this.cached_log_idx;
  }

  add_log_to_storage(log: Log): void {
    const row: SerializedLog = {
      type: log.type,
      term: log.term.toString(),
      data: log.data,
    };
    this.logdb.putSync(pad(log.idx), row);
    this.cached_log_idx = log.idx;
  }

  delete_invalid_logs_from_storage(idx: bigint): void {
    // Mark every log with key strictly greater than `idx` as deleted.
    const start = pad(idx + BigInt(1));
    const keys: string[] = [];
    for (const { key } of this.logdb.getRange({ start })) {
      keys.push(key as string);
    }
    for (const k of keys) {
      this.logdb.removeSync(k);
    }
    this.cached_log_idx = null;
  }

  close(): void {
    this.root.close();
  }
}
