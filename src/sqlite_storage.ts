// sqlite storage
import sqlite = require('better-sqlite3');
import { AbstractStorageEngine } from './interfaces';
import { Log, LogType } from './messages';

interface KvRow {
  value: string;
}
interface LogTermRow {
  term: string;
}
interface IdxRow {
  idx: string;
}
interface LogRow {
  idx: string;
  term: string;
  data: Buffer | null;
  type: LogType;
}

export class SqliteStorageEngine extends AbstractStorageEngine {
  private db: sqlite.Database;

  private log_term_sql: sqlite.Statement<[string], LogTermRow>;
  private last_log_idx_sql: sqlite.Statement<[], IdxRow>;
  private add_log_sql: sqlite.Statement<
    [string, string, Buffer | null, string]
  >;
  private delete_invalid_logs_sql: sqlite.Statement<[string]>;
  private get_log_sql: sqlite.Statement<[string], LogRow>;
  private get_logs_sql: sqlite.Statement<[string], LogRow>;
  private kv_get_sql: sqlite.Statement<[string], KvRow>;
  private kv_set_sql: sqlite.Statement<[string, string]>;

  private cached_log_idx: bigint | null;

  constructor(db_name: string) {
    super();
    this.db = sqlite(db_name);
    this.db.exec(`CREATE TABLE IF NOT EXISTS kv (key text, value text);
  CREATE TABLE IF NOT EXISTS log (idx INT, term INT, data BLOB, type TEXT, dirty INT);
  CREATE UNIQUE INDEX IF NOT EXISTS kv_key on kv(key);
  CREATE UNIQUE INDEX IF NOT EXISTS log_idx_term on log(idx, term, dirty);
  CREATE UNIQUE INDEX IF NOT EXISTS log_idx on log(idx, dirty);
  `);
    this.cached_log_idx = null;
    this.log_term_sql = this.db.prepare(
      `select CAST(term as TEXT) as term
 from log
 where idx = CAST(? as INTEGER) and dirty = 0`
    );
    this.last_log_idx_sql = this.db.prepare(
      `select CAST(IFNULL(MAX(idx),0) as TEXT) as idx
 from log
 where dirty = 0`
    );
    this.add_log_sql = this.db.prepare(
      `insert into log (idx,term,data,type, dirty)
 values (CAST( ? as INTEGER),CAST(? as INTEGER),?,?, 0)`
    );
    this.delete_invalid_logs_sql = this.db.prepare(
      `update log
 set dirty = 1
 where idx > CAST(? as INTEGER)`
    );
    this.get_log_sql = this.db.prepare(
      `select CAST(idx as TEXT) as idx, CAST(term as TEXT) as term, data, type
 from log
  where idx = CAST(? as INTEGER) and dirty = 0`
    );
    this.get_logs_sql = this.db.prepare(
      `select CAST(idx as TEXT) as idx, CAST(term as TEXT) as term, data, type
 from log
 where idx > CAST(? as INTEGER) and dirty = 0`
    );

    this.kv_get_sql = this.db.prepare('select value from kv where key = ?');
    this.kv_set_sql = this.db.prepare(
      'replace into kv (key,value) values (?,?)'
    );
  }

  kv_get(key: string): string | null {
    const row = this.kv_get_sql.get(key);
    return row ? row.value : null;
  }

  kv_set(key: string, value: string): void {
    this.kv_set_sql.run(key, value);
  }

  get_logs_after(idx: bigint): Log[] {
    return this.get_logs_sql
      .all(idx.toString())
      .map(val => new Log(val.type, BigInt(val.idx), BigInt(val.term), val.data));
  }

  log_term(idx: bigint): bigint {
    const row = this.log_term_sql.get(idx.toString());
    return row ? BigInt(row.term) : BigInt(0);
  }

  last_log_idx(): bigint {
    if (this.cached_log_idx === null) {
      const row = this.last_log_idx_sql.get();
      this.cached_log_idx = row ? BigInt(row.idx) : BigInt(0);
    }
    return this.cached_log_idx;
  }

  add_log_to_storage(log: Log): void {
    this.cached_log_idx = log.idx;
    this.add_log_sql.run(
      log.idx.toString(),
      log.term.toString(),
      log.data,
      log.type
    );
  }

  delete_invalid_logs_from_storage(idx: bigint): void {
    this.delete_invalid_logs_sql.run(idx.toString());
    this.cached_log_idx = null;
  }
}
