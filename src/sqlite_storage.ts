// sqlite storage
import * as sqlite from 'better-sqlite3'
import {AbstractStorageEngine} from './interfaces'
import {Log} from './messages'

export class SqliteStorageEngine extends AbstractStorageEngine {
    private db: sqlite.Database

    private log_term_sql: sqlite.Statement
    private last_log_idx_sql: sqlite.Statement
    private add_log_sql: sqlite.Statement
    private delete_invalid_logs_sql: sqlite.Statement
    private get_log_sql: sqlite.Statement
    private get_logs_sql: sqlite.Statement
    private kv_get_sql: sqlite.Statement
    private kv_set_sql: sqlite.Statement

    private cached_log_idx: bigint|null

    constructor(db_name: string) {
      super()
      this.db = sqlite(db_name)
      this.db.exec(`CREATE TABLE IF NOT EXISTS kv (key text, value text);
  CREATE TABLE IF NOT EXISTS log (idx INT, term INT, data BLOB, type TEXT, dirty INT);
  CREATE UNIQUE INDEX IF NOT EXISTS kv_key on kv(key);
  CREATE UNIQUE INDEX IF NOT EXISTS log_idx_term on log(idx, term, dirty);
  CREATE UNIQUE INDEX IF NOT EXISTS log_idx on log(idx, dirty);
  `)
      // sql
      this.cached_log_idx = null
      this.log_term_sql = this.db.prepare(
`select CAST(term as TEXT) as term
 from log
 where idx = CAST(? as INTEGER) and dirty = 0`)
      this.last_log_idx_sql = this.db.prepare(
`select CAST(IFNULL(MAX(idx),0) as TEXT) as idx
 from log
 where dirty = 0`)
      this.add_log_sql = this.db.prepare(
`insert into log (idx,term,data,type, dirty)
 values (CAST( ? as INTEGER),CAST(? as INTEGER),?,?, 0)`)
      this.delete_invalid_logs_sql = this.db.prepare(
`update log
 set dirty = 1
 where idx > CAST(? as INTEGER)`)
      this.get_log_sql = this.db.prepare(
`select CAST(idx as TEXT) as idx, CAST(term as TEXT) as term, data, type
 from log
  where idx = CAST(? as INTEGER) and dirty = 0`)
      this.get_logs_sql = this.db.prepare(
`select CAST(idx as TEXT) as idx, CAST(term as TEXT) as term, data, type
 from log
 where idx > CAST(? as INTEGER) and dirty = 0`)

      this.kv_get_sql = this.db.prepare('select value from kv where key = ?')
      this.kv_set_sql = this.db.prepare('replace into kv (key,value) values (?,?)')
    }

    public kv_get(key: string): string|null {
      const row = this.kv_get_sql.get(key)
      if (row) {
        return row.value
      } else {
        return null
      }
    }

    public kv_set(key: string, value: string): void {
      this.kv_set_sql.run(key, value)
    }

    public get_logs_after(idx: bigint): Log[] {
      return this.get_logs_sql.all(idx.toString()).map((val) => {
        const log = new Log()
        log.term = BigInt(val.term)
        log.idx = BigInt(val.idx)
        log.type = val.type
        log.data = val.data
        return log
      })
    }

    public log_term(idx: bigint): bigint {
      const row = this.log_term_sql.get(idx.toString())
      if (row) {
        return BigInt(row.term)
      } else {
        return BigInt(0)
      }
    }

    public last_log_idx(): bigint {
      if (this.cached_log_idx === null) {
        const row = this.last_log_idx_sql.get()
        this.cached_log_idx = BigInt(row.idx)
      }
      return this.cached_log_idx
    }

    public add_log_to_storage(log: Log): void {
      this.cached_log_idx = log.idx
      this.add_log_sql.run(log.idx.toString(), log.term.toString(), log.data, log.type)
    }

    public delete_invalid_logs_from_storage(idx: bigint): void {
      this.delete_invalid_logs_sql.run(idx.toString())
      this.cached_log_idx = null
    }
  }
