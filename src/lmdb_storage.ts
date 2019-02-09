// lmdb storage
import mp = require('msgpack5')
import {AbstractStorageEngine} from './interfaces'
import {Log} from './messages'
import * as lmdb from './node-lmdb'

const msgpack = mp()

export class LmdbStorageEngine extends AbstractStorageEngine {
    private cached_log_idx: bigint|null
    private env: lmdb.Env
    private kvdb: lmdb.Dbi
    private logdb: lmdb.Dbi

    constructor(db_name: string) {
      super()
      this.cached_log_idx = null
      this.env = new lmdb.Env()
      this.env.open({
        mapSize: 2 * 1024 * 1024 * 1024, // maximum database size
        maxDbs: 3,
        path: `${__dirname}/"${db_name}`,
    })

      this.kvdb = this.env.openDbi({
        create: true, // will create if database did not exist
        name: 'key',
    })

      this.logdb = this.env.openDbi({
        create: true, // will create if database did not exist
        name: 'log',
    })
    }

    public kv_get(key: string): string|null {
      const txn = this.env.beginTxn()
      const value = txn.getString(this.kvdb, key)
      txn.commit()
      return value
    }

    public kv_set(key: string, value: string): void {
        const txn = this.env.beginTxn()
        txn.putString(this.kvdb, key, value)
        txn.commit()
    }

    public get_logs_after(idx: bigint): Log[] {
        const logs: Log[] = []
        const txn = this.env.beginTxn()
        const cursor = new lmdb.Cursor(txn, this.logdb)
        idx++
        for (let found = cursor.goToRange(idx.toString().padStart(20, '0'));
          found !== null; found = cursor.goToNext()) {
          const data = msgpack.decode(cursor.getCurrentString())
          const log = new Log()
          log.term = BigInt(data[0])
          log.idx = BigInt(found)
          log.data = data[1]
          log.type = data[2]
        }
        txn.commit()
        return logs
    }

    public log_term(idx: bigint): bigint {
        const txn = this.env.beginTxn()
        const value = txn.getBinary(this.kvdb, idx.toString().padStart(20, '0'))
        txn.commit()
        if (value) {
        const row = msgpack.decode(value)
        return BigInt(row[0])
      } else {
        return BigInt(0)
      }
    }

    public last_log_idx(): bigint {
      if (this.cached_log_idx === null) {
        const txn = this.env.beginTxn()
        const cursor = new lmdb.Cursor(txn, this.logdb)
        const idx = cursor.goToLast()
        txn.commit()
        if (idx) {
          this.cached_log_idx = BigInt(idx)
        } else {
          this.cached_log_idx = BigInt(0)
        }
      }
      return this.cached_log_idx
    }

    public add_log_to_storage(log: Log): void {
      const txn = this.env.beginTxn()
      txn.putBinary(this.kvdb, log.idx.toString().padStart(20, '0'),
       msgpack.encode([log.term.toString(), log.data, log.type]).slice())
      txn.commit()
      this.cached_log_idx = log.idx
    }

    public delete_invalid_logs_from_storage(idx: bigint): void {
      const txn = this.env.beginTxn()
      const cursor = new lmdb.Cursor(txn, this.logdb)
      // one after
      idx++
      for (let found = cursor.goToRange(idx.toString()); found !== null; found = cursor.goToNext()) {
        cursor.del()
      }
      txn.commit()
      this.cached_log_idx = null
    }
  }
