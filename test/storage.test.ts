import {expect} from 'chai'
import * as del from 'del'
import { AbstractStorageEngine} from '../src/interfaces'
import {LmdbStorageEngine} from '../src/lmdb_storage'
import { MemStorage } from '../src/mem_storage'
import {Log} from '../src/messages'
import {SqliteStorageEngine} from '../src/sqlite_storage'
// chai uses these
/* tslint:disable no-unused-expression*/

del.sync(['test.lmdb/**', 'test.sqlite3'])
const lmdb = new LmdbStorageEngine('test.lmdb')
const sqlite = new SqliteStorageEngine('test.sqlite3')
const mem = new MemStorage()

const engines: AbstractStorageEngine[] = []
engines.push(lmdb)
engines.push(sqlite)
engines.push(mem)

describe('empty', () => {
engines.forEach((engine) => {
  engine.delete_invalid_logs_from_storage(BigInt(1))
  it('return no logs', () => {
  const ret = engine.get_logs_after(BigInt(1))
  expect(ret).to.be.empty
 })
  it('should be null', () => {
  const ret = engine.kv_get('key')
  expect(ret).to.be.null
  engine.kv_set('key', 'value')
 })
  engine.last_log_idx()
  engine.log_term(BigInt(1))
  const log = new Log()
  // empty test
  engine.add_log_to_storage(log)
})

})
