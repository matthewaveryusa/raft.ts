import {expect} from 'chai'
import * as del from 'del'
import { mkdirSync } from 'fs'
import { AbstractStorageEngine } from '../src/interfaces'
import {LmdbStorageEngine} from '../src/lmdb_storage'
import { MemStorage } from '../src/mem_storage'
import {Log, LogType} from '../src/messages'
import {SqliteStorageEngine} from '../src/sqlite_storage'
// chai uses these
/* tslint:disable no-unused-expression*/

del.sync(['test.lmdb/**', 'test.sqlite3'])
try {
  mkdirSync('test.lmdb')
} catch (e) {
  if (e.code !== 'EEXIST') {
    throw e
  }
}
const lmdb = new LmdbStorageEngine('test.lmdb', 2 * 1024 * 1024 * 1024)
const sqlite = new SqliteStorageEngine('test.sqlite3')
const mem = new MemStorage()

interface Iengine {
  name: string,
  engine: AbstractStorageEngine
}

const engines: Iengine[] = []
engines.push({name: 'lmdb', engine: lmdb})
engines.push({name: 'sqlite', engine: sqlite})
engines.push({name: 'mem', engine: mem})

engines.forEach((iter) => {
const engine = iter.engine
describe(`engine: ${iter.name}`, () => {
describe('empty', () => {
  it('return no logs', () => {
  const ret = engine.get_logs_after(BigInt(0))
  expect(ret).to.be.empty
  const ret2 = engine.get_logs_after(BigInt(1))
  expect(ret2).to.be.empty
  })
  it('should be null', () => {
  const ret = engine.kv_get('key')
  expect(ret).to.be.null
  engine.kv_set('key', 'value')
  })
  it('get last log', () => {
  const ret = engine.last_log_idx()
  expect(ret).equal(BigInt(0))
  engine.log_term(BigInt(0))
  engine.log_term(BigInt(1))
  })
  it('delete on empty', () => {
    engine.delete_invalid_logs_from_storage(BigInt(1))
    engine.delete_invalid_logs_from_storage(BigInt(0))
  })
})
describe('not empty', () => {
  it('add logs', () => {
  let log = new Log(LogType.message, BigInt(1), BigInt(1), Buffer.from('test1'))
  engine.add_log_to_storage(log)
  log = new Log(LogType.message, BigInt(2), BigInt(1), Buffer.from('test2'))
  engine.add_log_to_storage(log)
  log = new Log(LogType.message, BigInt(3), BigInt(2), Buffer.from('test3'))
  engine.add_log_to_storage(log)
  log = new Log(LogType.message, BigInt(4), BigInt(2), Buffer.from('test4'))
  engine.add_log_to_storage(log)
  })

  it('return logs', () => {
    const ret = engine.get_logs_after(BigInt(1))
    expect(ret.length).equals(3)
    const ret2 = engine.last_log_idx()
    expect(ret2).to.equal(BigInt(4))
  })

  it('delete logs', () => {
    engine.delete_invalid_logs_from_storage(BigInt(2))
    const ret = engine.get_logs_after(BigInt(1))
    expect(ret.length).equals(1)
    let ret2 = engine.last_log_idx()
    expect(ret2).to.equal(BigInt(2))
    ret2 = engine.log_term(BigInt(2))
    expect(ret2).to.equal(BigInt(1))
  })

  it('kv get and set', () => {
    engine.kv_set('key', 'value')
    const ret = engine.kv_get('key')
    expect(ret).equals('value')
  })
})
})
})
