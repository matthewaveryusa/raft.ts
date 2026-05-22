import { expect } from 'chai';
import * as del from 'del';
import { AbstractStorageEngine } from '../src/interfaces';
import { MemStorage } from '../src/mem_storage';
import { Log, LogType } from '../src/messages';
// chai uses these
/* tslint:disable no-unused-expression*/

// The new `lmdb` package opens its data file at the given path directly, so
// we no longer need a directory wrapper. Just clear any stale fixtures.
del.sync(['test.lmdb', 'test.lmdb-lock', 'test.sqlite3']);

interface Iengine {
  name: string;
  engine: AbstractStorageEngine;
}

const engines: Iengine[] = [];

// lmdb and better-sqlite3 are both native modules that can fail to build on
// some hosts (notably modern Node on arm64 darwin). Load them lazily so the
// in-memory tests can still run if their bindings are unavailable.
try {
  // tslint:disable-next-line: no-var-requires
  const { LmdbStorageEngine } = require('../src/lmdb_storage');
  const lmdb = new LmdbStorageEngine('test.lmdb', 2 * 1024 * 1024 * 1024);
  engines.push({ name: 'lmdb', engine: lmdb });
} catch (e) {
  // tslint:disable-next-line: no-console
  console.warn(
    `skipping lmdb storage tests: ${e instanceof Error ? e.message : e}`
  );
}

try {
  // tslint:disable-next-line: no-var-requires
  const { SqliteStorageEngine } = require('../src/sqlite_storage');
  const sqlite = new SqliteStorageEngine('test.sqlite3');
  engines.push({ name: 'sqlite', engine: sqlite });
} catch (e) {
  // tslint:disable-next-line: no-console
  console.warn(
    `skipping sqlite storage tests: ${e instanceof Error ? e.message : e}`
  );
}

const mem = new MemStorage();
engines.push({ name: 'mem', engine: mem });

engines.forEach(iter => {
  const engine = iter.engine;
  describe(`engine: ${iter.name}`, () => {
    describe('empty', () => {
      it('return no logs', () => {
        const ret = engine.get_logs_after(BigInt(0));
        expect(ret).to.be.empty;
        const ret2 = engine.get_logs_after(BigInt(1));
        expect(ret2).to.be.empty;
      });
      it('should be null', () => {
        const ret = engine.kv_get('key');
        expect(ret).to.be.null;
        engine.kv_set('key', 'value');
      });
      it('get last log', () => {
        const ret = engine.last_log_idx();
        expect(ret).equal(BigInt(0));
        engine.log_term(BigInt(0));
        engine.log_term(BigInt(1));
      });
      it('delete on empty', () => {
        engine.delete_invalid_logs_from_storage(BigInt(1));
        engine.delete_invalid_logs_from_storage(BigInt(0));
      });
    });
    describe('not empty', () => {
      it('add logs', () => {
        let log = new Log(
          LogType.message,
          BigInt(1),
          BigInt(1),
          Buffer.from('test1')
        );
        engine.add_log_to_storage(log);
        log = new Log(
          LogType.message,
          BigInt(2),
          BigInt(1),
          Buffer.from('test2')
        );
        engine.add_log_to_storage(log);
        log = new Log(
          LogType.message,
          BigInt(3),
          BigInt(2),
          Buffer.from('test3')
        );
        engine.add_log_to_storage(log);
        log = new Log(
          LogType.message,
          BigInt(4),
          BigInt(2),
          Buffer.from('test4')
        );
        engine.add_log_to_storage(log);
      });

      it('return logs', () => {
        const ret = engine.get_logs_after(BigInt(1));
        expect(ret.length).equals(3);
        const ret2 = engine.last_log_idx();
        expect(ret2).to.equal(BigInt(4));
      });

      it('returns logs with bigint idx and term', () => {
        // BUGFIX (sqlite_storage): previously get_logs_after returned Log
        // objects whose idx and term were strings (from CAST AS TEXT) rather
        // than bigints. Verify the contract is honored across engines.
        const ret = engine.get_logs_after(BigInt(0));
        expect(ret.length).to.be.greaterThan(0);
        for (const log of ret) {
          expect(typeof log.idx).to.equal('bigint');
          expect(typeof log.term).to.equal('bigint');
        }
      });

      it('delete logs', () => {
        engine.delete_invalid_logs_from_storage(BigInt(2));
        const ret = engine.get_logs_after(BigInt(1));
        expect(ret.length).equals(1);
        let ret2 = engine.last_log_idx();
        expect(ret2).to.equal(BigInt(2));
        ret2 = engine.log_term(BigInt(2));
        expect(ret2).to.equal(BigInt(1));
      });

      it('kv get and set', () => {
        engine.kv_set('key', 'value');
        const ret = engine.kv_get('key');
        expect(ret).equals('value');
      });

      it('kv overwrites', () => {
        engine.kv_set('overwrite', 'first');
        engine.kv_set('overwrite', 'second');
        expect(engine.kv_get('overwrite')).to.equal('second');
      });

      it('log_term on out-of-range idx returns 0', () => {
        // idx 0 is the canonical "before-the-log" sentinel
        expect(engine.log_term(BigInt(0))).to.equal(BigInt(0));
        // any far-future idx that has not been written yet
        expect(engine.log_term(BigInt(99999999))).to.equal(BigInt(0));
      });

      it('get_logs_after past last_log_idx returns empty', () => {
        const last = engine.last_log_idx();
        expect(engine.get_logs_after(last + BigInt(100))).to.eql([]);
      });

      it('delete_invalid_logs_from_storage with idx beyond last is a no-op', () => {
        const before = engine.last_log_idx();
        engine.delete_invalid_logs_from_storage(before + BigInt(1000));
        expect(engine.last_log_idx()).to.equal(before);
      });

      it('delete_invalid_logs_from_storage(0) clears all logs', () => {
        engine.delete_invalid_logs_from_storage(BigInt(0));
        expect(engine.last_log_idx()).to.equal(BigInt(0));
        expect(engine.get_logs_after(BigInt(0))).to.eql([]);
      });
    });
  });
});
