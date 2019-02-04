import { AbstractStorageEngine } from '../src/interfaces'
import {Log} from '../src/messages'
import { EventLog } from './event_log'

export class TestStorageEngine extends AbstractStorageEngine {
    public kv: Map<string, string>
    public log: Log[]
    public events: EventLog

    constructor(eventlog: EventLog) {
        super()
        this.kv = new Map()
        this.log = []
        this.events = eventlog
    }

   public kv_get(key: string): string|null {
       const ret = this.kv.get(key) || null
       this.events.add('TestStorageEngine::kv_get', [key, ret])
       return ret
   }

   public kv_set(key: string, value: string): void {
       this.events.add('TestStorageEngine::kv_set', [key, value])
       this.kv.set(key, value)
   }

   public get_logs_after(idx: bigint): Log[] {
       let logs: Log[] = []
       if (idx === BigInt(0)) {
           logs = this.log
       } else if (idx < this.log.length) {
         logs = this.log.slice(Number(idx.toString()))
       }
       this.events.add('TestStorageEngine::get_logs_after', [idx, logs])
       return logs
   }

   public log_term(idx: bigint): bigint {
      const log_idx = idx - BigInt(1)
      let ret = BigInt(0)
      if (idx === BigInt(0)) {
          ret = BigInt(0)
      } else if (log_idx <= this.log.length - 1) {
        ret = this.log[Number(log_idx.toString())].term
      }
      this.events.add('TestStorageEngine::log_term', [idx, ret])
      return ret
   }

   public last_log_idx(): bigint {
    if (this.log.length === 0) {
        return BigInt(0)
    } else {
        return this.log[this.log.length - 1].idx
    }
   }

   public add_log_to_storage(log: Log): void {
       this.events.add('TestStorageEngine::add_log_to_storage', [log])
   }

   public delete_invalid_logs_from_storage(idx: bigint): void {
      this.events.add('TestStorageEngine::delete_invalid_logs_from_storage', [idx])

   }
  }
