import { AbstractStorageEngine } from './interfaces'
import {Log} from './messages'

export class MemStorage extends AbstractStorageEngine {
    public kv: Map<string, string>
    public log: Log[]

    constructor() {
        super()
        this.kv = new Map()
        this.log = []
    }

   public kv_get(key: string): string|null {
       const ret = this.kv.get(key) || null
       return ret
   }

   public kv_set(key: string, value: string): void {
       this.kv.set(key, value)
   }

   public get_logs_after(idx: bigint): Log[] {
       let logs: Log[] = []
       if (idx === BigInt(0)) {
           logs = this.log
       } else if (idx < this.log.length) {
         logs = this.log.slice(Number(idx.toString()))
       }
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
       this.log.push(log)
   }

   public delete_invalid_logs_from_storage(idx: bigint): void {
    const log_idx = idx
    if (idx === BigInt(0)) {
        this.log = []
    } else if (log_idx <= this.log.length - 1) {
        this.log = this.log.slice(0, Number(log_idx.toString()))
    }
   }

   public latest_config_before_or_at(idx: bigint): Log | null {
    return null
   }
  }
