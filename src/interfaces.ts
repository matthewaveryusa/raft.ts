import { Log, Message } from './messages';
import { EventEmitter } from 'events';
import BufferList = require('bl');

export type variance_func_t = (timeout_ms: number) => number;
export abstract class AbstractTimeoutEngine {
  protected variance_function: variance_func_t;
  constructor(variance_function: variance_func_t) {
    this.variance_function = variance_function;
  }
  abstract set(name: string, timeout_ms: number, callback: () => void): void;
  abstract set_varied(
    name: string,
    timeout_ms: number,
    callback: () => void
  ): void;
  abstract clear(name: string): void;
}

export abstract class AbstractStorageEngine {
  abstract kv_get(key: string): string | null;
  abstract kv_set(key: string, value: string): void;
  abstract get_logs_after(idx: bigint): Log[];
  abstract log_term(idx: bigint): bigint;
  abstract last_log_idx(): bigint;
  abstract add_log_to_storage(log: Log): void;
  abstract delete_invalid_logs_from_storage(idx: bigint): void;
}

export abstract class AbstractMessagingEngine extends EventEmitter {
  constructor(private serde: AbstractSerde) {
    super();
  }
  encode(message: Message): BufferList | undefined {
    return this.serde.encode(message);
  }
  decode(data: Buffer): Message | undefined {
    return this.serde.decode(data);
  }
  abstract start(address: string): void;
  abstract send(peer_addr: string, message: Message): void;
  abstract stop(): void;
}

export abstract class AbstractSerde {
  abstract encode(message: Message): BufferList | undefined;
  abstract decode(data: Buffer): Message | undefined;
}
