import {Log, Message} from './messages'

export type variance_func_t = (timeout_ms: number) => number
export abstract class AbstractTimeoutEngine {
    protected variance_function: variance_func_t
    constructor(variance_function: variance_func_t) {
        this.variance_function = variance_function
    }
    public abstract set(name: string, timeout_ms: number, callback: () => void): void
    public abstract set_varied(name: string, timeout_ms: number, callback: () => void): void
    public abstract clear(name: string): void
}

export abstract class AbstractStorageEngine {
    public abstract kv_get(key: string): string|null
    public abstract kv_set(key: string, value: string): void
    public abstract get_logs_after(idx: bigint): Log[]
    public abstract log_term(idx: bigint): bigint
    public abstract last_log_idx(): bigint
    public abstract add_log_to_storage(log: Log): void
    public abstract delete_invalid_logs_from_storage(idx: bigint): void
}

export type message_cb = (message: Message) => void
export abstract class AbstractMessagingEngine {
    public abstract start(address: string, on_message_cb: message_cb): void
    public abstract send(peer_addr: string, message: Message): void
    public abstract stop(): void
}
