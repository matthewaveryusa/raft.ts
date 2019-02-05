import { AbstractTimeoutEngine } from '../src/interfaces'
import { EventLog } from './event_log'

export class TestTimeoutEngine extends AbstractTimeoutEngine {
    public events: EventLog
    constructor(events: EventLog) {
        super((timeout_ms: number) => timeout_ms)
        this.events = events
    }

 public set(name: string, timeout_ms: number, callback: () => void): void {
     this.events.add('TestTimeoutEngine::set', {name, timeout_ms, callback}, null)
 }
 public set_varied(name: string, timeout_ms: number, callback: () => void): void {
    this.events.add('TestTimeoutEngine::set_varied', {name, timeout_ms, callback}, null)
}

 public clear(name: string): void {
    this.events.add('TestTimeoutEngine::clear', {name}, null)
 }
}
