import { AbstractTimeoutEngine } from '../src/interfaces'
import { EventLog } from './event_log'

export class TestTimeoutEngine extends AbstractTimeoutEngine {
    public events: EventLog
    constructor(events: EventLog) {
        super()
        this.events = events
    }

 public set(name: string, timeout_ms: number, callback: () => void): void {
     this.events.add('TestTimeoutEngine::set', [name, timeout_ms, callback])
 }

 public clear(name: string): void {
    this.events.add('TestTimeoutEngine::clear', [name])
 }
}
