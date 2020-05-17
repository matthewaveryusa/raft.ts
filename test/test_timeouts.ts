import { AbstractTimeoutEngine } from '../src/interfaces';
import { EventLog } from './event_log';

export class TestTimeoutEngine extends AbstractTimeoutEngine {
  events: EventLog;
  timeouts: Map<string, () => void>;
  constructor(events: EventLog) {
    super((timeout_ms: number) => timeout_ms);
    this.events = events;
    this.timeouts = new Map();
  }

  set(name: string, timeout_ms: number, callback: () => void): void {
    this.events.add(
      'TestTimeoutEngine::set',
      { name, timeout_ms, callback },
      null
    );
    this.timeouts.set(name, callback);
  }
  set_varied(name: string, timeout_ms: number, callback: () => void): void {
    this.events.add(
      'TestTimeoutEngine::set_varied',
      { name, timeout_ms, callback },
      null
    );
  }

  clear(name: string): void {
    this.events.add('TestTimeoutEngine::clear', { name }, null);
  }
}
