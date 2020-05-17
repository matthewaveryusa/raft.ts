import { AbstractMessagingEngine, AbstractSerde } from '../src/interfaces';
import { Message } from '../src/messages';
import { EventLog } from './event_log';

export class TestMessagingEngine extends AbstractMessagingEngine {
  events: EventLog;
  constructor(serde: AbstractSerde, eventlog: EventLog) {
    super(serde);
    this.events = eventlog;
  }

  start(address: string): void {
    this.events.add('TestMessagingEngine::start', { address }, null);
  }

  send(peer_addr: string, message: Message): void {
    this.events.add('TestMessagingEngine::send', { peer_addr, message }, null);
  }

  stop(): void {
    // pass
  }
}
