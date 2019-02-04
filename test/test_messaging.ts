import { AbstractMessagingEngine, message_cb } from '../src/interfaces'
import {Message} from '../src/messages'
import { EventLog } from './event_log'

export class TestMessagingEngine extends AbstractMessagingEngine {
    public events: EventLog
    constructor(eventlog: EventLog) {
      super()
      this.events = eventlog
  }

 public start(address: string, on_message_cb: message_cb): void {
    this.events.add('TestMessagingEngine::start', [address, on_message_cb])
  }

 public send(peer_addr: string, message: Message): void {
    this.events.add('TestMessagingEngine::send', [peer_addr, message])
  }

}
