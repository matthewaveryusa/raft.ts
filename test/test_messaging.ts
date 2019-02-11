import { AbstractMessagingEngine, message_cb } from '../src/interfaces'
import {Message} from '../src/messages'
import { EventLog } from './event_log'

export class TestMessagingEngine extends AbstractMessagingEngine {
    public events: EventLog
    public on_message_cb: message_cb
    constructor(eventlog: EventLog) {
      super()
      this.events = eventlog
      this.on_message_cb = (msg) => undefined
  }

 public start(address: string, on_message_cb: message_cb): void {
   this.on_message_cb = on_message_cb
   this.events.add('TestMessagingEngine::start', {address, on_message_cb}, null)
  }

 public send(peer_addr: string, message: Message): void {
    this.events.add('TestMessagingEngine::send', {peer_addr, message}, null)
  }

}
