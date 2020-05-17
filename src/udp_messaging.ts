import * as dgram from 'dgram';
import { AbstractMessagingEngine, AbstractSerde } from './interfaces';
import { Message } from './messages';

export class UDPMessagingEngine extends AbstractMessagingEngine {
  private server: dgram.Socket | undefined;

  constructor(serde: AbstractSerde) {
    super(serde);
  }

  start(address: string): void {
    this.server = dgram.createSocket('udp4');
    const [ip, port] = address.split(':');
    this.server.bind(Number(port), ip);
    this.server.on('message', data => {
      const d = this.decode(data);
      if (!d) {
        return;
      }
      this.emit('message', d);
    });
  }

  stop() {
    this.server?.close();
    this.server = undefined;
  }

  send(peer_addr: string, message: Message): void {
    const wire_message = this.encode(message);
    if (!wire_message) {
      return;
    }
    if (!this.server) {
      return;
    }
    const client = dgram.createSocket('udp4');
    const [ip, port] = peer_addr.split(':');
    client.send(wire_message.slice(0), Number(port), ip, err => {
      client.close();
    });
  }
}
