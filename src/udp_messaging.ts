import * as dgram from 'dgram';
import { AbstractMessagingEngine, AbstractSerde } from './interfaces';
import { Logger } from './logger';
import { Message } from './messages';

export class UDPMessagingEngine extends AbstractMessagingEngine {
  private server: dgram.Socket | undefined;
  private client: dgram.Socket | undefined;

  constructor(serde: AbstractSerde, logger?: Logger) {
    super(serde, logger);
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

    // Keep one outgoing socket alive for the lifetime of the engine. Creating
    // a fresh dgram socket per message exhausts ephemeral ports under load
    // and is wasteful even at modest heartbeat rates.
    this.client = dgram.createSocket('udp4');
    this.client.on('error', err => {
      this.logger.warn?.('udp: client socket error', { err });
    });
  }

  stop() {
    this.server?.close();
    this.server = undefined;
    this.client?.close();
    this.client = undefined;
  }

  send(peer_addr: string, message: Message): void {
    const wire_message = this.encode(message);
    if (!wire_message) {
      return;
    }
    if (!this.client) {
      return;
    }
    const [ip, port] = peer_addr.split(':');
    this.client.send(wire_message.slice(0), Number(port), ip, err => {
      if (err) {
        this.logger.debug?.('udp: send error', { peer_addr, err });
      }
    });
  }
}
