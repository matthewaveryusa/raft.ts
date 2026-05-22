import * as mqtt from 'mqtt';
import { AbstractMessagingEngine, AbstractSerde } from './interfaces';
import { Logger } from './logger';
import { Message } from './messages';

export class MqttMessagingEngine extends AbstractMessagingEngine {
  private mqtt_client: mqtt.Client | undefined;

  constructor(
    serde: AbstractSerde,
    private broker_address: string,
    logger?: Logger
  ) {
    super(serde, logger);
    this.mqtt_client = undefined;
  }

  start(address: string): void {
    this.mqtt_client = mqtt.connect(this.broker_address);
    this.mqtt_client.on('connect', () => {
      if (this.mqtt_client === undefined) return;
      this.mqtt_client.subscribe(address, () => {});
    });
    this.mqtt_client.on('message', data => {
      const d = this.decode(Buffer.from(data));
      if (!d) {
        return;
      }
      this.emit('message', d);
    });
  }

  stop() {
    if (this.mqtt_client === undefined) return;
    this.mqtt_client.end();
    this.mqtt_client = undefined;
  }

  send(peer_addr: string, message: Message): void {
    const wire_message = this.encode(message);
    if (!wire_message) {
      return;
    }
    if (!this.mqtt_client) {
      return;
    }
    // Pass the raw bytes (not a UTF-8 string) so msgpack binary survives the
    // wire. wire_message is a BufferList; .slice(0) flattens it to a Buffer.
    this.mqtt_client.publish(peer_addr, wire_message.slice(0));
  }
}
