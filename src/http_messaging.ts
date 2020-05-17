// http messaging with msgpack payloads
import * as http from 'http';
import { AbstractMessagingEngine, AbstractSerde } from './interfaces';
import { Message } from './messages';

export class HttpMessagingEngine extends AbstractMessagingEngine {
  private http_client_agent: http.Agent;
  private http_server: http.Server;
  private open_connections: Map<string, http.ServerResponse>;

  constructor(serde: AbstractSerde) {
    super(serde);
    this.http_client_agent = new http.Agent({ keepAlive: true });
    this.http_server = http.createServer((req, res) =>
      this.http_callback(req, res)
    );
    this.http_server.on('clientError', (err, socket) => {
      socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
    });

    this.http_server.maxHeadersCount = 100;
    this.http_server.keepAliveTimeout = 1000 * 60;
    this.open_connections = new Map();
  }

  start(address: string): void {
    const [ip, port] = address.split(':');
    this.http_server.listen(Number(port), ip);
  }

  stop() {
    this.http_server.close();
    this.http_client_agent.destroy();
  }

  send(peer_addr: string, message: Message): void {
    const wire_message = this.encode(message);
    if (!wire_message) {
      return;
    }
    const open_connection = this.open_connections.get(message.to);
    if (open_connection) {
      open_connection.end(wire_message);
      this.open_connections.delete(message.to);
      return;
    }

    const [ip, port] = peer_addr.split(':');
    const options = {
      agent: this.http_client_agent,
      headers: {
        'Content-Length': wire_message.length,
      },
      host: ip,
      method: 'POST',
      port: Number(port),
    };
    const request = http.request(options, res => {
      let data: Buffer | null = null;
      res.on('data', chunk => {
        if (data) {
          data = Buffer.concat([data, chunk]);
        } else {
          data = chunk;
        }
      });
      res.on('end', () => {
        if (data === null || data.length === 0) {
          return;
        }
        const d = this.decode(data);
        if (d) {
          this.emit('message', d);
        }
      });

      res.on('error', err => {
        console.log('received error', err);
      });
    });
    request.end(wire_message);
  }

  http_callback(req: http.IncomingMessage, res: http.ServerResponse) {
    let data: Buffer | null = null;
    req.on('data', (chunk: Buffer) => {
      if (data) {
        data = Buffer.concat([data, chunk]);
      } else {
        data = chunk;
      }
    });
    req.on('end', () => {
      if (data === null) {
        res.end();
        return;
      }
      const msg = this.decode(data);
      if (!msg) {
        return;
      }
      const open_connection = this.open_connections.get(msg.from);
      if (open_connection) {
        // agent will cache these connections if needed
        console.log('ending connection because one already open');
        res.end();
      } else {
        // keep this response open and use it if a message needs a response back
        res.on('error', err => {
          console.log('response has error, removing');
          this.open_connections.delete(msg.from);
        });
        this.open_connections.set(msg.from, res);
      }
      const d = this.decode(data);
      if (!d) {
        return;
      }
      this.emit('message', d);
    });

    req.on('error', err => {
      console.log('request error', err);
    });
  }
}
