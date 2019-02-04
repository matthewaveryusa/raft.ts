// http messaging with msgpack payloads
import BufferList = require('bl')
import * as http from 'http'
import mp = require('msgpack5')
import {AbstractMessagingEngine, message_cb} from './interfaces'
import {AppendRequest, AppendResponse, Log, Message, MessageType, VoteRequest, VoteResponse} from './messages'

const msgpack = mp()

function encode(message: Message): BufferList|undefined {
    switch (message.type) {
        case(MessageType.append_request):
        const ar = message as AppendRequest
        const logs = ar.logs.map((val) => {
          return [val.idx.toString(), val.term.toString(), val.type, val.data]
        })
        return msgpack.encode([ar.id.toString(), ar.from, ar.to, ar.type, ar.term.toString(),
           ar.prev_idx.toString(), ar.prev_term.toString(), ar.commit_idx.toString(), ar.last_idx.toString(), logs])
        case(MessageType.append_response):
        const as = message as AppendResponse
        return msgpack.encode([as.id.toString(), as.from, as.to, as.type, as.term.toString(),
           as.success])
        case(MessageType.vote_request):
        const vr = message as VoteRequest
        return msgpack.encode([vr.id.toString(), vr.from, vr.to, vr.type, vr.term.toString(),
          vr.last_idx.toString(), vr.last_term.toString()])
        case(MessageType.vote_response):
        const vs = message as VoteResponse
        return msgpack.encode([vs.id.toString(), vs.from, vs.to, vs.type, vs.term.toString(),
          vs.vote_granted])
    }
}

function decode(data: Buffer): Message|undefined {
    const arr: any[] = msgpack.decode(data)
    const id = BigInt(arr[0])
    const from = arr[1]
    const to = arr[2]
    const type: MessageType = arr[3]
    const term = BigInt(arr[4])

    switch (type) {
        case(MessageType.append_request):
        const logs = arr[9].map((val: any[]): Log => {
          const l = new Log()
          l.idx = BigInt(val[0])
          l.term = BigInt(val[1])
          l.type = val[2]
          l.data = val[3]
          return l
        })
        return new AppendRequest(id, from, to, term,
          BigInt(arr[5]), BigInt(arr[6]), BigInt(arr[7]), BigInt(arr[8]), logs)
        case(MessageType.append_response):
        return new AppendResponse(id, from, to, term,
          arr[5])
        case(MessageType.vote_request):
        return new VoteRequest(id, from, to, term,
          BigInt(arr[5]), BigInt(arr[6]))
        case(MessageType.vote_response):
        return new VoteResponse(id, from, to, term,
           arr[5])
    }
}

export class HttpMessagingEngine extends AbstractMessagingEngine {

    private http_client_agent: http.Agent
    private http_server: http.Server
    private on_message_cb: message_cb
    private open_connections: Map<string, http.ServerResponse>

    constructor() {
      super()
      this.http_client_agent = new http.Agent({ keepAlive: true })
      this.on_message_cb = () => undefined
      this.http_server = http.createServer((req, res) => this.http_callback(req, res))
      this.http_server.on('clientError', (err, socket) => {
        socket.end('HTTP/1.1 400 Bad Request\r\n\r\n')
      })

      this.http_server.maxHeadersCount = 100
      this.http_server.keepAliveTimeout = 1000 * 60
      this.open_connections = new Map()
    }

    public start(address: string, on_message_cb: message_cb): void {
      this.on_message_cb = on_message_cb
      const [ip, port] = address.split(':')
      this.http_server.listen(parseInt(port, 10), ip)
    }

    public send(peer_addr: string, message: Message): void {
      const wire_message = encode(message)
      if (!wire_message) {
        return
      }
      const open_connection = this.open_connections.get(message.to)
      if (open_connection) {
        open_connection.end(wire_message)
        this.open_connections.delete(message.to)
        return
      }

      const [ip, port] = peer_addr.split(':')
      const options = {
        agent: this.http_client_agent,
        headers: {
          'Content-Length': wire_message.length,
        },
        host: ip,
        method: 'POST',
        port: parseInt(port, 10),
      }
      const request = http.request(options, (res) => {
        let data: Buffer|null = null
        res.on('data', (chunk) => {
          if (data) {
              data = Buffer.concat([data, chunk])
          } else {
              data = chunk
          }
        })
        res.on('end', () => {
          if (data === null || data.length === 0 ) { return }
          const d = decode(data)
          if (d) {
            this.on_message_cb(d)
          }
        })

        res.on('error', (err) => {
          console.log('received error', err)
        })
      })
      request.end(wire_message)
    }

    public http_callback(req: http.IncomingMessage, res: http.ServerResponse) {
      let data: Buffer|null = null
      req.on('data', (chunk: Buffer) => {
          if (data) {
              data = Buffer.concat([data, chunk])
          } else {
              data = chunk
          }
      })
      req.on('end', () => {
        if (data === null) {
          res.end()
          return
        }
        const msg = decode(data)
        if (!msg) {
          return
        }
        const open_connection = this.open_connections.get(msg.from)
        if (open_connection) {
          // agent will cache these connections if needed
          console.log('ending connection because one already open')
          res.end()
        } else {
          // keep this response open and use it if a message needs a response back
          res.on('error', (err) => {
            console.log('response has error, removing')
            this.open_connections.delete(msg.from)
          })
          this.open_connections.set(msg.from, res)
        }
        const d = decode(data)
        if (!d) {
          return
        }
        this.on_message_cb(d)
      })

      req.on('error', (err) => {
        console.log('request error', err)
      })
    }
}
