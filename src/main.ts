import {HttpMessagingEngine} from './http_messaging'
import {NodeTimeoutEngine} from './node_timeout'
import {IClientError, Server} from './server'
import {SqliteStorageEngine} from './sqlite_storage'

const config = ['127.0.0.1:9000', '127.0.0.1:9001', '127.0.0.1:9002']
const servers: Server[] = []
config.forEach((val) => {
  const s = new Server(val, config,
    new SqliteStorageEngine(`${val.replace(':', '_')}.sqlite`),
    new NodeTimeoutEngine((timeout_ms) => timeout_ms * Math.random() + 1),
    new HttpMessagingEngine())

  s.start_server()
  servers.push(s)
})

let leader_server = servers[0]
setInterval(() => {
  let ret = leader_server.on_client_request(Buffer.from(`hello @${new Date().toISOString()}`))
  if ('leader' in ret) {
    const ce = ret as IClientError
    if (ce.leader === null) { return }
    const leader_server_tmp = servers.find((s) => ce.leader === s.my_addr)
    if (!leader_server_tmp) {
        return
    } else {
      leader_server = leader_server_tmp
    }
    ret = leader_server.on_client_request(Buffer.from(`hello @${new Date().toISOString()}`))
  }
  console.log(ret)
}, 3000)
