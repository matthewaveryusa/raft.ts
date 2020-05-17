import { UDPMessagingEngine } from './udp_messaging';
import { HttpMessagingEngine } from './http_messaging';
import { NodeTimeoutEngine } from './node_timeout';
import { ClientError, Server } from './server';
import { SqliteStorageEngine } from './sqlite_storage';
import { MsgpackSerde } from './msgpack_serde';
import { MemStorage } from './mem_storage';
import { LmdbStorageEngine } from './lmdb_storage';

const config = ['127.0.0.1:9000', '127.0.0.1:9001', '127.0.0.1:9002'];
const servers: Server[] = [];
const msgpack_serde = new MsgpackSerde();

let s = new Server(
  '127.0.0.1:9000',
  config,
  new SqliteStorageEngine(`9000.sqlite`),
  new NodeTimeoutEngine(timeout_ms => timeout_ms * (Math.random() + 1)),
  new HttpMessagingEngine(msgpack_serde)
);
s.start_server();
s.on('current_leader', l => console.log('9000 current_leader', l));
s.on('commit_idx', l => console.log('9000 commit_idx', l));
s.on('no_common_log', (pidx, pterm, idx, term) =>
  console.log('9000 no common log', pidx, pterm, idx, term)
);
s.on('role_change', l => console.log('9000 role change', l));
servers.push(s);

s = new Server(
  '127.0.0.1:9001',
  config,
  new LmdbStorageEngine(`lmdb9001`, 2000000000),
  new NodeTimeoutEngine(timeout_ms => timeout_ms * (Math.random() + 1)),
  new HttpMessagingEngine(msgpack_serde)
);
s.start_server();
s.on('current_leader', l => console.log('9001 current_leader', l));
s.on('commit_idx', l => console.log('9001 commit_idx', l));
s.on('no_common_log', (pidx, pterm, idx, term) =>
  console.log('9001 no common log', pidx, pterm, idx, term)
);
s.on('role_change', l => console.log('9001 role change', l));
servers.push(s);

s = new Server(
  '127.0.0.1:9002',
  config,
  new MemStorage(),
  new NodeTimeoutEngine(timeout_ms => timeout_ms * (Math.random() + 1)),
  new HttpMessagingEngine(msgpack_serde)
);
s.start_server();
s.on('current_leader', l => console.log('9002 current_leader', l));
s.on('commit_idx', l => console.log('9002 commit_idx', l));
s.on('no_common_log', (pidx, pterm, idx, term) =>
  console.log('9002 no common log', pidx, pterm, idx, term)
);
s.on('role_change', l => console.log('9002 role change', l));
servers.push(s);

let leader_server = servers[0];
setInterval(() => {
  let ret = leader_server.on_client_request(
    Buffer.from(`hello @${new Date().toISOString()}`)
  );
  if ('leader' in ret) {
    const ce = ret as ClientError;
    if (ce.leader === null) {
      return;
    }
    const leader_server_tmp = servers.find(s => ce.leader === s.my_addr);
    if (!leader_server_tmp) {
      return;
    } else {
      leader_server = leader_server_tmp;
    }
    ret = leader_server.on_client_request(
      Buffer.from(`hello @${new Date().toISOString()}`)
    );
  }
  console.log(ret);
}, 3000);
