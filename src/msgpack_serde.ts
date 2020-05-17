import { AbstractSerde } from './interfaces';
import mp = require('msgpack5');
const msgpack = mp();

import {
  AppendRequest,
  AppendResponse,
  Log,
  Message,
  MessageType,
  VoteRequest,
  VoteResponse,
  LogType,
} from './messages';

import BufferList = require('bl');

export class MsgpackSerde extends AbstractSerde {
  constructor() {
    super();
  }
  encode(message: Message): BufferList | undefined {
    switch (message.type) {
      case MessageType.append_request:
        const ar = message as AppendRequest;
        const logs = ar.logs.map(val => {
          return [val.idx.toString(), val.term.toString(), val.type, val.data];
        });
        return msgpack.encode([
          ar.id.toString(),
          ar.from,
          ar.to,
          ar.type,
          ar.term.toString(),
          ar.prev_idx.toString(),
          ar.prev_term.toString(),
          ar.commit_idx.toString(),
          ar.last_idx.toString(),
          logs,
        ]);
      case MessageType.append_response:
        const as = message as AppendResponse;
        return msgpack.encode([
          as.id.toString(),
          as.from,
          as.to,
          as.type,
          as.term.toString(),
          as.success,
        ]);
      case MessageType.vote_request:
        const vr = message as VoteRequest;
        return msgpack.encode([
          vr.id.toString(),
          vr.from,
          vr.to,
          vr.type,
          vr.term.toString(),
          vr.last_idx.toString(),
          vr.last_term.toString(),
          vr.is_test,
        ]);
      case MessageType.vote_response:
        const vs = message as VoteResponse;
        return msgpack.encode([
          vs.id.toString(),
          vs.from,
          vs.to,
          vs.type,
          vs.term.toString(),
          vs.vote_granted,
        ]);
      default:
        return undefined;
    }
  }

  decode(data: Buffer): Message | undefined {
    type serializedAppendRequest = [
      string,
      string,
      string,
      MessageType.append_request,
      string,
      string,
      string,
      string,
      string,
      Array<[string, string, LogType, Buffer | null]>
    ];
    type serializedAppendResponse = [
      string,
      string,
      string,
      MessageType.append_response,
      string,
      boolean
    ];
    type serializedVoteRequest = [
      string,
      string,
      string,
      MessageType.vote_request,
      string,
      string,
      string,
      boolean
    ];
    type serializedVoteResponse = [
      string,
      string,
      string,
      MessageType.vote_response,
      string,
      boolean
    ];
    const arr:
      | serializedAppendRequest
      | serializedAppendResponse
      | serializedVoteRequest
      | serializedVoteResponse = msgpack.decode(data);
    const id = BigInt(arr[0]);
    const from = arr[1];
    const to = arr[2];
    const type: MessageType = arr[3];
    const term = BigInt(arr[4]);

    switch (arr[3]) {
      case MessageType.append_request:
        const logs = arr[9].map(
          (val): Log => new Log(val[2], BigInt(val[0]), BigInt(val[1]), val[3])
        );
        return new AppendRequest(
          id,
          from,
          to,
          term,
          BigInt(arr[5]),
          BigInt(arr[6]),
          BigInt(arr[7]),
          BigInt(arr[8]),
          logs
        );
      case MessageType.append_response:
        return new AppendResponse(id, from, to, term, arr[5]);
      case MessageType.vote_request:
        return new VoteRequest(
          id,
          from,
          to,
          term,
          BigInt(arr[5]),
          BigInt(arr[6]),
          arr[7]
        );
      case MessageType.vote_response:
        return new VoteResponse(id, from, to, term, arr[5]);
      default:
        return undefined;
    }
  }
}
