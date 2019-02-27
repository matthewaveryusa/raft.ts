
  export enum LogType {
    noop =  'N',
    config = 'C',
    message = 'M',
    snapshot = 'S',
  }

  export class Log {
        public static make_empty(): Log {
          return new Log(
           LogType.noop,
           BigInt(0),
           BigInt(0),
           null)
      }

      public static make_noop(): Log {
        return new Log(
         LogType.noop,
         BigInt(0),
         BigInt(0),
         null)
    }

      constructor(
        public type: LogType,
        public idx: bigint,
        public term: bigint,
        public data: Buffer | null) {}
  }

  export enum MessageType {
    vote_request = 'V',
    vote_response = 'W',
    append_request = 'A',
    append_response = 'B',
    none = 'N',

  }

  export class Message {
    constructor(public type: MessageType,
                public id: bigint,
                public from: string,
                public to: string,
                public term: bigint) {
    }

}

  export class VoteRequest extends Message {

    constructor(id: bigint, from: string, to: string, term: bigint,
                public last_idx: bigint, public last_term: bigint, public is_test: boolean) {
        super(MessageType.vote_request, id, from, to, term)
    }

}

  export class VoteResponse extends Message {
    constructor(id: bigint, from: string, to: string, term: bigint, public vote_granted: boolean) {
        super(MessageType.vote_response, id, from, to, term)
    }
}

  export class AppendRequest extends Message {
    constructor(id: bigint, from: string, to: string, term: bigint,
                public prev_idx: bigint,
                public prev_term: bigint,
                public commit_idx: bigint,
                public last_idx: bigint,
                public logs: Log[]) {
        super(MessageType.append_request, id, from, to, term)
    }
}

  export class AppendResponse extends Message {
    constructor(id: bigint, from: string, to: string, term: bigint, public success: boolean) {
        super(MessageType.append_response, id, from, to, term)
    }
}
