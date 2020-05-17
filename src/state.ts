export enum Role {
  leader = 'L',
  candidate = 'C',
  follower = 'F',
}

export class State {
  static make(str: string): State | null {
    const arr: [
      string,
      string,
      string,
      string[],
      string[],
      string
    ] = JSON.parse(str);
    if (arr.length < 4) {
      return null;
    }
    const s = new State();
    s.current_term = BigInt(arr[0]);
    s.commit_idx = BigInt(arr[1]);
    s.voted_for = arr[2];
    s.peer_addresses = arr[3];
    s.peer_addresses_old = arr[4];
    s.config_idx = BigInt(arr[5]);
    return s;
  }
  current_term: bigint;
  commit_idx: bigint;
  voted_for: null | string;
  peer_addresses: string[];
  peer_addresses_old: string[];
  config_idx: bigint;

  constructor() {
    this.current_term = BigInt(0);
    this.commit_idx = BigInt(0);
    this.voted_for = null;
    this.peer_addresses = [];
    this.peer_addresses_old = [];
    this.config_idx = BigInt(0);
  }

  toString(): string {
    this.peer_addresses.sort();
    return JSON.stringify([
      this.current_term.toString(),
      this.commit_idx.toString(),
      this.voted_for,
      this.peer_addresses,
      this.peer_addresses_old,
      this.config_idx.toString(),
    ]);
  }
}
