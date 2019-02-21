export enum Role {
    leader = 'L',
    candidate = 'C',
    follower = 'F',
}

export class State {

    public static make(str: string): State| null {
        const arr: any[] = JSON.parse(str)
        if (arr.length < 4) { return null }
        const s = new State()
        s.current_term = BigInt(arr[0])
        s.commit_idx = BigInt(arr[1])
        s.voted_for = arr[2]
        s.peer_addresses = arr[3]
        s.peer_addresses_old = arr[4]
        s.config_idx = BigInt(arr[5])
        return s
    }
    public current_term: bigint
    public commit_idx: bigint
    public voted_for: null|string
    public peer_addresses: string[]
    public peer_addresses_old: string[]
    public config_idx: bigint

    constructor() {
        this.current_term = BigInt(0)
        this.commit_idx = BigInt(0)
        this.voted_for = null
        this.peer_addresses = []
        this.peer_addresses_old = []
        this.config_idx = BigInt(0)
    }

    public toString(): string {
        this.peer_addresses.sort()
        return JSON.stringify([this.current_term.toString(),
            this.commit_idx.toString(),
            this.voted_for,
            this.peer_addresses,
            this.peer_addresses_old,
            this.config_idx.toString()])
    }
}
