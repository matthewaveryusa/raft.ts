export enum ops {
    Add,
    Remove,
    Read,
}

export class EventLog {
    public logs: any[]
    constructor() {
        this.logs = []
    }

    public add(name: string, val: any): void {
        this.logs.push([name, val])
    }

}
