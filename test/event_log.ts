export enum ops {
    Add,
    Remove,
    Read,
}

export interface ILog {
    name: string
    args: any
    ret: any
}

export class EventLog {
    public logs: ILog[]
    constructor() {
        this.logs = []
    }

    public add(name: string, args: object, ret: any): void {
        this.logs.push({ name, args, ret})
    }

}
