export enum ops {
  Add,
  Remove,
  Read,
}

export interface Log {
  name: string;
  // tslint:disable-next-line: no-any
  args: any;
  // tslint:disable-next-line: no-any
  ret: any;
}

export class EventLog {
  logs: Log[];
  constructor() {
    this.logs = [];
  }
  // tslint:disable-next-line: no-any
  add(name: string, args: any, ret: any): void {
    this.logs.push({ name, args, ret });
  }
}
