declare module 'node-lmdb' {

export class Cursor {
    constructor(txn: Txn, db: Dbi);
    close(): any;
    del(): any;
    getCurrentBinary(): any;
    getCurrentBinaryUnsafe(): any;
    getCurrentBoolean(): any;
    getCurrentNumber(): any;
    getCurrentString(): any;
    getCurrentStringUnsafe(): any;
    goToDup(): any;
    goToDupRange(): any;
    goToFirst(): any;
    goToFirstDup(): any;
    goToKey(): any;
    goToLast(): any;
    goToLastDup(): any;
    goToNext(): any;
    goToNextDup(): any;
    goToPrev(): any;
    goToPrevDup(): any;
    goToRange(key: string|number|Buffer): string|null;
}

export class Env {
    constructor();
    beginTxn(): Txn;
    close(): any;
    info(): any;
    open(options: any): any;
    openDbi(options: any): Dbi;
    resize(): any;
    stat(): any;
    sync(): any;
}

export class Dbi {
}

export class Txn {
    commit(): void;
    getString(dbi: Dbi, key: string|number|Buffer): string|null;
    getBinary(dbi: Dbi, key: string|number|Buffer): Buffer|null;
    putBinary(dbi: Dbi, key: string|number|Buffer, value: Buffer): void;
    putString(dbi: Dbi, key: string|number|Buffer, value: string): void;
}
}
