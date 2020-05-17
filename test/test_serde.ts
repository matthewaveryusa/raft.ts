import { AbstractSerde } from '../src/interfaces';
import { Message } from '../src/messages';

import BufferList = require('bl');

export class TestSerde extends AbstractSerde {
  constructor() {
    super();
  }
  encode(message: Message): BufferList | undefined {
    return undefined;
  }

  decode(data: Buffer): Message | undefined {
    return undefined;
  }
}
