// Tiny logger contract used across the library.
//
// We deliberately keep this interface compatible with the pino / winston /
// console shape: each level takes a message and optional structured context.
// Consumers can plug in any logger that has `debug` / `info` / `warn` /
// `error` methods. A no-op default is provided so unconfigured servers do
// not spam stdout.

// tslint:disable-next-line: no-any
export type LogContext = Record<string, any>;

export interface Logger {
  debug?(msg: string, ctx?: LogContext): void;
  info?(msg: string, ctx?: LogContext): void;
  warn?(msg: string, ctx?: LogContext): void;
  error?(msg: string, ctx?: LogContext): void;
}

export const NOOP_LOGGER: Logger = {};

export const CONSOLE_LOGGER: Logger = {
  debug: (msg, ctx) => console.debug(msg, ctx ?? ''),
  info: (msg, ctx) => console.info(msg, ctx ?? ''),
  warn: (msg, ctx) => console.warn(msg, ctx ?? ''),
  error: (msg, ctx) => console.error(msg, ctx ?? ''),
};
