import { AbstractTimeoutEngine, variance_func_t } from './interfaces'

export class NodeTimeoutEngine extends AbstractTimeoutEngine {
  private timeouts: Map<string, NodeJS.Timeout>

  constructor(variance_function: variance_func_t) {
    super(variance_function)
    this.timeouts = new Map()
  }

  public clear(name: string): void {
    const timeout = this.timeouts.get(name)
    if (timeout) {
      clearTimeout(timeout)
      this.timeouts.delete(name)
    }
  }

  public set(name: string, timeout_ms: number, callback: () => void): void {
    // wrap the callback so we delete from the map
    const timeout = this.timeouts.get(name)
    if (timeout) {
      clearTimeout(timeout)
    }
    this.timeouts.set(name, setTimeout(() => {
      this.timeouts.delete(name)
      callback()
    }, this.variance_function(timeout_ms)))
  }

  public set_varied(name: string, timeout_ms: number, callback: () => void): void {
    this.set(name, this.variance_function(timeout_ms), callback)
  }
}
