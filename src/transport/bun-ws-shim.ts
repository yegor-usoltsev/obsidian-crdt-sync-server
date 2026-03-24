/**
 * Compatibility shim between Bun's ServerWebSocket and the Node.js ws
 * event-emitter API that Hocuspocus expects.
 */

import type { ServerWebSocket } from "bun";

export class BunWsAdapter {
  private listeners = new Map<string, Set<(...args: unknown[]) => unknown>>();
  private ws: ServerWebSocket<unknown>;

  /** Maps to WebSocket.OPEN / WebSocket.CLOSED */
  get readyState(): number {
    return this.ws.readyState;
  }

  static readonly OPEN = 1;
  static readonly CLOSED = 3;

  constructor(ws: ServerWebSocket<unknown>) {
    this.ws = ws;
  }

  on(event: string, listener: (...args: unknown[]) => unknown): this {
    let set = this.listeners.get(event);
    if (!set) {
      set = new Set();
      this.listeners.set(event, set);
    }
    set.add(listener);
    return this;
  }

  once(event: string, listener: (...args: unknown[]) => unknown): this {
    const wrapper = (...args: unknown[]) => {
      this.removeListener(event, wrapper);
      listener(...args);
    };
    return this.on(event, wrapper);
  }

  removeListener(
    event: string,
    listener: (...args: unknown[]) => unknown,
  ): this {
    this.listeners.get(event)?.delete(listener);
    return this;
  }

  removeAllListeners(event?: string): this {
    if (event) {
      this.listeners.delete(event);
    } else {
      this.listeners.clear();
    }
    return this;
  }

  send(data: string | ArrayBuffer | Uint8Array): void {
    this.ws.send(data as string);
  }

  close(): void {
    this.ws.close();
  }

  /** Called by the Bun websocket `message` handler to forward data. */
  _emitMessage(data: string | ArrayBuffer): void {
    const set = this.listeners.get("message");
    if (set) {
      for (const fn of set) fn(data);
    }
  }

  /** Called by the Bun websocket `close` handler. */
  _emitClose(code?: number, reason?: string): void {
    const set = this.listeners.get("close");
    if (set) {
      for (const fn of set) fn(code ?? 1000, reason ?? "");
    }
  }
}
