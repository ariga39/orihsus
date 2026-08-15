export class Barriers {
  #items = new Map();

  wait(name, phase, requestId, target, signal) {
    if (!name) return Promise.resolve({ released: false });
    let item = this.#items.get(name);
    if (!item) {
      item = { name, target: target || 0, arrived: 0, released: 0, waiters: [], phases: {} };
      this.#items.set(name, item);
    }
    if (target) item.target = target;
    item.arrived++;
    item.phases[phase] = (item.phases[phase] || 0) + 1;
    return new Promise((resolve, reject) => {
      const waiter = { phase, requestId, resolve, reject, signal };
      item.waiters.push(waiter);
      const abort = () => {
        item.waiters = item.waiters.filter((entry) => entry !== waiter);
        reject(signal.reason ?? new Error('aborted'));
      };
      signal?.addEventListener('abort', abort, { once: true });
      waiter.cleanup = () => signal?.removeEventListener('abort', abort);
      if (item.target > 0 && item.arrived >= item.target) this.release(name, Infinity);
    });
  }

  release(name, count = Infinity, phase) {
    const item = this.#items.get(name);
    if (!item) return 0;
    let released = 0;
    const retained = [];
    for (const waiter of item.waiters) {
      if (released < count && (!phase || waiter.phase === phase)) {
        waiter.cleanup();
        waiter.resolve({ released: true });
        released++;
      } else retained.push(waiter);
    }
    item.waiters = retained;
    item.released += released;
    return released;
  }

  cancel(name) {
    const item = this.#items.get(name);
    if (!item) return 0;
    for (const waiter of item.waiters) {
      waiter.cleanup();
      waiter.reject(new Error('barrier cancelled'));
    }
    const count = item.waiters.length;
    this.#items.delete(name);
    return count;
  }

  reset() {
    for (const name of this.#items.keys()) this.cancel(name);
  }

  snapshot() {
    return Object.fromEntries([...this.#items].map(([name, item]) => [name, {
      target: item.target, arrived: item.arrived, waiting: item.waiters.length,
      released: item.released, phases: item.phases,
    }]));
  }
}
