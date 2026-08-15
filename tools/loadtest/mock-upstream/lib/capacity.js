import { monotonicMs } from './util.js';

export class Capacity {
  constructor(config, metrics) { this.config = config; this.metrics = metrics; this.queue = []; }

  async acquire(signal) {
    const arrived = monotonicMs();
    if (this.metrics.counters.active < this.config.maxConcurrency) {
      this.metrics.gauge('active', 1);
      this.metrics.observe(this.metrics.queueMs, monotonicMs() - arrived);
      return this.#lease();
    }
    this.metrics.inc('limit_hits');
    if (this.queue.length >= this.config.maxWaiting) {
      this.metrics.inc('rejected');
      throw Object.assign(new Error('mock capacity rejected'), { statusCode: 503, capacity: true });
    }
    this.metrics.gauge('waiting', 1);
    return new Promise((resolve, reject) => {
      const entry = { arrived, resolve, reject, signal };
      this.queue.push(entry);
      const finish = (error) => {
        const index = this.queue.indexOf(entry);
        if (index !== -1) { this.queue.splice(index, 1); this.metrics.gauge('waiting', -1); }
        reject(error);
      };
      entry.timer = setTimeout(() => {
        this.metrics.inc('rejected');
        finish(Object.assign(new Error('mock capacity wait timeout'), { statusCode: 503, capacity: true }));
      }, this.config.waitTimeoutMs);
      entry.abort = () => finish(signal.reason ?? new Error('aborted'));
      signal?.addEventListener('abort', entry.abort, { once: true });
    });
  }

  #lease() {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.metrics.gauge('active', -1);
      while (this.queue.length) {
        const next = this.queue.shift();
        clearTimeout(next.timer);
        next.signal?.removeEventListener('abort', next.abort);
        this.metrics.gauge('waiting', -1);
        if (next.signal?.aborted) continue;
        this.metrics.gauge('active', 1);
        this.metrics.observe(this.metrics.queueMs, monotonicMs() - next.arrived);
        next.resolve(this.#lease());
        break;
      }
    };
  }
}
