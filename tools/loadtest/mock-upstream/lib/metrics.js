import { monitorEventLoopDelay } from 'node:perf_hooks';
import { quantile } from './util.js';

export class Metrics {
  constructor(config) {
    this.startedAt = new Date().toISOString();
    this.config = config;
    this.counters = {
      requests_total: 0, completed: 0, cancelled: 0, errors: 0,
      active: 0, active_peak: 0, waiting: 0, waiting_peak: 0,
      rejected: 0, limit_hits: 0, connections_active: 0,
      connections_peak: 0, connections_rejected: 0, connection_limit_hits: 0,
      bytes_sent: 0, chunks_sent: 0, observation_dropped: 0,
    };
    this.status = {};
    this.queueMs = [];
    this.timingErrorMs = [];
    this.events = [];
    this.eventLimit = config.eventBufferSize;
    this.lag = monitorEventLoopDelay({ resolution: 10 });
    this.lag.enable();
  }

  inc(name, amount = 1) { this.counters[name] = (this.counters[name] || 0) + amount; }
  gauge(name, delta) {
    this.counters[name] += delta;
    const peak = `${name}_peak`;
    if (peak in this.counters) this.counters[peak] = Math.max(this.counters[peak], this.counters[name]);
  }
  observe(list, value) {
    list.push(value);
    if (list.length > 10000) list.splice(0, list.length - 10000);
  }
  event(value) {
    if (!this.config.detailedEvents) return;
    if (this.events.length >= this.eventLimit) { this.inc('observation_dropped'); return; }
    this.events.push(value);
  }
  reset() {
    for (const key of Object.keys(this.counters)) {
      if (!key.endsWith('_active') && key !== 'active' && key !== 'waiting') this.counters[key] = 0;
    }
    this.queueMs.length = 0; this.timingErrorMs.length = 0; this.events.length = 0;
    this.status = {};
    this.lag.reset();
  }
  snapshot(extra = {}) {
    const lagScale = 1e6;
    return {
      schema_version: 1,
      generated_at: new Date().toISOString(),
      started_at: this.startedAt,
      process: { pid: process.pid, model: 'single-process-single-event-loop', node: process.version,
        uptime_seconds: process.uptime(), rss_bytes: process.memoryUsage().rss },
      limits: { max_concurrency: this.config.maxConcurrency, max_waiting: this.config.maxWaiting,
        max_connections: this.config.maxConnections, wait_timeout_ms: this.config.waitTimeoutMs },
      counters: { ...this.counters }, status: { ...this.status },
      latency_ms: {
        handler_queue_p50: quantile(this.queueMs, .5), handler_queue_p99: quantile(this.queueMs, .99),
        timing_error_p50: quantile(this.timingErrorMs, .5), timing_error_p99: quantile(this.timingErrorMs, .99),
      },
      event_loop_lag_ms: { p50: this.lag.percentile(50) / lagScale, p99: this.lag.percentile(99) / lagScale,
        max: this.lag.max / lagScale },
      events: [...this.events], ...extra,
    };
  }
}
