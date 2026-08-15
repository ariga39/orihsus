use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize)]
pub struct RequestRecord {
    pub request_id: String,
    pub status: Option<u16>,
    pub retry_after: Option<String>,
    pub ttfb_ms: Option<f64>,
    pub first_event_ms: Option<f64>,
    pub completion_ms: f64,
    pub response_bytes: u64,
    pub sse_events: u64,
    pub saw_done: bool,
    pub outcome: String,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct Percentiles {
    pub min: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

#[derive(Debug, Serialize)]
pub struct Summary {
    pub schema_version: u8,
    pub elapsed_ms: f64,
    pub requests_started: u64,
    pub requests_finished: u64,
    pub peak_in_flight: u64,
    pub statuses: BTreeMap<String, u64>,
    pub retry_after: BTreeMap<String, u64>,
    pub outcomes: BTreeMap<String, u64>,
    pub errors: BTreeMap<String, u64>,
    pub response_bytes: u64,
    pub sse_events: u64,
    pub ttfb_ms: Option<Percentiles>,
    pub first_event_ms: Option<Percentiles>,
    pub completion_ms: Option<Percentiles>,
}

pub fn summarize(records: &[RequestRecord], elapsed_ms: f64, started: u64, peak: u64) -> Summary {
    let mut statuses = BTreeMap::new();
    let mut retry_after = BTreeMap::new();
    let mut outcomes = BTreeMap::new();
    let mut errors = BTreeMap::new();
    let mut ttfb = vec![];
    let mut first = vec![];
    let mut completion = vec![];
    let mut bytes = 0;
    let mut events = 0;
    for r in records {
        if let Some(v) = r.status {
            *statuses.entry(v.to_string()).or_insert(0) += 1
        }
        if let Some(v) = &r.retry_after {
            *retry_after.entry(v.clone()).or_insert(0) += 1
        }
        *outcomes.entry(r.outcome.clone()).or_insert(0) += 1;
        if let Some(v) = &r.error {
            *errors.entry(v.clone()).or_insert(0) += 1
        }
        if let Some(v) = r.ttfb_ms {
            ttfb.push(v)
        }
        if let Some(v) = r.first_event_ms {
            first.push(v)
        }
        completion.push(r.completion_ms);
        bytes += r.response_bytes;
        events += r.sse_events;
    }
    Summary {
        schema_version: 1,
        elapsed_ms,
        requests_started: started,
        requests_finished: records.len() as u64,
        peak_in_flight: peak,
        statuses,
        retry_after,
        outcomes,
        errors,
        response_bytes: bytes,
        sse_events: events,
        ttfb_ms: pct(ttfb),
        first_event_ms: pct(first),
        completion_ms: pct(completion),
    }
}
fn pct(mut v: Vec<f64>) -> Option<Percentiles> {
    if v.is_empty() {
        return None;
    }
    v.sort_by(f64::total_cmp);
    let at = |p: f64| v[((v.len() as f64 * p).ceil() as usize).saturating_sub(1)];
    Some(Percentiles {
        min: v[0],
        p50: at(0.50),
        p95: at(0.95),
        p99: at(0.99),
        max: *v.last().unwrap(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn percentile_is_nearest_rank() {
        let p = pct((1..=100).map(|x| x as f64).collect()).unwrap();
        assert_eq!(p.p99, 99.0);
        assert_eq!(p.p50, 50.0);
    }
}
