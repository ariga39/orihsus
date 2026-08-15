use crate::{
    cli::{Protocol, ResponseMode, RunArgs},
    sse::Decoder,
    stats::{summarize, RequestRecord, Summary},
};
use reqwest::{Certificate, Client, Method};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{Barrier, Mutex};

pub async fn run(args: RunArgs) -> Result<(Summary, Vec<RequestRecord>), String> {
    let client = build_client(&args).await?;
    let workers = args
        .concurrency
        .min(args.requests.unwrap_or(args.concurrency as u64) as usize);
    let barrier = Arc::new(Barrier::new(workers.max(1)));
    let next = Arc::new(AtomicU64::new(0));
    let active = Arc::new(AtomicU64::new(0));
    let peak = Arc::new(AtomicU64::new(0));
    let records = Arc::new(Mutex::new(Vec::new()));
    let started = Instant::now();
    let deadline = args.duration.map(|d| started + d);
    let args = Arc::new(args);
    let mut joins = Vec::new();
    for _ in 0..workers {
        let (client, barrier, next, active, peak, records, args) = (
            client.clone(),
            barrier.clone(),
            next.clone(),
            active.clone(),
            peak.clone(),
            records.clone(),
            args.clone(),
        );
        joins.push(tokio::spawn(async move {
            barrier.wait().await;
            loop {
                if deadline.is_some_and(|d| Instant::now() >= d) {
                    break;
                }
                let id = next.fetch_add(1, Ordering::SeqCst);
                if args.requests.is_some_and(|n| id >= n) {
                    break;
                }
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                let rec = one(&client, &args, id).await;
                active.fetch_sub(1, Ordering::SeqCst);
                records.lock().await.push(rec);
            }
        }));
    }
    for j in joins {
        j.await.map_err(|e| e.to_string())?;
    }
    let elapsed = started.elapsed().as_secs_f64() * 1000.0;
    let records = Arc::try_unwrap(records)
        .map_err(|_| "internal records reference")?
        .into_inner();
    let started_count = records.len() as u64;
    let summary = summarize(
        &records,
        elapsed,
        started_count,
        peak.load(Ordering::SeqCst),
    );
    Ok((summary, records))
}

async fn build_client(args: &RunArgs) -> Result<Client, String> {
    let mut b = Client::builder()
        .danger_accept_invalid_certs(args.insecure)
        .timeout(args.timeout)
        .pool_max_idle_per_host(args.concurrency);
    if args.no_keepalive {
        b = b.pool_max_idle_per_host(0);
    }
    b = match args.protocol {
        Protocol::Http1 => b.http1_only(),
        Protocol::Http2 => b.http2_prior_knowledge(),
    };
    if let Some(path) = &args.ca {
        let pem = tokio::fs::read(path)
            .await
            .map_err(|e| format!("read CA: {e}"))?;
        b = b.add_root_certificate(
            Certificate::from_pem(&pem).map_err(|e| format!("parse CA: {e}"))?,
        );
    }
    b.build().map_err(|e| format!("build HTTP client: {e}"))
}

async fn one(client: &Client, args: &RunArgs, id: u64) -> RequestRecord {
    let begin = Instant::now();
    let request_id = format!("loadgen-{id}");
    let mut rec = RequestRecord {
        request_id: request_id.clone(),
        status: None,
        retry_after: None,
        ttfb_ms: None,
        first_event_ms: None,
        completion_ms: 0.0,
        response_bytes: 0,
        sse_events: 0,
        saw_done: false,
        outcome: "error".into(),
        error: None,
    };
    let method = match Method::from_bytes(args.method.as_bytes()) {
        Ok(v) => v,
        Err(e) => {
            rec.error = Some(format!("invalid_method:{e}"));
            return finish(rec, begin);
        }
    };
    let mut req = client
        .request(method, &args.url)
        .header("x-request-id", &request_id);
    req = if let Some(rate) = args.write_bytes_per_sec {
        req.body(paced_request_body(args.body.clone(), rate))
    } else {
        req.body(args.body.clone())
    };
    for (k, v) in &args.headers {
        req = req.header(k, v)
    }
    let mut response = match req.send().await {
        Ok(v) => v,
        Err(e) => {
            rec.error = Some(classify_reqwest(&e));
            return finish(rec, begin);
        }
    };
    rec.ttfb_ms = Some(begin.elapsed().as_secs_f64() * 1000.0);
    rec.status = Some(response.status().as_u16());
    rec.retry_after = response
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let mut decoder = Decoder::default();
    let mut json = Vec::new();
    let mut read_bytes = 0u64;
    let read_started = Instant::now();
    loop {
        let chunk = if let Some(stop_after) = args.stop_read_after {
            let remaining = stop_after.saturating_sub(read_started.elapsed());
            if remaining.is_zero() {
                tokio::time::sleep(args.hold_after_stop).await;
                rec.outcome = "stopped_reading".into();
                return finish(rec, begin);
            }
            tokio::select! {
                result = response.chunk() => result,
                _ = tokio::time::sleep(remaining) => {
                    tokio::time::sleep(args.hold_after_stop).await;
                    rec.outcome = "stopped_reading".into();
                    return finish(rec, begin);
                }
            }
        } else {
            response.chunk().await
        };
        match chunk {
            Ok(Some(chunk)) => {
                rec.response_bytes += chunk.len() as u64;
                read_bytes += chunk.len() as u64;
                match args.mode {
                    ResponseMode::Json => json.extend_from_slice(&chunk),
                    ResponseMode::Sse => match decoder.push(&chunk) {
                        Ok(events) => {
                            for e in events {
                                if rec.first_event_ms.is_none() {
                                    rec.first_event_ms =
                                        Some(begin.elapsed().as_secs_f64() * 1000.0)
                                }
                                rec.sse_events += 1;
                                if e.data.trim() == "[DONE]" {
                                    rec.saw_done = true
                                }
                            }
                        }
                        Err(e) => {
                            rec.error = Some(format!("sse_parse:{e}"));
                            return finish(rec, begin);
                        }
                    },
                }
                if let Some(rate) = args.read_bytes_per_sec {
                    if rate > 0 {
                        let due = Duration::from_secs_f64(read_bytes as f64 / rate as f64);
                        if due > read_started.elapsed() {
                            tokio::time::sleep(due - read_started.elapsed()).await
                        }
                    }
                }
            }
            Ok(None) => break,
            Err(e) => {
                rec.error = Some(classify_reqwest(&e));
                return finish(rec, begin);
            }
        }
    }
    if args.mode == ResponseMode::Sse {
        match decoder.finish() {
            Ok(events) => {
                for e in events {
                    rec.sse_events += 1;
                    if e.data.trim() == "[DONE]" {
                        rec.saw_done = true
                    }
                }
            }
            Err(e) => rec.error = Some(format!("sse_parse:{e}")),
        }
    } else if let Err(e) = serde_json::from_slice::<serde_json::Value>(&json) {
        rec.error = Some(format!("json_parse:{e}"));
    }
    if rec.error.is_none() {
        rec.outcome = "completed".into()
    }
    finish(rec, begin)
}

fn paced_request_body(body: Vec<u8>, bytes_per_sec: u64) -> reqwest::Body {
    const CHUNK: usize = 64 * 1024;
    let started = Instant::now();
    let stream = futures_util::stream::unfold((body, 0usize), move |(body, offset)| async move {
        if offset >= body.len() {
            return None;
        }
        let end = (offset + CHUNK).min(body.len());
        let due = Duration::from_secs_f64(end as f64 / bytes_per_sec as f64);
        if due > started.elapsed() {
            tokio::time::sleep(due - started.elapsed()).await;
        }
        let chunk = body[offset..end].to_vec();
        Some((Ok::<_, std::io::Error>(chunk), (body, end)))
    });
    reqwest::Body::wrap_stream(stream)
}
fn finish(mut r: RequestRecord, start: Instant) -> RequestRecord {
    r.completion_ms = start.elapsed().as_secs_f64() * 1000.0;
    r
}
fn classify_reqwest(e: &reqwest::Error) -> String {
    if e.is_timeout() {
        "timeout".into()
    } else if e.is_connect() {
        "connect".into()
    } else if e.is_body() {
        "body".into()
    } else {
        format!("request:{e}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::State, response::Response, routing::post, Router};
    use std::sync::atomic::AtomicUsize;

    #[derive(Clone)]
    struct Counts {
        active: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
    }

    async fn json(State(c): State<Counts>) -> &'static str {
        let now = c.active.fetch_add(1, Ordering::SeqCst) + 1;
        c.peak.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(30)).await;
        c.active.fetch_sub(1, Ordering::SeqCst);
        r#"{"ok":true}"#
    }

    async fn sse() -> Response {
        Response::builder()
            .header("content-type", "text/event-stream")
            .header("retry-after", "1")
            .body(axum::body::Body::from(
                "data: {\"n\":1}\n\ndata: [DONE]\n\n",
            ))
            .unwrap()
    }

    fn args(url: String) -> RunArgs {
        RunArgs {
            url,
            protocol: Protocol::Http1,
            concurrency: 4,
            requests: Some(12),
            duration: None,
            method: "POST".into(),
            headers: vec![],
            body: b"{}".to_vec(),
            mode: ResponseMode::Json,
            ca: None,
            insecure: false,
            read_bytes_per_sec: None,
            write_bytes_per_sec: None,
            stop_read_after: None,
            hold_after_stop: Duration::ZERO,
            timeout: Duration::from_secs(2),
            jsonl: false,
            no_keepalive: false,
        }
    }

    async fn serve(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn maintains_requested_in_flight_count() {
        let counts = Counts {
            active: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
        };
        let url = serve(
            Router::new()
                .route("/", post(json))
                .with_state(counts.clone()),
        )
        .await;
        let (summary, records) = run(args(url)).await.unwrap();
        assert_eq!(records.len(), 12);
        assert_eq!(summary.peak_in_flight, 4);
        assert_eq!(counts.peak.load(Ordering::SeqCst), 4);
        assert_eq!(summary.statuses.get("200"), Some(&12));
    }

    #[tokio::test]
    async fn parses_sse_done_and_retry_after() {
        let url = serve(Router::new().route("/", post(sse))).await;
        let mut input = args(url);
        input.concurrency = 1;
        input.requests = Some(1);
        input.mode = ResponseMode::Sse;
        let (summary, records) = run(input).await.unwrap();
        assert_eq!(records[0].sse_events, 2);
        assert!(records[0].saw_done);
        assert!(records[0].first_event_ms.is_some());
        assert_eq!(summary.retry_after.get("1"), Some(&1));
    }
}
