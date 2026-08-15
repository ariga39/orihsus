//! HTTP serving with header hardening and optional TLS.
//!
//! This module owns the listener/accept loop so the HTTP/1 header-read timeout
//! and max header size (and the HTTP/2 header-list cap) are actually enforced,
//! which `axum::serve` does not expose. The per-connection builder is pure and
//! cheap to construct, so the accept loop builds one per connection.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;
use tokio::net::TcpListener;
use tower::{Service, ServiceExt};

use crate::config;

/// Errors from [`load_server_config`]. Contains only paths and OS/rustls
/// errors, never certificate/key material.
#[derive(Debug)]
pub enum ServerTlsError {
    ReadCert {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
    ReadKey {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
    ParseCert {
        source: std::io::Error,
    },
    MissingKey,
    Rustls {
        source: rustls::Error,
    },
}

impl std::fmt::Display for ServerTlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerTlsError::ReadCert { path, source } => {
                write!(
                    f,
                    "cannot read TLS certificate {}: {}",
                    path.display(),
                    source
                )
            }
            ServerTlsError::ReadKey { path, source } => {
                write!(f, "cannot read TLS key {}: {}", path.display(), source)
            }
            ServerTlsError::ParseCert { source } => {
                write!(f, "cannot parse TLS certificate: {source}")
            }
            ServerTlsError::MissingKey => write!(f, "no private key found in the TLS key file"),
            ServerTlsError::Rustls { source } => write!(f, "cannot configure TLS: {source}"),
        }
    }
}

impl std::error::Error for ServerTlsError {}

/// Load a rustls server config from PEM certificate/key files. ALPN advertises
/// `h2` and `http/1.1`; the config enforces the protocol (never plaintext).
pub fn load_server_config(
    cert_path: &Path,
    key_path: &Path,
) -> Result<rustls::ServerConfig, ServerTlsError> {
    let cert_pem = std::fs::read(cert_path).map_err(|source| ServerTlsError::ReadCert {
        path: cert_path.to_path_buf(),
        source,
    })?;
    let key_pem = std::fs::read(key_path).map_err(|source| ServerTlsError::ReadKey {
        path: key_path.to_path_buf(),
        source,
    })?;
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        rustls_pemfile::certs(&mut cert_pem.as_slice())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| ServerTlsError::ParseCert { source })?;
    let key = rustls_pemfile::private_key(&mut key_pem.as_slice())
        .map_err(|source| ServerTlsError::ReadKey {
            path: key_path.to_path_buf(),
            source,
        })?
        .ok_or(ServerTlsError::MissingKey)?;
    // Multiple rustls features (ring + aws-lc-rs) are enabled transitively by
    // the dependency tree, so rustls cannot auto-pick; choose ring explicitly.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|source| ServerTlsError::Rustls { source })?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(config)
}

/// Server hardening knobs derived from the config.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerSettings {
    pub read_header_timeout: Duration,
    pub max_header_bytes: usize,
}

impl ServerSettings {
    pub fn from_config(server: &config::Server) -> ServerSettings {
        ServerSettings {
            read_header_timeout: server.read_header_timeout,
            max_header_bytes: server.max_header_bytes,
        }
    }
}

/// The HTTP/1 connection limits handed to hyper. Kept as a separate, pure type
/// so the seam is directly assertable in tests (default and configured values).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http1Limits {
    pub header_read_timeout: Duration,
    pub max_header_bytes: usize,
}

impl Http1Limits {
    pub fn from_settings(settings: &ServerSettings) -> Http1Limits {
        Http1Limits {
            header_read_timeout: settings.read_header_timeout,
            max_header_bytes: settings.max_header_bytes,
        }
    }
}

fn build_builder(limits: Http1Limits) -> Builder<TokioExecutor> {
    let mut builder = Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(hyper_util::rt::TokioTimer::new())
        .max_buf_size(limits.max_header_bytes)
        .header_read_timeout(limits.header_read_timeout)
        // Serve exactly one request per HTTP/1 connection and close afterwards:
        // a keep-alive idle h1 connection would otherwise hold a connection
        // permit forever. Trade-off: no HTTP/1 keep-alive reuse, so every
        // request opens a new connection (documented in docs/DEPLOYMENT.md).
        // HTTP/2 multiplexing/keep-alive is unaffected.
        .keep_alive(false);
    builder
        .http2()
        .max_header_list_size(
            u32::try_from(limits.max_header_bytes)
                .expect("config validation caps max_header_bytes at u32::MAX"),
        )
        // Bounded HTTP/2 keep-alive: an established h2 connection that stops
        // answering pings is closed (and its permit released) instead of living
        // forever. The same bound is reused by the first-request watchdog.
        .keep_alive_interval(Some(limits.header_read_timeout))
        .keep_alive_timeout(limits.header_read_timeout)
        .timer(hyper_util::rt::TokioTimer::new());
    builder
}

/// Static, desensitized stderr warning emitted (once per serve invocation) when
/// a connection task panics or is cancelled. The listener keeps accepting; the
/// warning is the only side effect.
const CONNECTION_TASK_WARNING: &str =
    "orihsus: a connection task panicked or was cancelled; continuing to serve";

/// Outcome of reaping one finished connection task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReapOutcome {
    /// The connection task finished normally; its (unit) result is discarded.
    Completed,
    /// The connection task panicked or was cancelled.
    Failed,
}

/// Reap one finished connection task from the set, returning `None` once the
/// set is empty. `Ok` results are discarded; a panic or cancellation is merely
/// surfaced as the [`CONNECTION_TASK_WARNING`] (latching `warned` so a burst of
/// failing tasks does not flood stderr) — never propagated, so the listener is
/// never terminated by a crashing connection task.
async fn reap_next_connection(
    connections: &mut tokio::task::JoinSet<()>,
    warned: &std::sync::atomic::AtomicBool,
) -> Option<ReapOutcome> {
    match connections.join_next().await? {
        Ok(_) => Some(ReapOutcome::Completed),
        Err(_) => {
            if !warned.swap(true, std::sync::atomic::Ordering::Relaxed) {
                eprintln!("{CONNECTION_TASK_WARNING}");
            }
            Some(ReapOutcome::Failed)
        }
    }
}

/// Fixed short backoff applied to a `listener.accept()` error that is not a
/// benign transient signal-interrupt (e.g. `EMFILE`/`ENFILE` fd exhaustion or
/// any other error), so a sustained accept failure never busy-spins the loop.
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(100);

/// Static, desensitized stderr warning emitted once per serve invocation on the
/// first `accept()` failure. Never contains the OS error, a path or a value.
const ACCEPT_FAILURE_WARNING: &str =
    "orihsus: accept failed; retrying (further accept failures are not logged)";

/// One-shot stderr warning guard for accept failures: prints on the first call,
/// stays silent afterwards so an accept-error storm does not flood stderr.
fn warn_accept_once(warned: &std::sync::atomic::AtomicBool) -> bool {
    if warned.swap(true, std::sync::atomic::Ordering::Relaxed) {
        return false;
    }
    eprintln!("{ACCEPT_FAILURE_WARNING}");
    true
}

/// How long to wait before retrying an `accept()` that failed with `err`.
/// Benign transient errors (`Interrupted`, `ConnectionAborted`) retry
/// immediately; everything else — including the raw Unix `EMFILE`/`ENFILE`
/// resource-exhaustion codes (23/24) that std maps to no `ErrorKind` — gets the
/// fixed short [`ACCEPT_RETRY_DELAY`]. Pure seam: tests lock the policy without
/// any listener framework.
fn accept_retry_delay(err: &std::io::Error) -> Duration {
    match err.kind() {
        std::io::ErrorKind::Interrupted | std::io::ErrorKind::ConnectionAborted => Duration::ZERO,
        _ => ACCEPT_RETRY_DELAY,
    }
}

/// Serve `router` on `listener`. When `tls` is `Some` every accepted stream is
/// upgraded to TLS before serving; otherwise it is served as plaintext (used by
/// tests). `connection_cap` bounds the number of simultaneous TCP connections
/// (TLS handshake, header read and serving alike): an accepted socket beyond
/// the cap is closed immediately and no task is spawned, so slowloris clients
/// that never complete TLS or headers cannot grow the task/FD set. Once
/// `shutdown` resolves, stops accepting new connections, then drains in-flight
/// connections up to `drain_timeout` before returning.
pub async fn serve(
    listener: TcpListener,
    router: Router,
    tls: Option<tokio_rustls::TlsAcceptor>,
    limits: Http1Limits,
    connection_cap: Arc<tokio::sync::Semaphore>,
    drain_timeout: Duration,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let make_service = router.into_make_service();
    let mut shutdown = std::pin::pin!(shutdown);
    let mut connections = tokio::task::JoinSet::new();
    let connection_warned = std::sync::atomic::AtomicBool::new(false);
    let accept_warned = std::sync::atomic::AtomicBool::new(false);

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            // Continuously reap finished connection tasks so the JoinSet never
            // accumulates completed tasks as connections come and go. Guarded on
            // non-empty: join_next on an empty set returns None immediately and
            // would otherwise busy-loop and starve accept.
            _ = reap_next_connection(&mut connections, &connection_warned), if !connections.is_empty() => {}
            res = listener.accept() => {
                match res {
                    Ok((tcp, _)) => {
                        let Ok(permit) = connection_cap.clone().try_acquire_owned() else {
                            // At the connection cap: close the socket immediately and
                            // spawn nothing. A slowloris client that never completes
                            // TLS or headers therefore cannot push the task/FD set
                            // past max_connections.
                            drop(tcp);
                            continue;
                        };
                        let mut make_service = make_service.clone();
                        let tls = tls.clone();
                        connections.spawn(async move {
                            // Hold the connection permit for the whole lifecycle. It is
                            // released (RAII) when the TLS handshake times out, the
                            // connection ends, or the drain aborts this task.
                            let _permit = permit;
                            let stream = match tls {
                                Some(acceptor) => {
                                    // Bound the TLS handshake with the header-read
                                    // timeout: a client that never sends a ClientHello
                                    // is dropped instead of holding the task forever.
                                    match tokio::time::timeout(
                                        limits.header_read_timeout,
                                        acceptor.accept(tcp),
                                    )
                                    .await
                                    {
                                        Ok(Ok(s)) => ServerStream::Tls(Box::new(s)),
                                        _ => return,
                                    }
                                }
                                None => ServerStream::Plain(tcp),
                            };
                            let io = TokioIo::new(stream);
                            let tower_service = match make_service.call(()).await {
                                Ok(s) => s,
                                Err(infallible) => match infallible {},
                            };
                            // First-request watchdog seam: the service's first request
                            // fires this Notify. Until then a client that went silent
                            // right after the transport handshake is dropped once
                            // `read_header_timeout` elapses — the auto builder's
                            // protocol-detection read has no timeout of its own, so a
                            // silent HTTP/1 connect or a silent ALPN-h2 connection
                            // (no client preface, or preface but no request) would
                            // otherwise hold a connection permit forever.
                            let first_request = Arc::new(tokio::sync::Notify::new());
                            let first_request_notify = first_request.clone();
                            let tower_service = tower_service.map_request(
                                move |req: axum::http::Request<Incoming>| {
                                    first_request_notify.notify_one();
                                    req.map(axum::body::Body::new)
                                },
                            );
                            let hyper_service = TowerToHyperService::new(tower_service);
                            let builder = build_builder(limits);
                            let serve = builder.serve_connection(io, hyper_service);
                            let serve = std::pin::pin!(serve);
                            let first_request = first_request.notified();
                            tokio::pin!(first_request);
                            // Before the first request: close the connection if the
                            // bound elapses. After the first request the watchdog goes
                            // silent and `serve` runs unbounded — idle is then covered
                            // by the HTTP/2 keep-alive and active streams (SSE) are
                            // never killed by an overall timeout.
                            let watchdog = async {
                                if tokio::time::timeout(
                                    limits.header_read_timeout,
                                    first_request,
                                )
                                .await
                                .is_ok()
                                {
                                    std::future::pending::<()>().await;
                                }
                            };
                            tokio::pin!(watchdog);
                            tokio::select! {
                                _ = serve => {}
                                _ = watchdog => {}
                            }
                        });
                    }
                    Err(err) => {
                        // A transient accept error (e.g. fd exhaustion) must not
                        // terminate the listener: warn once, back off briefly and
                        // keep accepting. Interrupted-style errors retry
                        // immediately. Shutdown is delayed by at most the 100ms
                        // backoff, then the shutdown branch wins the next round.
                        warn_accept_once(&accept_warned);
                        let delay = accept_retry_delay(&err);
                        if !delay.is_zero() {
                            tokio::time::sleep(delay).await;
                        }
                        continue;
                    }
                }
            }
        }
    }

    let deadline = tokio::time::Instant::now() + drain_timeout;
    while !connections.is_empty() {
        match tokio::time::timeout_at(deadline, connections.join_next()).await {
            Ok(_) => {
                // a connection finished (or the set became empty): reap and continue
            }
            Err(_) => {
                // drain deadline hit with connections still in flight: abort the
                // rest and reap to empty so this function returns promptly
                // (no busy loop when the deadline is already past).
                connections.abort_all();
                while connections.join_next().await.is_some() {}
                break;
            }
        }
    }
    Ok(())
}

enum ServerStream {
    Tls(Box<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>),
    Plain(tokio::net::TcpStream),
}

impl tokio::io::AsyncRead for ServerStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            ServerStream::Tls(s) => std::pin::Pin::new(s.as_mut()).poll_read(cx, buf),
            ServerStream::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for ServerStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match &mut *self {
            ServerStream::Tls(s) => std::pin::Pin::new(s.as_mut()).poll_write(cx, buf),
            ServerStream::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            ServerStream::Tls(s) => std::pin::Pin::new(s.as_mut()).poll_flush(cx),
            ServerStream::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            ServerStream::Tls(s) => std::pin::Pin::new(s.as_mut()).poll_shutdown(cx),
            ServerStream::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::Semaphore;

    #[test]
    fn settings_map_to_http1_limits() {
        let settings = ServerSettings {
            read_header_timeout: Duration::from_secs(5),
            max_header_bytes: 32 * 1024,
        };
        let limits = Http1Limits::from_settings(&settings);
        assert_eq!(limits.header_read_timeout, Duration::from_secs(5));
        assert_eq!(limits.max_header_bytes, 32 * 1024);

        let custom = ServerSettings {
            read_header_timeout: Duration::from_secs(2),
            max_header_bytes: 8192,
        };
        assert_eq!(
            Http1Limits::from_settings(&custom),
            Http1Limits {
                header_read_timeout: Duration::from_secs(2),
                max_header_bytes: 8192
            }
        );
    }

    #[test]
    fn accept_retry_delay_classifies_transient_errors_immediately() {
        // Signal-interrupt style errors are transient: retry immediately, never
        // an artificial backoff.
        assert_eq!(
            accept_retry_delay(&std::io::Error::from(std::io::ErrorKind::Interrupted)),
            Duration::ZERO
        );
        assert_eq!(
            accept_retry_delay(&std::io::Error::from(std::io::ErrorKind::ConnectionAborted)),
            Duration::ZERO
        );
    }

    #[test]
    fn accept_retry_delay_backs_off_on_resource_and_other_errors() {
        // EMFILE (24) / ENFILE (23) are raw Unix resource-exhaustion codes not
        // mapped to an ErrorKind; they and every other error get the fixed
        // short backoff so a sustained accept failure never busy-spins.
        assert_eq!(
            accept_retry_delay(&std::io::Error::from_raw_os_error(24)),
            ACCEPT_RETRY_DELAY
        );
        assert_eq!(
            accept_retry_delay(&std::io::Error::from_raw_os_error(23)),
            ACCEPT_RETRY_DELAY
        );
        assert_eq!(
            accept_retry_delay(&std::io::Error::from(std::io::ErrorKind::Other)),
            ACCEPT_RETRY_DELAY
        );
        assert_eq!(
            accept_retry_delay(&std::io::Error::from_raw_os_error(111)),
            ACCEPT_RETRY_DELAY
        );
    }

    #[test]
    fn warn_accept_once_prints_only_on_the_first_failure() {
        let warned = std::sync::atomic::AtomicBool::new(false);
        assert!(
            warn_accept_once(&warned),
            "the first accept failure must warn"
        );
        assert!(
            !warn_accept_once(&warned),
            "later accept failures stay silent"
        );
        assert!(!warn_accept_once(&warned));
    }

    async fn start_plain(
        limits: Http1Limits,
    ) -> (
        std::net::SocketAddr,
        tokio::task::JoinHandle<Result<(), std::io::Error>>,
    ) {
        let router = Router::new().route("/", get(|| async { "ok" }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(serve(
            listener,
            router,
            None,
            limits,
            Arc::new(Semaphore::new(1024)),
            Duration::from_secs(5),
            std::future::pending(),
        ));
        (addr, handle)
    }

    async fn raw_get(addr: std::net::SocketAddr, extra_headers: &str) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let req = format!("GET / HTTP/1.1\r\nHost: {addr}\r\n{extra_headers}\r\n");
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = vec![0u8; 4096];
        let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
            .await
            .unwrap()
            .unwrap_or(0);
        String::from_utf8_lossy(&buf[..n]).to_string()
    }

    /// Poll `cap` until it reports `expected` available permits, failing after
    /// a real-time deadline. Connection teardown (EOF propagation through the
    /// kernel and hyper) is I/O-driven, so `yield_now` alone may outrun it.
    async fn wait_for_permits(cap: &Arc<Semaphore>, expected: usize) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while cap.available_permits() != expected {
            assert!(
                tokio::time::Instant::now() < deadline,
                "connection permits never reached {expected} (stayed {})",
                cap.available_permits()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn plaintext_server_serves_and_enforces_max_header_bytes() {
        let (addr, handle) = start_plain(Http1Limits {
            header_read_timeout: Duration::from_secs(5),
            max_header_bytes: 8192,
        })
        .await;

        let resp = raw_get(addr, "").await;
        assert!(
            resp.starts_with("HTTP/1.1 200"),
            "small request must be served: {resp:?}"
        );

        let oversized = format!("X-Big: {}\r\n", "a".repeat(16384));
        let resp = raw_get(addr, &oversized).await;
        assert!(
            resp.contains("431") || resp.contains("Request Header Fields Too Large"),
            "oversized header must be rejected with 431: {resp:?}"
        );

        handle.abort();
    }

    #[tokio::test]
    async fn header_read_timeout_closes_stalled_connections() {
        let (addr, handle) = start_plain(Http1Limits {
            header_read_timeout: Duration::from_millis(200),
            max_header_bytes: 8192,
        })
        .await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: x\r\n")
            .await
            .unwrap();
        // stall; hyper must close the connection after the header read timeout
        let mut buf = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut buf))
            .await
            .unwrap()
            .unwrap_or(1);
        assert_eq!(
            read, 0,
            "stalled header must time out and close the connection"
        );

        handle.abort();
    }

    fn test_tls_config() -> (rustls::ServerConfig, tempfile::TempDir) {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let dir = tempfile::TempDir::new().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::write(&cert_path, certified.cert.pem()).unwrap();
        std::fs::write(&key_path, certified.key_pair.serialize_pem()).unwrap();
        let config = load_server_config(&cert_path, &key_path).unwrap();
        (config, dir)
    }

    #[tokio::test]
    async fn tls_server_serves_over_https() {
        let (config, _dir) = test_tls_config();
        let router = Router::new().route("/healthz", get(|| async { "ok" }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config));
        let handle = tokio::spawn(serve(
            listener,
            router,
            Some(acceptor),
            Http1Limits {
                header_read_timeout: Duration::from_secs(5),
                max_header_bytes: 32 * 1024,
            },
            Arc::new(Semaphore::new(1024)),
            Duration::from_secs(5),
            std::future::pending(),
        ));

        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();
        let url = format!("https://127.0.0.1:{}/healthz", addr.port());
        let resp = client.get(url).send().await.unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.text().await.unwrap(), "ok");

        handle.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn serve_returns_after_drain_timeout_even_with_stuck_connections() {
        use axum::routing::get;

        let started = Arc::new(tokio::sync::Notify::new());
        let s2 = started.clone();
        let router = Router::new().route(
            "/",
            get(move || async move {
                s2.notify_one();
                std::future::pending::<()>().await
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let limits = Http1Limits {
            header_read_timeout: Duration::from_secs(5),
            max_header_bytes: 8192,
        };
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(serve(
            listener,
            router,
            None,
            limits,
            Arc::new(Semaphore::new(1024)),
            Duration::from_millis(100),
            async move {
                let _ = shutdown_rx.await;
            },
        ));

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .unwrap();
        shutdown_tx.send(()).unwrap();

        let joined = tokio::time::timeout(Duration::from_secs(5), handle).await;
        assert!(
            joined.is_ok(),
            "serve must return after drain_timeout even with a stuck connection (no busy loop)"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tls_handshake_times_out_and_closes_the_connection() {
        let (config, _dir) = test_tls_config();
        let router = Router::new().route("/healthz", get(|| async { "ok" }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let limits = Http1Limits {
            header_read_timeout: Duration::from_millis(300),
            max_header_bytes: 8192,
        };
        let handle = tokio::spawn(serve(
            listener,
            router,
            Some(acceptor),
            limits,
            Arc::new(Semaphore::new(1024)),
            Duration::from_secs(5),
            std::future::pending(),
        ));

        // Connect without sending a ClientHello: the server must time out the
        // handshake (real signal, no long sleeps) and close the connection.
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 1];
        let result = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut buf)).await;
        assert!(
            result.is_ok(),
            "a TLS client that never sends ClientHello must be dropped after the handshake timeout"
        );
        let n = result.unwrap().unwrap_or(0);
        assert_eq!(
            n, 0,
            "the connection must be closed after the handshake timeout"
        );

        handle.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plaintext_connection_cap_closes_third_and_recovers_after_a_close() {
        let router = Router::new().route("/", get(|| async { "ok" }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let cap = Arc::new(Semaphore::new(2));
        let handle = tokio::spawn(serve(
            listener,
            router,
            None,
            Http1Limits {
                header_read_timeout: Duration::from_secs(10),
                max_header_bytes: 8192,
            },
            Arc::clone(&cap),
            Duration::from_secs(5),
            std::future::pending(),
        ));

        // Two slowloris connections that hold their header read open (partial
        // headers, no terminating CRLF) occupy both connection permits.
        let mut a = TcpStream::connect(addr).await.unwrap();
        a.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n").await.unwrap();
        let mut b = TcpStream::connect(addr).await.unwrap();
        b.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n").await.unwrap();
        wait_for_permits(&cap, 0).await;

        // A third connection must be closed promptly at accept, with no task
        // spawned for it (a spawned task would hold the socket open instead).
        let mut c = TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(1), c.read(&mut buf)).await;
        assert!(
            read.is_ok() && read.unwrap().unwrap_or(0) == 0,
            "the third connection must be closed promptly, not held by a spawned task"
        );
        assert_eq!(
            cap.available_permits(),
            0,
            "no permit may be freed for the rejected third connection"
        );

        // Closing one slowloris connection frees its permit and the next real
        // request is served.
        drop(a);
        wait_for_permits(&cap, 1).await;
        let resp = raw_get(addr, "").await;
        assert!(
            resp.starts_with("HTTP/1.1 200"),
            "a new connection must be served once a permit frees: {resp:?}"
        );

        drop(b);
        handle.abort();
        let _ = handle.await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tls_connection_cap_closes_third_slow_handshake_promptly() {
        let (config, _dir) = test_tls_config();
        let router = Router::new().route("/healthz", get(|| async { "ok" }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let cap = Arc::new(Semaphore::new(2));
        let handle = tokio::spawn(serve(
            listener,
            router,
            Some(acceptor),
            Http1Limits {
                header_read_timeout: Duration::from_secs(10),
                max_header_bytes: 8192,
            },
            Arc::clone(&cap),
            Duration::from_secs(5),
            std::future::pending(),
        ));

        // Two TLS clients that never send a ClientHello occupy both permits
        // while their handshakes wait out the (long) handshake timeout.
        let a = TcpStream::connect(addr).await.unwrap();
        let b = TcpStream::connect(addr).await.unwrap();
        wait_for_permits(&cap, 0).await;

        // A third slow client must be closed promptly at accept.
        let mut c = TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(1), c.read(&mut buf)).await;
        assert!(
            read.is_ok() && read.unwrap().unwrap_or(0) == 0,
            "the third TLS connection must be closed promptly, not held by a spawned handshake task"
        );
        assert_eq!(
            cap.available_permits(),
            0,
            "no permit may be freed for the rejected third TLS connection"
        );

        drop(a);
        drop(b);
        handle.abort();
        let _ = handle.await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn connection_permits_are_released_on_drain() {
        let router = Router::new().route("/", get(|| async { "ok" }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let cap = Arc::new(Semaphore::new(2));
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(serve(
            listener,
            router,
            None,
            Http1Limits {
                header_read_timeout: Duration::from_secs(10),
                max_header_bytes: 8192,
            },
            Arc::clone(&cap),
            Duration::from_millis(100),
            async move {
                let _ = shutdown_rx.await;
            },
        ));

        // A stuck slowloris connection holds a permit while shutdown fires; the
        // drain abort must release it (RAII) so serve returns with no leaks.
        let mut a = TcpStream::connect(addr).await.unwrap();
        a.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n").await.unwrap();
        wait_for_permits(&cap, 1).await;

        shutdown_tx.send(()).unwrap();
        let joined = tokio::time::timeout(Duration::from_secs(5), handle).await;
        assert!(
            joined.is_ok(),
            "serve must return after the drain timeout even with a stuck connection"
        );
        assert_eq!(
            cap.available_permits(),
            2,
            "draining must release every connection permit (no leak)"
        );
    }

    #[test]
    fn tls_load_fails_when_files_are_missing() {
        let dir = tempfile::TempDir::new().unwrap();
        let err = load_server_config(&dir.path().join("nope.pem"), &dir.path().join("nope.key"))
            .unwrap_err();
        assert!(matches!(err, ServerTlsError::ReadCert { .. }));
    }

    #[tokio::test]
    async fn reap_next_connection_brings_a_finished_join_set_back_to_empty() {
        // The production reap seam: spawn many short tasks and prove the same
        // helper the accept loop uses drains the set back to empty, so completed
        // connection tasks never accumulate as connections grow.
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..32 {
            set.spawn(async { tokio::task::yield_now().await });
        }
        let warned = std::sync::atomic::AtomicBool::new(false);
        let mut reaped = 0u32;
        while let Some(outcome) = reap_next_connection(&mut set, &warned).await {
            assert_eq!(outcome, ReapOutcome::Completed);
            reaped += 1;
        }
        assert_eq!(reaped, 32, "every finished task must be reaped");
        assert!(set.is_empty(), "the join set must return to empty");
        assert!(
            !warned.load(std::sync::atomic::Ordering::SeqCst),
            "normal completions must not trip the warning guard"
        );
    }

    #[tokio::test]
    async fn reap_next_connection_surfaces_a_panicking_task_without_propagating() {
        // A panicking connection task must be reaped as Failed (the listener is
        // never terminated by it) and the static warning guard latches once.
        let mut set = tokio::task::JoinSet::new();
        set.spawn(async { panic!("connection task boom") });
        let warned = std::sync::atomic::AtomicBool::new(false);
        let outcome = reap_next_connection(&mut set, &warned)
            .await
            .expect("one task to reap");
        assert_eq!(outcome, ReapOutcome::Failed);
        assert!(set.is_empty());
        assert!(
            warned.load(std::sync::atomic::Ordering::SeqCst),
            "the warning guard must latch after the first panicking task"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn serial_connections_far_beyond_the_cap_keep_serving_and_shutdown_returns() {
        // Serially serve far more completed connections than max_connections:
        // each must be served 200 (completed connection tasks are reaped, never
        // accumulated in the JoinSet) and shutdown must still return promptly.
        let router = Router::new().route("/", get(|| async { "ok" }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(serve(
            listener,
            router,
            None,
            Http1Limits {
                header_read_timeout: Duration::from_secs(5),
                max_header_bytes: 8192,
            },
            Arc::new(Semaphore::new(2)),
            Duration::from_secs(5),
            async move {
                let _ = shutdown_rx.await;
            },
        ));

        for i in 0..50 {
            let resp = raw_get(addr, "").await;
            assert!(
                resp.starts_with("HTTP/1.1 200"),
                "serial connection {i} must be served: {resp:?}"
            );
        }

        shutdown_tx.send(()).unwrap();
        let joined = tokio::time::timeout(Duration::from_secs(5), handle).await;
        assert!(
            joined.is_ok(),
            "serve must return promptly after the serial burst and shutdown"
        );
    }

    /// Minimal no-verify server-cert verifier for the raw TLS test client.
    #[derive(Debug)]
    struct NoVerify;

    impl rustls::client::danger::ServerCertVerifier for NoVerify {
        fn verify_server_cert(
            &self,
            _end_entity: &rustls::pki_types::CertificateDer<'_>,
            _intermediates: &[rustls::pki_types::CertificateDer<'_>],
            _server_name: &rustls::pki_types::ServerName<'_>,
            _ocsp_response: &[u8],
            _now: rustls::pki_types::UnixTime,
        ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &rustls::pki_types::CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &rustls::pki_types::CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            rustls::crypto::ring::default_provider()
                .signature_verification_algorithms
                .supported_schemes()
        }
    }

    /// Raw TLS client that completes the handshake with ALPN `h2` and sends no
    /// further bytes (no HTTP/2 client preface). Returns the established
    /// stream, whose peer (the server) must then close it within the bound.
    async fn silent_h2_client(
        addr: std::net::SocketAddr,
    ) -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut client_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify))
            .with_no_client_auth();
        client_config.alpn_protocols = vec![b"h2".to_vec()];
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
        let stream = TcpStream::connect(addr).await.unwrap();
        let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let tls = connector.connect(server_name, stream).await.unwrap();
        assert_eq!(
            tls.get_ref().1.alpn_protocol(),
            Some(b"h2".as_slice()),
            "the raw client must negotiate h2 via ALPN"
        );
        tls
    }

    /// The established stream must be closed by the server within a bounded
    /// window (EOF), never held open holding a connection permit. The server may
    /// push a SETTINGS frame before closing, so drain until EOF.
    async fn assert_h2_silent_closed(
        client: &mut tokio_rustls::client::TlsStream<tokio::net::TcpStream>,
    ) {
        let mut buf = [0u8; 64];
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match client.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
        })
        .await
        .expect("a silent h2 client must be closed within the bound, not hold the permit forever");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn h1_keep_alive_connection_closes_after_one_request_and_releases_the_permit() {
        // An HTTP/1 client that sends one keep-alive request and then idles
        // must not hold the connection permit forever: the server serves the
        // request, closes the connection (EOF), and releases the permit. A
        // second request on the same socket must not be served, and a fresh
        // connection must work normally.
        let router = Router::new()
            .route("/healthz", get(|| async { "ok" }))
            .route("/", get(|| async { "ok" }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let cap = Arc::new(Semaphore::new(1));
        let handle = tokio::spawn(serve(
            listener,
            router,
            None,
            Http1Limits {
                header_read_timeout: Duration::from_secs(5),
                max_header_bytes: 8192,
            },
            Arc::clone(&cap),
            Duration::from_secs(5),
            std::future::pending(),
        ));

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: x\r\nConnection: keep-alive\r\n\r\n")
            .await
            .unwrap();

        // Read the full response to EOF within a bounded window: a keep-alive
        // connection that stays open would block here forever.
        let resp = tokio::time::timeout(Duration::from_secs(3), async {
            let mut resp = Vec::new();
            let mut buf = [0u8; 1024];
            loop {
                let n = stream.read(&mut buf).await.unwrap_or(0);
                resp.extend_from_slice(&buf[..n]);
                if n == 0 {
                    break;
                }
            }
            resp
        })
        .await
        .expect("the h1 connection must close (EOF) after serving one request");
        assert!(
            String::from_utf8_lossy(&resp).starts_with("HTTP/1.1 200"),
            "the keep-alive request must be served"
        );
        assert!(
            String::from_utf8_lossy(&resp)
                .to_ascii_lowercase()
                .contains("connection: close"),
            "the response must declare connection: close"
        );
        wait_for_permits(&cap, 1).await;

        // A second request on the same (now closed) socket must not be served:
        // the read yields EOF/reset promptly instead of another response.
        let _ = stream
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n")
            .await;
        let mut buf = [0u8; 1];
        let second = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut buf)).await;
        assert!(
            matches!(second, Ok(Ok(0)) | Ok(Err(_))),
            "a second request on the same h1 socket must not be served"
        );

        // A fresh connection is served normally.
        let resp = raw_get(addr, "").await;
        assert!(
            resp.starts_with("HTTP/1.1 200"),
            "a fresh connection must be served: {resp:?}"
        );

        handle.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn silent_h2_connection_is_closed_after_tls_and_releases_the_permit() {
        // Clients that complete TLS with ALPN h2 and then stop talking — either
        // sending no HTTP/2 client preface at all, or sending the preface and
        // SETTINGS but never a request — must be closed within a bounded window
        // and release the single connection permit; a legal connection must then
        // be served. The auto builder's protocol-detection read has no timeout
        // of its own, so without a first-request watchdog these connections
        // would hold the permit forever.
        let (config, _dir) = test_tls_config();
        let router = Router::new().route("/healthz", get(|| async { "ok" }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let cap = Arc::new(Semaphore::new(1));
        let handle = tokio::spawn(serve(
            listener,
            router,
            Some(acceptor),
            Http1Limits {
                header_read_timeout: Duration::from_millis(300),
                max_header_bytes: 8192,
            },
            Arc::clone(&cap),
            Duration::from_secs(5),
            std::future::pending(),
        ));

        // Scenario 1: TLS + ALPN h2, no preface at all.
        let mut client = silent_h2_client(addr).await;
        assert_h2_silent_closed(&mut client).await;
        wait_for_permits(&cap, 1).await;

        // Scenario 2: TLS + ALPN h2, client preface + SETTINGS, then silent.
        let mut client = silent_h2_client(addr).await;
        let preface = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
        let settings = [0u8, 0, 0, 4, 0, 0, 0, 0, 0];
        client.write_all(preface).await.unwrap();
        client.write_all(&settings).await.unwrap();
        client.flush().await.unwrap();
        assert_h2_silent_closed(&mut client).await;
        wait_for_permits(&cap, 1).await;

        // A legal connection must now be served.
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();
        let url = format!("https://127.0.0.1:{}/healthz", addr.port());
        let resp = client.get(url).send().await.unwrap();
        assert_eq!(resp.status(), 200);

        handle.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn active_h2_stream_with_traffic_survives_the_keepalive_window() {
        use futures_util::StreamExt;

        // An SSE-like route that sends one chunk then stays open silently: the
        // first-request watchdog and the h2 keep-alive must never kill a stream
        // that is actively served.
        let (config, _dir) = test_tls_config();
        let router = Router::new().route(
            "/stream",
            get(|| async {
                axum::body::Body::from_stream(
                    futures_util::stream::once(async {
                        Ok::<axum::body::Bytes, std::convert::Infallible>(
                            axum::body::Bytes::from_static(b"data: hello\n\n"),
                        )
                    })
                    .chain(futures_util::stream::pending::<
                        Result<axum::body::Bytes, std::convert::Infallible>,
                    >()),
                )
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let handle = tokio::spawn(serve(
            listener,
            router,
            Some(acceptor),
            Http1Limits {
                header_read_timeout: Duration::from_millis(300),
                max_header_bytes: 8192,
            },
            Arc::new(Semaphore::new(4)),
            Duration::from_secs(5),
            std::future::pending(),
        ));

        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();
        let url = format!("https://127.0.0.1:{}/stream", addr.port());
        let resp = client.get(url).send().await.unwrap();
        assert_eq!(resp.status(), 200);
        let mut body = resp.bytes_stream();
        let first = tokio::time::timeout(Duration::from_secs(3), body.next())
            .await
            .expect("the first SSE chunk must arrive")
            .expect("a data chunk")
            .unwrap();
        assert_eq!(&first[..], b"data: hello\n\n");

        // Hold the stream open well past the keep-alive window (2x header bound):
        // the connection must stay alive — the next read blocks, it does not EOF.
        tokio::time::sleep(Duration::from_millis(700)).await;
        let next = tokio::time::timeout(Duration::from_millis(200), body.next()).await;
        assert!(
            next.is_err(),
            "an active h2 stream must not be killed by the keep-alive or the first-request watchdog"
        );
        handle.abort();
    }
}
