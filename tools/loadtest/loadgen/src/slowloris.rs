use crate::cli::{SlowArgs, SlowStage};
use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, ServerName, UnixTime},
    ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};
use serde::Serialize;
use std::{io::BufReader, sync::Arc, time::Instant};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    task::JoinSet,
};
use tokio_rustls::TlsConnector;

#[derive(Debug, Serialize)]
pub struct SlowRecord {
    pub connection: u64,
    pub stage: String,
    pub elapsed_ms: f64,
    pub outcome: String,
    pub error: Option<String>,
}
#[derive(Debug, Serialize)]
pub struct SlowSummary {
    pub schema_version: u8,
    pub target: String,
    pub requested: u64,
    pub connected: u64,
    pub completed: u64,
    pub errors: u64,
    pub records: Vec<SlowRecord>,
}

pub async fn run(args: SlowArgs) -> Result<SlowSummary, String> {
    let (host, _) = split_target(&args.target)?;
    let tls = if args.stage == SlowStage::Tcp {
        None
    } else {
        Some(tls_config(&args).await?)
    };
    let args = Arc::new(args);
    let mut set = JoinSet::new();
    for id in 0..args.connections {
        let args = args.clone();
        let host = host.clone();
        let tls = tls.clone();
        set.spawn(async move { one(id as u64, &args, &host, tls).await });
    }
    let mut records = Vec::new();
    while let Some(r) = set.join_next().await {
        records.push(r.map_err(|e| e.to_string())?)
    }
    records.sort_by_key(|r| r.connection);
    let connected = records
        .iter()
        .filter(|r| r.outcome != "connect_error")
        .count() as u64;
    let completed = records.iter().filter(|r| r.error.is_none()).count() as u64;
    let errors = records.len() as u64 - completed;
    Ok(SlowSummary {
        schema_version: 1,
        target: args.target.clone(),
        requested: args.connections as u64,
        connected,
        completed,
        errors,
        records,
    })
}

async fn one(id: u64, args: &SlowArgs, host: &str, tls: Option<Arc<ClientConfig>>) -> SlowRecord {
    let start = Instant::now();
    let result = async {
        let tcp = TcpStream::connect(&args.target)
            .await
            .map_err(|e| format!("connect:{e}"))?;
        tcp.set_nodelay(true).map_err(|e| format!("nodelay:{e}"))?;
        if args.stage == SlowStage::Tcp {
            tokio::time::sleep(args.hold).await;
            return Ok(());
        }
        let server_name =
            ServerName::try_from(host.to_owned()).map_err(|e| format!("server_name:{e}"))?;
        let mut stream = TlsConnector::from(tls.unwrap())
            .connect(server_name, tcp)
            .await
            .map_err(|e| format!("tls:{e}"))?;
        match args.stage {
            SlowStage::Tls => tokio::time::sleep(args.hold).await,
            SlowStage::Header => {
                drip(&mut stream, args.header.as_bytes(), args.interval).await?;
                tokio::time::sleep(args.hold).await
            }
            SlowStage::Body => {
                let head = format!(
                    "POST / HTTP/1.1\r\nHost: {host}\r\nContent-Length: 1000000000\r\n\r\n"
                );
                stream
                    .write_all(head.as_bytes())
                    .await
                    .map_err(|e| format!("write_header:{e}"))?;
                let until = Instant::now() + args.hold;
                while Instant::now() < until {
                    stream
                        .write_all(&[args.body_byte])
                        .await
                        .map_err(|e| format!("write_body:{e}"))?;
                    tokio::time::sleep(args.interval).await
                }
            }
            SlowStage::H2Preface => {
                stream
                    .write_all(
                        b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\x00\x00\x00\x04\x00\x00\x00\x00\x00",
                    )
                    .await
                    .map_err(|e| format!("write_h2:{e}"))?;
                tokio::time::sleep(args.hold).await
            }
            SlowStage::Tcp => unreachable!(),
        }
        Ok::<(), String>(())
    }
    .await;
    let (outcome, error) = match result {
        Ok(()) => ("completed".into(), None),
        Err(e) => {
            let o = if e.starts_with("connect:") {
                "connect_error"
            } else {
                "io_error"
            };
            (o.into(), Some(e))
        }
    };
    SlowRecord {
        connection: id,
        stage: format!("{:?}", args.stage).to_lowercase(),
        elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
        outcome,
        error,
    }
}
async fn drip<W: AsyncWrite + Unpin>(
    w: &mut W,
    bytes: &[u8],
    interval: std::time::Duration,
) -> Result<(), String> {
    for b in bytes {
        w.write_all(&[*b]).await.map_err(|e| format!("drip:{e}"))?;
        tokio::time::sleep(interval).await
    }
    Ok(())
}
fn split_target(target: &str) -> Result<(String, u16), String> {
    let u = url::Url::parse(&format!("https://{target}"))
        .map_err(|e| format!("invalid target: {e}"))?;
    Ok((
        u.host_str().ok_or("target has no host")?.to_owned(),
        u.port().ok_or("target has no port")?,
    ))
}

async fn tls_config(args: &SlowArgs) -> Result<Arc<ClientConfig>, String> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(path) = &args.ca {
        let pem = tokio::fs::read(path)
            .await
            .map_err(|e| format!("read CA: {e}"))?;
        let mut reader = BufReader::new(pem.as_slice());
        for cert in rustls_pemfile::certs(&mut reader) {
            roots
                .add(cert.map_err(|e| format!("parse CA: {e}"))?)
                .map_err(|e| format!("add CA: {e}"))?
        }
    }
    let mut config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    if args.insecure {
        config
            .dangerous()
            .set_certificate_verifier(Arc::new(NoVerifier));
    }
    config.alpn_protocols = match args.stage {
        SlowStage::H2Preface => vec![b"h2".to_vec()],
        _ => vec![b"http/1.1".to_vec()],
    };
    Ok(Arc::new(config))
}

#[derive(Debug)]
struct NoVerifier;
impl ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}
