use std::{env, path::PathBuf, time::Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    Http1,
    Http2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResponseMode {
    Json,
    Sse,
}

#[derive(Clone, Debug)]
pub struct RunArgs {
    pub url: String,
    pub protocol: Protocol,
    pub concurrency: usize,
    pub requests: Option<u64>,
    pub duration: Option<Duration>,
    pub method: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub mode: ResponseMode,
    pub ca: Option<PathBuf>,
    pub insecure: bool,
    pub read_bytes_per_sec: Option<u64>,
    pub write_bytes_per_sec: Option<u64>,
    pub stop_read_after: Option<Duration>,
    pub hold_after_stop: Duration,
    pub timeout: Duration,
    pub jsonl: bool,
    pub no_keepalive: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlowStage {
    Tcp,
    Tls,
    Header,
    Body,
    H2Preface,
}

#[derive(Clone, Debug)]
pub struct SlowArgs {
    pub target: String,
    pub connections: usize,
    pub stage: SlowStage,
    pub ca: Option<PathBuf>,
    pub insecure: bool,
    pub interval: Duration,
    pub hold: Duration,
    pub header: String,
    pub body_byte: u8,
}

#[derive(Clone, Debug)]
pub enum Command {
    Run(RunArgs),
    Slowloris(SlowArgs),
    Help,
}

fn duration(s: &str) -> Result<Duration, String> {
    if let Some(v) = s.strip_suffix("ms") {
        return v
            .parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|_| format!("invalid duration: {s}"));
    }
    if let Some(v) = s.strip_suffix('s') {
        return v
            .parse::<u64>()
            .map(Duration::from_secs)
            .map_err(|_| format!("invalid duration: {s}"));
    }
    s.parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|_| format!("invalid duration: {s}"))
}

fn value(args: &[String], i: &mut usize, flag: &str) -> Result<String, String> {
    *i += 1;
    args.get(*i)
        .cloned()
        .ok_or_else(|| format!("{flag} requires a value"))
}

pub fn parse() -> Result<Command, String> {
    parse_from(env::args().skip(1).collect())
}

pub fn parse_from(args: Vec<String>) -> Result<Command, String> {
    if args.is_empty() || args[0] == "--help" || args[0] == "-h" {
        return Ok(Command::Help);
    }
    match args[0].as_str() {
        "run" => parse_run(&args[1..]).map(Command::Run),
        "slowloris" => parse_slow(&args[1..]).map(Command::Slowloris),
        x => Err(format!("unknown subcommand: {x}")),
    }
}

fn parse_run(args: &[String]) -> Result<RunArgs, String> {
    let mut out = RunArgs {
        url: String::new(),
        protocol: Protocol::Http1,
        concurrency: 1,
        requests: None,
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
        hold_after_stop: Duration::from_secs(60),
        timeout: Duration::from_secs(120),
        jsonl: false,
        no_keepalive: false,
    };
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--url" => out.url = value(args, &mut i, "--url")?,
            "--protocol" => {
                out.protocol = match value(args, &mut i, "--protocol")?.as_str() {
                    "http1" => Protocol::Http1,
                    "http2" => Protocol::Http2,
                    x => return Err(format!("invalid protocol: {x}")),
                }
            }
            "-c" | "--concurrency" => {
                out.concurrency = value(args, &mut i, "--concurrency")?
                    .parse()
                    .map_err(|_| "invalid concurrency")?
            }
            "-n" | "--requests" => {
                out.requests = Some(
                    value(args, &mut i, "--requests")?
                        .parse()
                        .map_err(|_| "invalid requests")?,
                )
            }
            "--duration" => out.duration = Some(duration(&value(args, &mut i, "--duration")?)?),
            "--method" => out.method = value(args, &mut i, "--method")?,
            "-H" | "--header" => {
                let h = value(args, &mut i, "--header")?;
                let (k, v) = h.split_once(':').ok_or("header must be NAME: VALUE")?;
                out.headers.push((k.trim().into(), v.trim().into()));
            }
            "--body" => out.body = value(args, &mut i, "--body")?.into_bytes(),
            "--body-file" => {
                out.body =
                    std::fs::read(value(args, &mut i, "--body-file")?).map_err(|e| e.to_string())?
            }
            "--mode" => {
                out.mode = match value(args, &mut i, "--mode")?.as_str() {
                    "json" => ResponseMode::Json,
                    "sse" => ResponseMode::Sse,
                    x => return Err(format!("invalid mode: {x}")),
                }
            }
            "--ca" => out.ca = Some(value(args, &mut i, "--ca")?.into()),
            "-k" | "--insecure" => out.insecure = true,
            "--read-bytes-per-sec" => {
                out.read_bytes_per_sec = Some(
                    value(args, &mut i, "--read-bytes-per-sec")?
                        .parse()
                        .map_err(|_| "invalid read rate")?,
                )
            }
            "--write-bytes-per-sec" => {
                let rate = value(args, &mut i, "--write-bytes-per-sec")?
                    .parse()
                    .map_err(|_| "invalid write rate")?;
                if rate == 0 {
                    return Err("write rate must be positive".into());
                }
                out.write_bytes_per_sec = Some(rate);
            }
            "--stop-read-after" => {
                out.stop_read_after = Some(duration(&value(args, &mut i, "--stop-read-after")?)?)
            }
            "--hold-after-stop" => {
                out.hold_after_stop = duration(&value(args, &mut i, "--hold-after-stop")?)?
            }
            "--timeout" => out.timeout = duration(&value(args, &mut i, "--timeout")?)?,
            "--jsonl" => out.jsonl = true,
            "--no-keepalive" => out.no_keepalive = true,
            x => return Err(format!("unknown run option: {x}")),
        }
        i += 1;
    }
    if out.url.is_empty() {
        return Err("--url is required".into());
    }
    if out.concurrency == 0 {
        return Err("concurrency must be positive".into());
    }
    if out.requests == Some(0) {
        return Err("requests must be positive".into());
    }
    if out.requests.is_none() && out.duration.is_none() {
        return Err("one of --requests or --duration is required".into());
    }
    if out.requests.is_some() && out.duration.is_some() {
        return Err("--requests and --duration are mutually exclusive".into());
    }
    Ok(out)
}

fn parse_slow(args: &[String]) -> Result<SlowArgs, String> {
    let mut out = SlowArgs {
        target: String::new(),
        connections: 1,
        stage: SlowStage::Tcp,
        ca: None,
        insecure: false,
        interval: Duration::from_secs(1),
        hold: Duration::from_secs(10),
        header: "GET / HTTP/1.1\r\nHost: localhost\r\nX-Slow: ".into(),
        body_byte: b'x',
    };
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--target" => out.target = value(args, &mut i, "--target")?,
            "-c" | "--connections" => {
                out.connections = value(args, &mut i, "--connections")?
                    .parse()
                    .map_err(|_| "invalid connections")?
            }
            "--stage" => {
                out.stage = match value(args, &mut i, "--stage")?.as_str() {
                    "tcp" => SlowStage::Tcp,
                    "tls" => SlowStage::Tls,
                    "header" => SlowStage::Header,
                    "body" => SlowStage::Body,
                    "h2-preface" => SlowStage::H2Preface,
                    x => return Err(format!("invalid stage: {x}")),
                }
            }
            "--ca" => out.ca = Some(value(args, &mut i, "--ca")?.into()),
            "-k" | "--insecure" => out.insecure = true,
            "--interval" => out.interval = duration(&value(args, &mut i, "--interval")?)?,
            "--hold" => out.hold = duration(&value(args, &mut i, "--hold")?)?,
            "--header" => out.header = value(args, &mut i, "--header")?,
            "--body-byte" => {
                out.body_byte = value(args, &mut i, "--body-byte")?
                    .as_bytes()
                    .first()
                    .copied()
                    .ok_or("empty body byte")?
            }
            x => return Err(format!("unknown slowloris option: {x}")),
        }
        i += 1;
    }
    if out.target.is_empty() {
        return Err("--target HOST:PORT is required".into());
    }
    if out.connections == 0 {
        return Err("connections must be positive".into());
    }
    Ok(out)
}

pub const HELP: &str = r#"loadgen run --url URL (-n REQUESTS | --duration 30s) [options]
  --protocol http1|http2  -c/--concurrency N  --mode json|sse
  --ca FILE | -k/--insecure  -H 'Name: value'  --body JSON | --body-file FILE
  --write-bytes-per-sec N  --read-bytes-per-sec N
  --stop-read-after 1s  --hold-after-stop 60s
  --timeout 120s  --no-keepalive  --jsonl (records on stderr; summary on stdout)

loadgen slowloris --target HOST:PORT --stage tcp|tls|header|body|h2-preface [options]
  -c/--connections N  --ca FILE | -k  --interval 1s  --hold 10s"#;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_run() {
        let c = parse_from(
            vec![
                "run",
                "--url",
                "https://x",
                "-n",
                "10",
                "-c",
                "2",
                "--mode",
                "sse",
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
        )
        .unwrap();
        let Command::Run(r) = c else { panic!() };
        assert_eq!(r.requests, Some(10));
        assert_eq!(r.mode, ResponseMode::Sse);
        assert_eq!(r.write_bytes_per_sec, None);
    }
}
