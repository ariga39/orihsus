use loadgen::cli::{Command, HELP};

#[tokio::main]
async fn main() {
    if let Err(e) = real_main().await {
        eprintln!("loadgen: {e}");
        std::process::exit(2)
    }
}
async fn real_main() -> Result<(), String> {
    match loadgen::cli::parse()? {
        Command::Help => println!("{HELP}"),
        Command::Run(args) => {
            let jsonl = args.jsonl;
            let (summary, records) = loadgen::runner::run(args).await?;
            if jsonl {
                for r in records {
                    eprintln!("{}", serde_json::to_string(&r).map_err(|e| e.to_string())?)
                }
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&summary).map_err(|e| e.to_string())?
            )
        }
        Command::Slowloris(args) => {
            let summary = loadgen::slowloris::run(args).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&summary).map_err(|e| e.to_string())?
            )
        }
    }
    Ok(())
}
