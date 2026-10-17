use anyhow::{bail, Context, Result};
use boom_clickhouse_example::{PreparedBatch, Store};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
struct Input {
    #[serde(rename = "_id")]
    object_id: String,
    prv_candidates: Vec<Value>,
    #[serde(default)]
    prv_nondetections: Vec<Value>,
    fp_hists: Vec<Value>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some(command @ ("prepare" | "write")) if args.len() == 3 => {
            let input: Input = serde_json::from_str(&std::fs::read_to_string(&args[1])?)
                .context("expected normalized BOOM ZTF object JSON")?;
            let batch = PreparedBatch::ztf(&input.object_id, args[2].parse()?,
                &input.prv_candidates, &input.prv_nondetections, &input.fp_hists)?;
            if command == "prepare" {
                print!("{}", batch.body);
            } else {
                Store::from_env()?.insert(&batch).await?;
                eprintln!("Inserted {} rows; token {}", batch.rows, batch.token);
            }
        }
        Some("init") if args.len() == 1 => Store::from_env()?.initialize().await?,
        Some("read") if args.len() == 2 => {
            println!("{}", serde_json::to_string_pretty(&Store::from_env()?.history(&args[1]).await?)?);
        }
        Some("nightly") if args.len() == 2 => print!("{}", Store::from_env()?.nightly(&args[1]).await?),
        _ => bail!("usage: boom-clickhouse-example init | prepare FILE REVISION | write FILE REVISION | read OBJECT_ID | nightly OBJECT_ID"),
    }
    Ok(())
}
