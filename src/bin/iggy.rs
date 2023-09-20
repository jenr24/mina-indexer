use std::{path::PathBuf, time::Duration};

use clap::Parser;
use mina_indexer::google_cloud::{MinaNetwork, worker::gsutil_download_blocks};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct IggyCli {
    #[arg(short, long)]
    output_dir: PathBuf,
    #[arg(short, long, default_value_t = 0)]
    start_from: u64,
    #[arg(short, long, default_value_t = 1)]
    batch_size: u64,
    #[arg(short, long, default_value_t = 1000)]
    poll_freq_ms: u64,
    #[clap(value_enum)]
    #[arg(short, long)]
    mina_network: MinaNetwork,
    #[arg(short, long, default_value = "mina_network_block_data")]
    google_cloud_bucket: String
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let cli_args = IggyCli::parse();
    let poll_freq = Duration::from_millis(cli_args.poll_freq_ms);
    let mut temp_blocks_dir = std::env::temp_dir();
    temp_blocks_dir.push("temp_blocks_dir");
    if tokio::fs::metadata(&temp_blocks_dir).await.is_ok() {
        tokio::fs::remove_dir_all(&temp_blocks_dir).await?;
    }

    let mut start_from = cli_args.start_from;
    loop {
        tokio::time::sleep(poll_freq).await;
        gsutil_download_blocks(
            &cli_args.output_dir, 
            start_from, cli_args.batch_size, 
            &cli_args.google_cloud_bucket, cli_args.mina_network
        ).await?;
        start_from = start_from + cli_args.batch_size;
    }
}