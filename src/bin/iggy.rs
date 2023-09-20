use std::{path::PathBuf, time::Duration};

use clap::Parser;
use mina_indexer::google_cloud::{GoogleCloudBlockReceiver, MinaNetwork};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct IggyCli {
    /// Directory to output files to, uses stdout if none provided
    #[arg(short, long, default_value = None)]
    output_dir: Option<PathBuf>,
    #[arg(short, long, default_value_t = 0)]
    start_from: u64,
    #[arg(short, long, default_value_t = 1)]
    batch_size: u64,
    #[arg(short, long, default_value_t = 1000)]
    poll_freq_ms: u64,
    #[clap(value_enum)]
    #[arg(short, long, default_value_t = MinaNetwork::Mainnet)]
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


    let receiver = GoogleCloudBlockReceiver::new(
        cli_args.start_from, cli_args.batch_size, 
        &temp_blocks_dir, 
        poll_freq, cli_args.mina_network, cli_args.google_cloud_bucket
    ).await?;

    Ok(())
}