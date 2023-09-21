use std::{path::{PathBuf, Path}, process::Stdio};

use clap::{Parser, ValueEnum};
use serde_derive::{Serialize, Deserialize};
use tokio::{process::Command, io::AsyncWriteExt};
use tracing::{instrument, trace};

#[derive(ValueEnum, Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum MinaNetwork {
    #[serde(rename = "mainnet")]
    Mainnet,
    #[serde(rename = "berkeley")]
    Berkeley,
    #[serde(rename = "testnet")]
    Testnet,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct MinaBlocksCLI {
    #[arg(short, long)]
    num_blocks: u64,
    #[arg(short, long)]
    start_from: u64,
    #[arg(short, long)]
    output_dir: PathBuf,
    #[arg(value_enum, short, long)]
    mina_network: MinaNetwork,
    #[arg(short, long)]
    bucket: String
}

#[tokio::main]
pub async fn main() -> () {
    let _cli = MinaBlocksCLI::parse();
}

#[instrument]
async fn gsutil_download_blocks(
    temp_blocks_dir: impl AsRef<Path> + std::fmt::Debug,
    max_height: u64,
    overlap_num: u64,
    blocks_bucket: impl AsRef<str> + std::fmt::Debug,
    network: MinaNetwork,
) -> Result<(), anyhow::Error> {
    trace!("spawning child gsutil process");
    let mut child = Command::new("gsutil")
        .stdin(Stdio::piped())
        .arg("-m")
        .arg("cp")
        .arg("-n")
        .arg("-I")
        .arg(AsRef::<Path>::as_ref(temp_blocks_dir.as_ref()))
        .spawn()
        .map_err(|e| anyhow::Error::msg(e.to_string()))?;
    let mut child_stdin = child.stdin.take().unwrap();

    let start = 2.max(max_height.saturating_sub(overlap_num));
    let end = max_height + overlap_num;

    for length in start..=end {
        let bucket_file = bucket_file_from_length(network, blocks_bucket.as_ref(), length);
        trace!("downloading bucket file {}", bucket_file);
        child_stdin
            .write_all(bucket_file.as_bytes())
            .await
            .map_err(|e| anyhow::Error::msg(e.to_string()))?;
    }

    Ok(())
}

fn bucket_file_from_length(network: MinaNetwork, bucket: &str, length: u64) -> String {
    format!("gs://{bucket}/{}-{length}-*.json\n", network)
}

impl std::fmt::Display for MinaNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            MinaNetwork::Mainnet => "mainnet",
            MinaNetwork::Berkeley => "berkeley",
            MinaNetwork::Testnet => "testnet",
        })
    }
}