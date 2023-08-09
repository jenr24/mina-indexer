use async_ringbuf::{AsyncHeapConsumer, AsyncHeapProducer};
use serde_derive::{Serialize, Deserialize};
use thiserror::Error;
use std::{time::{Duration, Instant}, path::{Path, PathBuf}};
use tokio::{sync::{watch, mpsc}, time::sleep, process::Command, io::AsyncWriteExt, fs::read_dir};

use crate::block::{precomputed::PrecomputedBlock, is_valid_block_file, parse_file};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum MinaNetwork {
    #[serde(rename = "mainnet")]
    Mainnet,
    #[serde(rename = "berkeley")]
    Berkeley,
    #[serde(rename = "testnet")]
    Testnet,
}

impl MinaNetwork {
    pub fn to_string(&self) -> String {
        String::from(match self {
            MinaNetwork::Mainnet => "mainnet",
            MinaNetwork::Berkeley => "berkeley",
            MinaNetwork::Testnet => "testnet",
        })
    }
}

#[derive(Debug, Error)]
pub enum GoogleCloudBlockWorkerError {
    TempBlocksDirIsNotADirectory(PathBuf),
    IOError(tokio::io::Error),
    BlockParseError(PathBuf, String),
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum GoogleCloudBlockWorkerCommand {
    Shutdown,
}

pub struct GoogleCloudBlockWorker {
    max_height: u64,
    overlap_num: u64,
    temp_blocks_dir: PathBuf,
    update_freq: Duration,
    network: MinaNetwork,
    bucket: String,
    blocks_sender: AsyncHeapProducer<PrecomputedBlock>,
    error_sender: watch::Sender<GoogleCloudBlockWorkerError>,
    command_receiver: mpsc::Receiver<GoogleCloudBlockWorkerCommand>
}

impl GoogleCloudBlockWorker {
    pub fn new(
        max_height: u64,
        overlap_num: u64,
        temp_blocks_dir: impl AsRef<Path>,
        update_freq: Duration, 
        network: MinaNetwork, 
        bucket: String, 
        blocks_sender: AsyncHeapProducer<PrecomputedBlock>,
        error_sender: watch::Sender<GoogleCloudBlockWorkerError>,
        command_receiver: mpsc::Receiver<GoogleCloudBlockWorkerCommand>)
    -> Result<Self, GoogleCloudBlockWorkerError> {
        if !temp_blocks_dir.as_ref().is_dir() {
            return Err(GoogleCloudBlockWorkerError::TempBlocksDirIsNotADirectory(
                temp_blocks_dir.as_ref().into())
            );
        }
        let temp_blocks_dir = temp_blocks_dir.as_ref().into();
        Ok(Self { max_height, overlap_num, temp_blocks_dir, update_freq, network, bucket, blocks_sender, error_sender, command_receiver })
    }

    pub async fn worker_loop(&mut self) -> () {
        loop {
            let work_unit_started = Instant::now();

            if let Ok(command) = self.command_receiver.try_recv() {
                match command {
                    GoogleCloudBlockWorkerCommand::Shutdown => {
                        if tokio::fs::metadata(&self.temp_blocks_dir).await.is_ok() {
                            tokio::fs::remove_dir_all(&self.temp_blocks_dir)
                                .await.expect("remove temp dir works");
                        }
                        return;
                    },
                }
            }

            let mut child = match Command::new("gsutil")
                .arg("-m")
                .arg("cp")
                .arg("-n")
                .arg("-I")
                .arg(AsRef::<Path>::as_ref(&self.temp_blocks_dir))
                .spawn().map_err(|e| GoogleCloudBlockWorkerError::IOError(e)) {
                    Ok(child) => child,
                    Err(io_error) => {
                        self.error_sender.send_replace(io_error);
                        continue;
                    },
                };
            let mut child_stdin = child.stdin.take().unwrap();

            let start = 2.max(self.max_height.saturating_sub(self.overlap_num));
            let end = self.max_height + self.overlap_num;

            for length in start..=end {
                if let Err(e) = child_stdin.write_all(bucket_file_from_length(
                    self.network, &self.bucket, length).as_bytes()
                ).await {
                    self.error_sender.send_replace(GoogleCloudBlockWorkerError::IOError(e));
                }
            }

            match read_dir(&self.temp_blocks_dir).await {
                Err(io_error) => {
                    self.error_sender.send_replace(GoogleCloudBlockWorkerError::IOError(io_error));
                },
                Ok(mut read_dir) => {
                    while let Ok(Some(entry)) = read_dir.next_entry().await {
                        if !is_valid_block_file(&entry.path()) {
                            continue;
                        }

                        match parse_file(&entry.path()).await {
                            Ok(precomputed_block) => {
                                self.blocks_sender.push(precomputed_block)
                                    .await
                                    .expect("consumer not dropped");

                                if entry.metadata().await.is_ok() {
                                    tokio::fs::remove_file(entry.path()).await
                                        .expect("file guaranteed to exist");
                                }
                            },
                            Err(parse_error) => {
                                self.error_sender.send_replace(
                                    GoogleCloudBlockWorkerError::BlockParseError(entry.path(), parse_error.to_string())
                                );
                            },
                        }
                    }
                },
                
            }

            let work_unit_finished = Instant::now();
            let work_unit_duration = work_unit_finished
                .duration_since(work_unit_started);
            if work_unit_duration < self.update_freq {
                sleep(self.update_freq - work_unit_duration).await;
            }
        }
    }
}

pub fn bucket_file_from_length(network: MinaNetwork, bucket: &str, length: u64) -> String {
    format!("gs://{bucket}/{}-{length}-*.json\n", network.to_string())
}

impl std::fmt::Display for GoogleCloudBlockWorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GoogleCloudBlockWorkerError::TempBlocksDirIsNotADirectory(not_directory) 
                => f.write_str(&format!("temporary block directory {} is not a directory", not_directory.display())),
            GoogleCloudBlockWorkerError::IOError(io_error) 
                => f.write_str(&format!("encountered an IOError: {}", io_error.to_string())),
            GoogleCloudBlockWorkerError::BlockParseError(block_file, parse_error) 
                => f.write_str(&format!("could not parse block file {}: {}", block_file.display(), parse_error)),
        }
    }
}