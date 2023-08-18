use crate::{
    block::{
        parser::{filesystem::FilesystemParser, BlockParser},
        store::BlockStore,
        Block, BlockHash, BlockWithoutHeight,
    },
    receiver::{filesystem::FilesystemReceiver, BlockReceiver, google_cloud::{GoogleCloudBlockReceiver, MinaNetwork}},
    state::{
        ledger::{self, genesis::GenesisRoot, public_key::PublicKey, Ledger},
        summary::{SummaryShort, SummaryVerbose},
        IndexerMode, IndexerState,
    },
    store::IndexerStore,
    CANONICAL_UPDATE_THRESHOLD, MAINNET_GENESIS_HASH, MAINNET_TRANSITION_FRONTIER_K,
    PRUNE_INTERVAL_DEFAULT, SOCKET_NAME,
};
use clap::{Parser, ValueEnum};
use futures::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use interprocess::local_socket::tokio::{LocalSocketListener, LocalSocketStream};
use log::trace;
use serde::Deserializer;
use serde_derive::{Deserialize, Serialize};
use std::{path::PathBuf, process, sync::Arc, time::Duration};
use tokio::{
    fs::{self, create_dir_all, metadata},
    io,
    sync::mpsc,
};
use tracing::{debug, error, info, instrument, level_filters::LevelFilter};
use uuid::Uuid;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Deserialize)]
pub enum WatchMode {
    #[serde(rename = "filesystem")]
    Filesystem,
    #[serde(rename = "google_cloud")]
    GoogleCloud
}

impl std::fmt::Display for WatchMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            WatchMode::Filesystem => "filesystem",
            WatchMode::GoogleCloud => "google_cloud",
        })
    }
}

#[derive(Parser, Debug, Clone, Deserialize)]
#[command(author, version, about, long_about = None)]
pub struct ServerArgs {
    /// Path to the root ledger (if non-genesis, set --non-genesis-ledger and --root-hash)
    #[arg(short, long)]
    ledger: PathBuf,
    /// Hash of the base ledger
    #[arg(
        long,
        default_value = MAINNET_GENESIS_HASH
    )]
    root_hash: String,
    /// Path to startup blocks directory
    #[arg(short, long, default_value = concat!(env!("HOME"), "/.mina-indexer/startup-blocks"))]
    startup_dir: PathBuf,
    /// Path to directory to watch for new blocks
    #[arg(short, long, default_value = concat!(env!("HOME"), "/.mina-indexer/watch-blocks"))]
    watch_dir: PathBuf,
    #[arg(long, default_value_t = String::from("mina_network_block_data"))]
    google_cloud_watch_bucket: String,
    #[arg(long)]
    google_cloud_watcher_lookup_num: Option<u64>,
    #[arg(long)]
    google_cloud_watcher_lookup_freq: Option<u64>,
    #[arg(long)]
    google_cloud_watcher_lookup_network: Option<MinaNetwork>,
    #[arg(long, default_value_t = WatchMode::Filesystem)]
    watch_mode: WatchMode,
    /// Path to directory for rocksdb
    #[arg(short, long, default_value = concat!(env!("HOME"), "/.mina-indexer/database"))]
    pub database_dir: PathBuf,
    /// Path to directory for logs
    #[arg(long, default_value = concat!(env!("HOME"), "/.mina-indexer/logs"))]
    pub log_dir: PathBuf,
    /// Only store canonical blocks in the db
    #[arg(short, long, default_value_t = false)]
    keep_non_canonical_blocks: bool,
    /// Max file log level
    #[serde(deserialize_with = "level_filter_deserializer")]
    #[arg(long, default_value_t = LevelFilter::DEBUG)]
    pub log_level: LevelFilter,
    /// Max stdout log level
    #[serde(deserialize_with = "level_filter_deserializer")]
    #[arg(long, default_value_t = LevelFilter::INFO)]
    pub log_level_stdout: LevelFilter,
    /// Interval for pruning the root branch
    #[arg(short, long, default_value_t = PRUNE_INTERVAL_DEFAULT)]
    prune_interval: u32,
    /// Threshold for updating the canonical tip/ledger
    #[arg(short, long, default_value_t = CANONICAL_UPDATE_THRESHOLD)]
    canonical_update_threshold: u32,
    /// Path to an indexer snapshot
    #[arg(long)]
    pub snapshot_path: Option<PathBuf>,
}

pub enum ConfigWatchMode {
    Filesystem(PathBuf),
    GoogleCloud(String, u64, u64, MinaNetwork)
}

pub struct IndexerConfiguration {
    ledger: GenesisRoot,
    root_hash: BlockHash,
    startup_dir: PathBuf,
    watch_mode: ConfigWatchMode,
    keep_noncanonical_blocks: bool,
    prune_interval: u32,
    canonical_update_threshold: u32,
    from_snapshot: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct SaveCommand(PathBuf);

#[derive(Debug, Serialize, Deserialize)]
struct SaveResponse(String);

pub async fn handle_command_line_arguments(
    args: ServerArgs,
) -> anyhow::Result<IndexerConfiguration> {
    trace!("Parsing server args");

    let ledger = args.ledger;
    let root_hash = BlockHash(args.root_hash.to_string());
    let startup_dir = args.startup_dir;
    let watch_dir = args.watch_dir;
    let watch_mode = args.watch_mode;
    let watch_bucket = args.google_cloud_watch_bucket; 
    let google_cloud_watcher_lookup_num = args.google_cloud_watcher_lookup_num.unwrap_or(20);
    let google_cloud_watcher_lookup_freq = args.google_cloud_watcher_lookup_freq.unwrap_or(30);
    let google_cloud_watcher_lookup_network = args.google_cloud_watcher_lookup_network.unwrap_or(MinaNetwork::Mainnet);
    let keep_noncanonical_blocks = args.keep_non_canonical_blocks;
    let prune_interval = args.prune_interval;
    let canonical_update_threshold = args.canonical_update_threshold;
    let watch_mode = match watch_mode {
        WatchMode::Filesystem => ConfigWatchMode::Filesystem(watch_dir),
        WatchMode::GoogleCloud => ConfigWatchMode::GoogleCloud(
            watch_bucket, 
            google_cloud_watcher_lookup_num, 
            google_cloud_watcher_lookup_freq, 
            google_cloud_watcher_lookup_network
        ),
    };

    assert!(
        ledger.is_file(),
        "Ledger file does not exist at {}",
        ledger.display()
    );
    assert!(
        // bad things happen if this condition fails
        canonical_update_threshold < MAINNET_TRANSITION_FRONTIER_K,
        "canonical update threshold must be strictly less than the transition frontier length!"
    );

    info!("Parsing ledger file at {}", ledger.display());

    match ledger::genesis::parse_file(&ledger).await {
        Err(err) => {
            error!(
                reason = "Unable to parse ledger",
                error = err.to_string(),
                path = &ledger.display().to_string()
            );
            process::exit(100)
        }
        Ok(ledger) => {
            info!("Ledger parsed successfully!");

            Ok(IndexerConfiguration {
                ledger,
                root_hash,
                startup_dir,
                watch_mode,
                keep_noncanonical_blocks,
                prune_interval,
                canonical_update_threshold,
                from_snapshot: args.snapshot_path.is_some(),
            })
        }
    }
}

#[instrument(skip_all)]
pub async fn run(
    config: IndexerConfiguration,
    indexer_store: Arc<IndexerStore>,
) -> Result<(), anyhow::Error> {
    debug!("Checking that a server instance isn't already running");
    LocalSocketStream::connect(SOCKET_NAME)
        .await
        .expect_err("Server is already running... Exiting.");

    debug!("Setting Ctrl-C handler");
    ctrlc::set_handler(move || {
        info!("SIGINT received. Exiting.");
        process::exit(0);
    })
    .expect("Error setting Ctrl-C handler");

    info!("Starting mina-indexer server");
    let IndexerConfiguration {
        ledger,
        root_hash,
        startup_dir,
        watch_mode,
        keep_noncanonical_blocks,
        prune_interval,
        canonical_update_threshold,
        from_snapshot,
    } = config;

    let database_dir = PathBuf::from(indexer_store.db_path());
    let mode = if keep_noncanonical_blocks {
        IndexerMode::Full
    } else {
        IndexerMode::Light
    };
    let mut indexer_state = if !from_snapshot {
        info!(
            "Initializing indexer state from blocks in {}",
            startup_dir.display()
        );
        let mut indexer_state = IndexerState::new(
            mode,
            root_hash.clone(),
            ledger.ledger,
            indexer_store,
            MAINNET_TRANSITION_FRONTIER_K,
            prune_interval,
            canonical_update_threshold,
        )?;

        let block_parser = Box::new(FilesystemParser::new(&startup_dir)?)
            as Box<dyn BlockParser + Send + Sync + 'static>;
        indexer_state.initialize_with_parser(block_parser).await?;

        indexer_state
    } else {
        info!("initializing indexer state from snapshot");
        IndexerState::from_state_snapshot(
            indexer_store,
            MAINNET_TRANSITION_FRONTIER_K,
            prune_interval,
            canonical_update_threshold,
        )?
    };

    let mut block_receiver: Box<dyn BlockReceiver + Send + Sync + 'static> = match watch_mode {
        ConfigWatchMode::Filesystem(path) => {
            let mut filesystem_receiver = FilesystemReceiver::new(1024, 64).await?;
            filesystem_receiver.load_directory(&path)?;
            info!("Block receiver set to watch {path:?}");
            Box::new(filesystem_receiver)
        },
        ConfigWatchMode::GoogleCloud(bucket, lookup_num, lookup_freq, network) => {
            let best_tip_height = indexer_state.root_branch.best_tip().unwrap().blockchain_length.unwrap();
            let mut temp_blocks_dir = database_dir.clone();
            temp_blocks_dir.push("temp_blocks");
            create_dir_all(&temp_blocks_dir).await?;
            let google_cloud_receiver = GoogleCloudBlockReceiver::new(
                best_tip_height as u64, lookup_num, temp_blocks_dir, Duration::from_secs(lookup_freq), network, bucket
            ).await?;
            Box::new(google_cloud_receiver)
        }
    };

    
    let listener = LocalSocketListener::bind(SOCKET_NAME).unwrap_or_else(|e| {
        if e.kind() == io::ErrorKind::AddrInUse {
            let name = &SOCKET_NAME[1..];
            debug!(
                "Domain socket: {} already in use. Removing old vestige",
                name
            );
            std::fs::remove_file(name).expect("Should be able to remove socket file");
            LocalSocketListener::bind(SOCKET_NAME).unwrap_or_else(|e| {
                panic!("Unable to bind domain socket {:?}", e);
            })
        } else {
            panic!("Unable to bind domain socket {:?}", e);
        }
    });

    info!("Local socket listener started");

    let (save_tx, mut save_rx) = tokio::sync::mpsc::channel(1);
    let (mut save_resp_tx, save_resp_rx) = spmc::channel();
    let save_tx = Arc::new(save_tx);
    let save_resp_rx = Arc::new(save_resp_rx);

    loop {
        debug!("waiting for future on main thread");
        tokio::select! {
            Ok(block_response) = block_receiver.recv_block() => {
                trace!("got block from block receiver");
                match block_response {
                    None => {
                        info!("Block receiver shutdown, system exit");
                        return Ok(())
                    }
                    Some(precomputed_block) => {
                        let block = BlockWithoutHeight::from_precomputed(&precomputed_block);
                        debug!("Receiving block {block:?}");

                        indexer_state.add_block(&precomputed_block)?;
                        indexer_state.update_canonical()?;
                        info!("Added {block:?}");
                    }
                }
            }

            Ok(conn) = listener.accept() => {
                info!("Receiving connection");
                let best_tip = indexer_state.best_tip_block().clone();
                let summary = indexer_state.summary_verbose();
                let ledger = indexer_state.best_ledger()?.unwrap();
                let save_tx = save_tx.clone();
                let save_resp_rx = save_resp_rx.clone();

                debug!("Spawning secondary readonly RocksDB instance");
                let primary_path = database_dir.clone();
                let mut secondary_path = primary_path.clone();
                secondary_path.push(Uuid::new_v4().to_string());
                let block_store_readonly = IndexerStore::new_read_only(&primary_path, &secondary_path)?;

                // handle the connection
                debug!("Handling connection");
                tokio::spawn(async move {
                    if let Err(e) = handle_conn(
                        conn, 
                        block_store_readonly, 
                        best_tip, 
                        ledger, 
                        summary, 
                        save_tx, 
                        save_resp_rx
                    ).await {
                        error!("Error handling connection: {e}");
                    }

                    debug!("Removing readonly instance at {}", secondary_path.display());
                    tokio::fs::remove_dir_all(&secondary_path).await.ok();
                });
            }

            Some(SaveCommand(snapshot_path)) = save_rx.recv() => {
                trace!("saving snapshot in {}", &snapshot_path.display());
                match indexer_state.save_snapshot(snapshot_path) {
                    Ok(path) => save_resp_tx.send(Some(
                        SaveResponse(format!(
                            "snapshot created at {}\n",
                            path.display()
                    ))))?,
                    Err(e) => save_resp_tx.send(Some(SaveResponse(e.to_string())))?,
                }
            }
        }
    }
}

#[instrument(skip_all)]
async fn handle_conn(
    conn: LocalSocketStream,
    db: IndexerStore,
    best_tip: Block,
    ledger: Ledger,
    summary: SummaryVerbose,
    save_tx: Arc<mpsc::Sender<SaveCommand>>,
    save_rx: Arc<spmc::Receiver<Option<SaveResponse>>>,
) -> Result<(), anyhow::Error> {
    let (reader, mut writer) = conn.into_split();
    let mut reader = BufReader::new(reader);
    let mut buffer = Vec::with_capacity(128);
    let _read = reader.read_until(0, &mut buffer).await?;

    let mut buffers = buffer.split(|byte| *byte == 32);
    let command = buffers.next().unwrap();
    let command_string = String::from_utf8(command.to_vec())?;

    match command_string.as_str() {
        "account" => {
            info!("Received account command");
            let data_buffer = buffers.next().unwrap();
            let public_key = PublicKey::from_address(&String::from_utf8(
                data_buffer[..data_buffer.len() - 1].to_vec(),
            )?)?;
            let account = ledger.accounts.get(&public_key);
            if let Some(account) = account {
                debug!("Writing account {account:?} to client");
                let bytes = bcs::to_bytes(account)?;
                writer.write_all(&bytes).await?;
            } else {
                debug!("Got bad public key, writing error message");
                writer.write_all(format!(
                    "{:?} is not in the ledger!", public_key)
                    .as_bytes()
                ).await?;
            }
        }
        "best_chain" => {
            info!("Received best_chain command");
            let data_buffer = buffers.next().unwrap();
            let num = String::from_utf8(data_buffer[..data_buffer.len() - 1].to_vec())?
                .parse::<usize>()?;
            let mut parent_hash = best_tip.parent_hash;
            let mut best_chain = vec![db.get_block(&best_tip.state_hash)?.unwrap()];
            for _ in 1..num {
                let parent_pcb = db.get_block(&parent_hash)?.unwrap();
                parent_hash =
                    BlockHash::from_hashv1(parent_pcb.protocol_state.previous_state_hash.clone());
                best_chain.push(parent_pcb);
            }
            let bytes = bcs::to_bytes(&best_chain)?;
            writer.write_all(&bytes).await?;
        }
        "best_ledger" => {
            info!("Received best_ledger command");
            let data_buffer = buffers.next().unwrap();
            let path = &String::from_utf8(data_buffer[..data_buffer.len() - 1].to_vec())?
                .parse::<PathBuf>()?;
            debug!("Writing ledger to {}", path.display());
            fs::write(path, format!("{ledger:?}")).await?;
            let bytes = bcs::to_bytes(&format!("Ledger written to {}", path.display()))?;
            writer.write_all(&bytes).await?;
        }
        "summary" => {
            info!("Received summary command");
            let data_buffer = buffers.next().unwrap();
            let verbose = String::from_utf8(data_buffer[..data_buffer.len() - 1].to_vec())?
                .parse::<bool>()?;
            if verbose {
                let bytes = bcs::to_bytes(&summary)?;
                writer.write_all(&bytes).await?;
            } else {
                let summary: SummaryShort = summary.into();
                let bytes = bcs::to_bytes(&summary)?;
                writer.write_all(&bytes).await?;
            }
        }
        "save_state" => {
            info!("Received save_state command");
            let data_buffer = buffers.next().unwrap();
            let snapshot_path = PathBuf::from(String::from_utf8(
                data_buffer[..data_buffer.len() - 1].to_vec(),
            )?);

            trace!("sending SaveCommand to primary indexer thread");
            save_tx.send(SaveCommand(snapshot_path)).await?;

            trace!("awaiting SaveResponse from primary indexer thread");
            let mut save_result = None;
            tokio::spawn(async move {
                loop {
                    match save_rx.try_recv() {
                        Ok(save_response) => {
                            save_result = save_response;
                            break;
                        },
                        Err(e) => match e {
                            spmc::TryRecvError::Empty => continue,
                            spmc::TryRecvError::Disconnected => break,
                        }
                    }
                }
                if let Some(save_response) = save_result {
                    writer.write_all(&bcs::to_bytes(&save_response)?).await?;
                } else {
                    writer.write_all(b"handler was disconnected from main thread!").await?;
                }

                Ok::<(), anyhow::Error>(())
            });
        }
        bad_request => {
            let err_msg = format!("Malformed request: {bad_request}");
            error!("{err_msg}");
            return Err(anyhow::Error::msg(err_msg));
        }
    }

    Ok(())
}

pub async fn create_dir_if_non_existent(path: &str) {
    if metadata(path).await.is_err() {
        debug!("Creating directory {path}");
        create_dir_all(path).await.unwrap();
    }
}

pub fn level_filter_deserializer<'de, D>(deserializer: D) -> Result<LevelFilter, D::Error>
where
    D: Deserializer<'de>,
{
    struct YAMLStringVisitor;

    impl<'de> serde::de::Visitor<'de> for YAMLStringVisitor {
        type Value = LevelFilter;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a string containing yaml data")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            // unfortunately we lose some typed information
            // from errors deserializing the json string
            let level_filter_str: &str = serde_yaml::from_str(v).map_err(E::custom)?;
            match level_filter_str {
                "info" => Ok(LevelFilter::INFO),
                "debug" => Ok(LevelFilter::DEBUG),
                "error" => Ok(LevelFilter::ERROR),
                "trace" => Ok(LevelFilter::TRACE),
                "warn" => Ok(LevelFilter::TRACE),
                "off" => Ok(LevelFilter::OFF),
                other => Err(E::custom(format!("{} is not a valid level filter", other))),
            }
        }
    }

    deserializer.deserialize_any(YAMLStringVisitor)
}
