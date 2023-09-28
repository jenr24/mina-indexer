use crate::{
    block::{
        parser::BlockParser, precomputed::PrecomputedBlock, store::BlockStore, BlockHash,
        BlockWithoutHeight,
    },
    receiver::{filesystem::FilesystemReceiver, BlockReceiver},
    state::{
        ledger::{genesis::GenesisRoot, public_key::PublicKey},
        IndexerState, Tip,
    },
    store::IndexerStore,
    MAINNET_TRANSITION_FRONTIER_K, SOCKET_NAME,
};
use anyhow::anyhow;
use futures::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use interprocess::local_socket::tokio::{LocalSocketListener, LocalSocketStream};
use log::trace;

use serde_derive::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    process,
    sync::Arc,
    time::Duration,
};
use tokio::{
    fs::{self, create_dir_all, metadata},
    io,
    sync::{mpsc, watch, RwLock},
    task::JoinHandle,
};
use tracing::{debug, info, instrument};

pub struct IndexerConfiguration {
    pub ledger: GenesisRoot,
    pub is_genesis_ledger: bool,
    pub root_hash: BlockHash,
    pub startup_dir: PathBuf,
    pub watch_dir: PathBuf,
    pub prune_interval: u32,
    pub canonical_threshold: u32,
    pub canonical_update_threshold: u32,
    pub from_snapshot: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SaveCommand(PathBuf);

#[derive(Debug, Serialize, Deserialize)]
pub struct SaveResponse(String);

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum MinaIndexerRunPhase {
    JustStarted,
    ConnectingToIPCSocket,
    SettingSIGINTHandler,
    InitializingState,
    StateInitializedFromParser,
    StateInitializedFromSnapshot,
    StartingBlockReceiver,
    StartingIPCSocketListener,
    StartingMainServerLoop,
    ReceivingBlock,
    ReceivingIPCConnection,
    SavingStateSnapshot,
}

pub enum MinaIndexerQuery {
    NumBlocksProcessed,
    BestTip,
    CanonicalTip,
    Uptime,
}

pub enum MinaIndexerQueryResponse {
    NumBlocksProcessed(u32),
    BestTip(Tip),
    CanonicalTip(Tip),
    Uptime(Duration),
}

pub struct MinaIndexer {
    _loop_join_handle: JoinHandle<anyhow::Result<()>>,
    phase_receiver: watch::Receiver<MinaIndexerRunPhase>,
    query_sender: mpsc::Sender<(MinaIndexerQuery, oneshot::Sender<MinaIndexerQueryResponse>)>,
}

impl MinaIndexer {
    pub async fn new(
        config: IndexerConfiguration,
        store: Arc<IndexerStore>,
    ) -> anyhow::Result<Self> {
        let (phase_sender, phase_receiver) = watch::channel(MinaIndexerRunPhase::JustStarted);
        let (query_sender, query_receiver) = mpsc::channel(1);
        let (save_tx, save_rx) = tokio::sync::mpsc::channel(1);
        let (save_resp_tx, save_resp_rx) = spmc::channel();

        let state_lock: Arc<RwLock<Option<IndexerState>>> = Arc::new(RwLock::new(None));

        let loop_state_lock = state_lock.clone();
        let state_store = store.clone();
        let _loop_join_handle = tokio::spawn(async move {
            let watch_dir = config.watch_dir.clone();
            let phase_sender =
                initialize(config, state_store, phase_sender, &loop_state_lock).await?;
            run(
                watch_dir,
                &loop_state_lock,
                phase_sender,
                query_receiver,
                save_rx,
                save_resp_tx,
            )
            .await
        });

        tokio::spawn(async move {
            LocalSocketStream::connect(SOCKET_NAME)
                .await
                .expect_err("Server is already running... Exiting.");
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

            loop {
                match listener.accept().await {
                    Err(_e) => {
                        process::exit(1);
                    }
                    Ok(stream) => {
                        let indexer_state = loop {
                            if let Ok(state) = state_lock.try_read() {
                                break state;
                            }
                        };
                        let (reader, mut writer) = stream.into_split();
                        let mut reader = BufReader::new(reader);
                        let mut buffer = Vec::with_capacity(1024);
                        let read_size = reader.read_until(0, &mut buffer).await.unwrap_or(0);
                        if read_size == 0 {
                            continue;
                        }
                        let mut buffers = buffer.split(|byte| *byte == b' ');
                        let command = buffers.next().unwrap();
                        let command_string = String::from_utf8(command.to_vec()).unwrap();
                        match command_string.as_str() {
                            "account" => {
                                let data_buffer = buffers.next().unwrap();
                                let public_key = PublicKey::from_address(
                                    &String::from_utf8(
                                        data_buffer[..data_buffer.len() - 1].to_vec(),
                                    )
                                    .unwrap(),
                                )
                                .unwrap();
                                match indexer_state.as_ref() {
                                    None => writer
                                        .write_all(
                                            b"Mina Indexer state still initializing, please wait",
                                        )
                                        .await
                                        .unwrap(),
                                    Some(state) => {
                                        let ledger = state.best_ledger().unwrap().unwrap();
                                        let account = ledger.accounts.get(&public_key);
                                        if let Some(account) = account {
                                            let bytes = bcs::to_bytes(account).unwrap();
                                            writer.write_all(&bytes).await.unwrap();
                                        }
                                    }
                                }
                            }
                            "best_chain" => {
                                info!("Received best_chain command");
                                let data_buffer = buffers.next().unwrap();
                                let num = String::from_utf8(
                                    data_buffer[..data_buffer.len() - 1].to_vec(),
                                )
                                .unwrap()
                                .parse::<usize>()
                                .unwrap();
                                match indexer_state.as_ref() {
                                    None => writer
                                        .write_all(
                                            &bcs::to_bytes::<Option<Vec<PrecomputedBlock>>>(&None)
                                                .unwrap(),
                                        )
                                        .await
                                        .unwrap(),
                                    Some(state) => {
                                        let best_tip = state.best_tip_block().clone();
                                        let mut parent_hash = best_tip.parent_hash;
                                        let mut best_chain = vec![store
                                            .get_block(&best_tip.state_hash)
                                            .unwrap()
                                            .unwrap()];
                                        for _ in 1..num {
                                            let parent_pcb =
                                                store.get_block(&parent_hash).unwrap().unwrap();
                                            parent_hash = BlockHash::from_hashv1(
                                                parent_pcb
                                                    .protocol_state
                                                    .previous_state_hash
                                                    .clone(),
                                            );
                                            best_chain.push(parent_pcb);
                                        }
                                        let bytes = bcs::to_bytes(&Some(best_chain)).unwrap();
                                        writer.write_all(&bytes).await.unwrap();
                                    }
                                }
                            }
                            "best_ledger" => {
                                info!("Received best_ledger command");
                                let data_buffer = buffers.next().unwrap();
                                let path = &String::from_utf8(
                                    data_buffer[..data_buffer.len() - 1].to_vec(),
                                )
                                .unwrap()
                                .parse::<PathBuf>()
                                .unwrap();
                                match indexer_state.as_ref() {
                                    None => writer
                                        .write_all(
                                            b"Mina Indexer state still initializing, please wait",
                                        )
                                        .await
                                        .unwrap(),
                                    Some(state) => {
                                        let ledger = state.best_ledger().unwrap().unwrap();
                                        if !path.is_dir() {
                                            debug!("Writing ledger to {}", path.display());
                                            fs::write(path, format!("{ledger:?}")).await.unwrap();
                                            let bytes = bcs::to_bytes(&format!(
                                                "Ledger written to {}",
                                                path.display()
                                            ))
                                            .unwrap();
                                            writer.write_all(&bytes).await.unwrap();
                                        } else {
                                            let bytes = bcs::to_bytes(&format!(
                                                "The path provided must be a file: {}",
                                                path.display()
                                            ))
                                            .unwrap();
                                            writer.write_all(&bytes).await.unwrap();
                                        }
                                    }
                                }
                            }
                            "summary" => {
                                info!("Received summary command");
                                let data_buffer = buffers.next().unwrap();
                                let verbose = String::from_utf8(
                                    data_buffer[..data_buffer.len() - 1].to_vec(),
                                )
                                .unwrap()
                                .parse::<bool>()
                                .unwrap();
                                match indexer_state.as_ref() {
                                    None => {
                                        info!("Pre-init summary to client");
                                        let _ = writer.write_all("Mina Indexer state still initializing, please wait".as_bytes())
                                        .await
                                        .map_err(|e| { info!("{e:?}"); });
                                    }
                                    Some(state) => {
                                        if verbose {
                                            let summary = state.summary_verbose();
                                            let bytes = bcs::to_bytes(&summary).unwrap();
                                            info!("Writing summary to client");
                                            writer.write_all(&bytes).await.unwrap();
                                        } else {
                                            let summary = state.summary_short();
                                            let bytes = bcs::to_bytes(&summary).unwrap();
                                            info!("Writing summary to client");
                                            writer.write_all(&bytes).await.unwrap();
                                        }
                                    }
                                }
                            }
                            "save_state" => {
                                info!("Received save_state command");
                                let data_buffer = buffers.next().unwrap();
                                let snapshot_path = PathBuf::from(
                                    String::from_utf8(
                                        data_buffer[..data_buffer.len() - 1].to_vec(),
                                    )
                                    .unwrap(),
                                );
                                match indexer_state.as_ref() {
                                    None => writer
                                        .write_all(
                                            b"Mina Indexer state still initializing, please wait",
                                        )
                                        .await
                                        .unwrap(),
                                    Some(_state) => {
                                        save_tx.send(SaveCommand(snapshot_path)).await.unwrap();
                                        writer.write_all(b"saving snapshot...").await.unwrap();
                                        match save_resp_rx.recv().unwrap() {
                                            None => writer
                                                .write_all(b"Unable to save snapshot!")
                                                .await
                                                .unwrap(),
                                            Some(SaveResponse(resp)) => {
                                                writer.write_all(resp.as_bytes()).await.unwrap()
                                            }
                                        }
                                    }
                                }
                            }
                            _bad_request => {
                                continue;
                            }
                        }
                    }
                }
            }
        });

        Ok(Self {
            _loop_join_handle,
            phase_receiver,
            query_sender,
        })
    }

    async fn send_query(
        &self,
        command: MinaIndexerQuery,
    ) -> anyhow::Result<MinaIndexerQueryResponse> {
        let (response_sender, response_receiver) = oneshot::channel();
        self.query_sender
            .send((command, response_sender))
            .await
            .map_err(|_| anyhow!("could not send command to running Mina Indexer"))?;
        response_receiver.recv().map_err(|recv_err| recv_err.into())
    }

    pub fn initialized(&self) -> bool {
        use MinaIndexerRunPhase::*;
        !matches!(
            *self.phase_receiver.borrow(),
            JustStarted | SettingSIGINTHandler | InitializingState
        )
    }

    pub fn state(&self) -> MinaIndexerRunPhase {
        *self.phase_receiver.borrow()
    }

    pub async fn blocks_processed(&self) -> anyhow::Result<u32> {
        match self
            .send_query(MinaIndexerQuery::NumBlocksProcessed)
            .await?
        {
            MinaIndexerQueryResponse::NumBlocksProcessed(blocks_processed) => Ok(blocks_processed),
            _ => Err(anyhow!("unexpected response!")),
        }
    }
}

pub async fn initialize(
    config: IndexerConfiguration,
    store: Arc<IndexerStore>,
    phase_sender: watch::Sender<MinaIndexerRunPhase>,
    state_lock: &RwLock<Option<IndexerState>>,
) -> anyhow::Result<watch::Sender<MinaIndexerRunPhase>> {
    use MinaIndexerRunPhase::*;
    debug!("Checking that a server instance isn't already running");
    phase_sender.send_replace(ConnectingToIPCSocket);

    phase_sender.send_replace(SettingSIGINTHandler);
    debug!("Setting Ctrl-C handler");
    ctrlc::set_handler(move || {
        info!("SIGINT received. Exiting.");
        process::exit(0);
    })
    .expect("Error setting Ctrl-C handler");

    phase_sender.send_replace(InitializingState);
    info!("Starting mina-indexer server");
    let IndexerConfiguration {
        ledger,
        is_genesis_ledger,
        root_hash,
        startup_dir,
        watch_dir: _,
        prune_interval,
        canonical_threshold,
        canonical_update_threshold,
        from_snapshot,
    } = config;

    let state = if !from_snapshot {
        info!(
            "Initializing indexer state from blocks in {}",
            startup_dir.display()
        );
        let mut state = IndexerState::new(
            root_hash.clone(),
            ledger.ledger,
            store,
            MAINNET_TRANSITION_FRONTIER_K,
            prune_interval,
            canonical_update_threshold,
        )?;

        let mut block_parser = BlockParser::new(&startup_dir, canonical_threshold)?;
        if is_genesis_ledger {
            state
                .initialize_with_contiguous_canonical(&mut block_parser)
                .await?;
        } else {
            state
                .initialize_without_contiguous_canonical(&mut block_parser)
                .await?;
        }

        phase_sender.send_replace(StateInitializedFromParser);
        state
    } else {
        info!("initializing indexer state from snapshot");
        let state = IndexerState::from_state_snapshot(
            store,
            MAINNET_TRANSITION_FRONTIER_K,
            prune_interval,
            canonical_update_threshold,
        )?;

        phase_sender.send_replace(StateInitializedFromSnapshot);
        state
    };
    let mut state_writer = loop {
        if let Ok(state_writer) = state_lock.try_write() {
            break state_writer;
        }
    };
    state_writer.replace(state);
    Ok(phase_sender)
}

#[instrument(skip_all)]
pub async fn run(
    block_watch_dir: impl AsRef<Path>,
    state: &RwLock<Option<IndexerState>>,
    phase_sender: watch::Sender<MinaIndexerRunPhase>,
    mut query_receiver: mpsc::Receiver<(
        MinaIndexerQuery,
        oneshot::Sender<MinaIndexerQueryResponse>,
    )>,
    mut save_rx: mpsc::Receiver<SaveCommand>,
    mut save_resp_tx: spmc::Sender<Option<SaveResponse>>,
) -> Result<(), anyhow::Error> {
    use MinaIndexerRunPhase::*;

    phase_sender.send_replace(StartingBlockReceiver);
    let mut filesystem_receiver = FilesystemReceiver::new(1024, 64).await?;
    filesystem_receiver.load_directory(block_watch_dir.as_ref())?;
    info!("Block receiver set to watch {:?}", block_watch_dir.as_ref());

    phase_sender.send_replace(StartingMainServerLoop);
    loop {
        tokio::select! {
            Some((command, response_sender)) = query_receiver.recv() => {
                let state_reader = loop {
                    if let Ok(state_reader) = state.try_read() {
                        break state_reader;
                    }
                };
                if let Some(state) = state_reader.as_ref() {
                    use MinaIndexerQuery::*;
                    let response = match command {
                        NumBlocksProcessed
                            => MinaIndexerQueryResponse::NumBlocksProcessed(state.blocks_processed),
                        BestTip => {
                            let best_tip = state.best_tip.clone();
                            MinaIndexerQueryResponse::BestTip(best_tip)
                        },
                        CanonicalTip => {
                            let canonical_tip = state.canonical_tip.clone();
                            MinaIndexerQueryResponse::CanonicalTip(canonical_tip)
                        },
                        Uptime
                            => MinaIndexerQueryResponse::Uptime(state.init_time.elapsed())
                    };
                    response_sender.send(response).unwrap();
                };
            }

            block_fut = filesystem_receiver.recv_block() => {
                let mut state_writer = loop {
                    if let Ok(state_writer) = state.try_write() {
                        break state_writer;
                    }
                };
                state_writer.as_mut().map(|state| {
                    phase_sender.send_replace(ReceivingBlock);
                    if let Some(precomputed_block) = block_fut? {
                        let block = BlockWithoutHeight::from_precomputed(&precomputed_block);
                        debug!("Receiving block {block:?}");

                        state.add_block(&precomputed_block)?;
                        info!("Added {block:?}");
                        Ok::<(), anyhow::Error>(())
                    } else {
                        info!("Block receiver shutdown, system exit");
                        Ok(())
                    }
                });
            }

            save_rx_fut = save_rx.recv() => {
                let mut state_writer = loop {
                    if let Ok(state_writer) = state.try_write() {
                        break state_writer;
                    }
                };
                state_writer.as_mut().map(|state| {
                    if let Some(SaveCommand(snapshot_path)) = save_rx_fut {
                        phase_sender.send_replace(SavingStateSnapshot);
                        trace!("saving snapshot in {}", &snapshot_path.display());
                        match state.save_snapshot(snapshot_path) {
                            Ok(_) => save_resp_tx.send(Some(SaveResponse("snapshot created".to_string())))?,
                            Err(e) => save_resp_tx.send(Some(SaveResponse(e.to_string())))?,
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                });
            }
        }
    }
}

pub async fn create_dir_if_non_existent(path: &str) {
    if metadata(path).await.is_err() {
        debug!("Creating directory {path}");
        create_dir_all(path).await.unwrap();
    }
}
