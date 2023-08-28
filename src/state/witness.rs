use id_tree::NodeId;
use tracing::{instrument, trace};

use crate::block::{BlockHash, Block};

use super::{branch::Branch, Tip, ledger::diff::LedgerDiff, ExtensionType};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WitnessConfig {
    transition_frontier_k: usize,
    canonical_update_threshold: usize,
    prune_interval: usize,
}

#[derive(Debug)]
pub struct Witness {
    best_tip: Tip,
    canonical_tip: Tip,
    root_branch: Branch,
    dangling_branches: Vec<Branch>,
    config: WitnessConfig
}

impl Witness {
    pub fn new(
        root_hash: BlockHash,
        config: WitnessConfig
    ) -> Self {
        let root_branch = Branch::new_genesis(root_hash);
        let best_tip = root_branch.best_tip();
        let canonical_tip = if let Some(tip) = root_branch
            .canonical_tip(config.canonical_update_threshold) 
        { tip } else { best_tip.clone() };
        let dangling_branches = vec![];
        Witness {
            best_tip, canonical_tip, 
            root_branch, dangling_branches,
            config
        }
    }

    #[instrument(skip(self))]
    pub fn add_block(&mut self, block: Block) -> ExtensionType {
        use ExtensionType::*;

        // determine blockchain lengths for relevant blocks
        let root_block_length = self.root_branch
            .root_block().blockchain_length.unwrap_or(0);
        let best_tip_length = self.root_branch.branches.get(&self.best_tip.node_id)
            .expect("best tip always exists").data().blockchain_length.unwrap_or(0);
        let new_block_length = block.blockchain_length
            .unwrap_or(u32::MAX);

        // adding a block below the root of the witness tree is not supported
        if root_block_length >= new_block_length {
            trace!("new block is below the root!");
            return BlockNotAdded;
        }

        if new_block_length <= best_tip_length + 1 {
            // the new block is within the witness tree's root branch
            if let Some(new_node_id) = self.root_branch.extension(block) {
                self.best_tip = self.root_branch.best_tip();
                if self.try_merge_dangling(new_node_id) {
                    RootComplex
                } else {
                    RootSimple
                }
            } else {
                // this indicates an uncaught LRF
                panic!("uncaught long range fork!");
            }
        } else {
            // TODO! the new block is not within the witness tree's root branch

            // TODO: try extension on each dangling branch
            for (branch_idx, branch) in self.dangling_branches
                .iter_mut().enumerate()
            {
                // determine blockchain lengths for relevant blocks
                let dangling_root_length = branch
                    .root_block().blockchain_length.unwrap_or(0);
                let dangling_tip_length = branch
                    .best_tip_block().unwrap().blockchain_length.unwrap_or(0);

                if new_block_length == dangling_root_length - 1 {
                    // reverse extension
                    let new_node_id = branch.reroot(block);
                    todo!();
                } else
                if dangling_root_length < new_block_length && dangling_tip_length + 1 >= new_block_length {
                    // forward extension
                    if let Some(node_id) = branch.extension(block) {

                    } else {
                        panic!("uncaught long range fork!");
                    }
                    todo!();
                } else {
                    // create new dangling branch
                    todo!();
                }

            }
            todo!();

            // TODO: if an extension was performed, check for merge with other dangling branches
            // don't check root branch, as if the root branch would have connected to a dangling branch,
            // it would have been a root extension, and this block would be skipped
            todo!()
        }
    }

    pub fn try_merge_dangling(&mut self, new_node_id: NodeId) -> bool {
        let mut to_remove_idxs = vec![];
        for (index, branch) in self.dangling_branches
            .iter_mut().enumerate() 
        {
            let new_state_hash = &self.root_branch.branches
                .get(&new_node_id).expect("new_node_id is valid")
                .data().state_hash;
            if new_state_hash == &branch.root_block().state_hash {
                self.root_branch
                    .merge_on(&new_node_id, branch);
                to_remove_idxs.push(index);
            }
        }

        if !to_remove_idxs.is_empty() {
            self.best_tip = self.root_branch.best_tip();
            for (num_removed, idx_to_remove) in to_remove_idxs.iter().enumerate() {
                self.dangling_branches.remove(idx_to_remove - num_removed);
            } true
        } else { false }
    }
}