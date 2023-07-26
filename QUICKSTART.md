# Mina Indexer Quickstart Guide

The Mina Indexer is a new software product created by Granola, Inc. and the Mina Foundation that is designed to address the flaws with the currently used Archive Node, in order to make storing and accessing the history of the Mina Blockchain far easier, as well as encouraging decentralization in this storage. The project consists of a main binary, written in rust, that imports blocks created on the chain in Mina's "Precomputed" format to build up a view into the state of the chain; as well as several peripheral binaries of varying importance, also written in rust, built around various use cases in the ecosystem. This QuickStart guide is designed to help less technical users, as well as users who are new to the ecosystem, to get up and running with the Mina Indexer to follow Mina's Mainnet Network in a continually updating manner. This guide was written against a new Debian 12 virtual machine, and can be reasonably guaranteed to work in that context, although it is also likely to work on other Debian based Linux distributions such as Ubuntu, Pop OS, among others. Thanks for taking an interest in our project and Happy Indexing <3

## OS Requirements and Native Setup

The Mina Indexer is packaged with Nix Flakes, which are designed to be system independent and reproducible on a bitwise scale, however, this guide is tested against Debian 12 and cannot be guaranteed to work on other machines, though testing and input would be welcome. If you run into an issue, feel free to raise an issue in this repository and the team will take a look!

### Install Debian 12 on a Virtual Machine or Local Hardware, update the software, install the Nix Package Manager, and create an unprivileged user.
   - Installation of the Debian Operating System is outside the scope of this guide, though guidance can be found at debian.org/support
   - As the `root` user, update your software to the latest version using `apt update && apt dist-upgrade`, following all prompts and restarting your machine, if prompted.
   - Install the Nix Package Manager by navigating to nixos.org, clicking the green "Download" button, and running the command line for "Multi User Installation" (`sh <(curl -L https://nixos.org/nix/install) --daemon`). This command will ask for `sudo` permissions if being run by an previously created unprivileged user, which is expected behavior.
   - Enable the Flakes feature of the Nix Package Manager by running the following command: `mkdir -p ~/.config/nix && echo "experimental-features = nix-command flakes" >> ~/.config/nix/nix.conf`. While it may be alarming that Flakes are considered an "experimental feature," they are well tested and can be expected not to produce any undefined behavior within the parameters of the Mina Indexer.
   - If not already done, it is reccommended to create an unprivileged user for the indexer. It may make setup easier if the user is added to the `sudo` group, though `sudo` permissions are not necessary for normal operation of the Mina Indexer

## `mina-indexer` Binary Setup

The main binary for the Mina Indexer is packaged under the repository https://github.com/Granola-Team/mina-indexer, and implements the main indexing functionality of consuming individual Precomputed Blocks, adding them to an embedded database, and tracking the best chain within the transition frontier.

### Download and Build the Mina Indexer Binary, and Install it to your System's PATH.
  - Clone the `mina-indexer` repository by running `git clone https://github.com/Granola-Team/mina-indexer.git` in a directory of your choosing
  - Enter the cloned directory and update the repository's submodules by running `git submodule update --init --recursive --remote`
  - Build the `mina-indexer` binary by running `nix build '.?submodules=1'` in the root of the repository.
  - Add the built binaries to your path by either adding the following to your `~/.bashrc` or simply executing it at the command line: `export PATH=$PATH:<indexer repo dir>/result/bin` where `<indexer repo dir>` is the directory to which you have cloned our repository.
  - Test the installation by running `mina-indexer --help`

## `iggy` Binary Setup

`iggy` is a peripheral binary to the Mina Indexer that is used to keep the Indexer up to date with Mina's `mainnet` network. We have future plans to expand this tool to also follow the `berkeley` and `testnet` networks, as well as to integrate the tool into the main `mina-indexer` binary to reduce the setup overhead required by it.

### Download and Build the `iggy` Binary, and Install it to your System's PATH.
  - Install the Google Cloud SDK using Nix: `nix-env -iA nixpkgs.google-cloud-sdk`
  - Clone Isaac DeFrain's `fn` repository with `git clone https://github.com/Isaac-DeFrain/fn.git`
  - Enter the cloned directory, then enter the subdirectory `mina-indexer-block-util`.
  - Build the binary with the following command: `nix-shell -p cargo rustc --command cargo build --release`
  - Add the built binary to your system's PATH by adding the following to your `~/.bashrc` or executing it at the command line: `export PATH=$PATH:<isaac's fn repo dir>/target/release`
  - Test proper installation by running `mina-indexer-block-util --help`

## Mainnet Block Download

To get a view into the history of the Mina Blockchain, you will need to download the Precomputed Blocks that have been generated throughout the Chain's history and stored in a Google Cloud Bucket hosted by O(1) Labs. The current size for all of these blocks is on the order of 300 gigabytes, so we reccomend using a disk or OS volume with at least 500 gigabytes, and preferentially, one terabyte, to prepare for the future. In the future, we imagine a setup where different decentralized users become trusted "historians" of certain segments of the chain, so no user is required to store the entire chain on their machine, though knowledge of the entire chain is unnecessary, and only becoming a "witness" starting at a specific point and indexing thereafter will suffice for most practical use cases (ZKApps, Governance Utilities, etc.)

### Use Either `iggy`'s `continuous` Mode or the `gsutil` Binary to Download Precomputed Blocks from O(1)'s Bucket to your Machine
  - Using `gsutil`, run the following command, given a block storage directory `<block-storage-dir>`: `gsutil -m cp -n "gs://mina_network_block_data/mainnet-*.json" <block-storage-dir>`
  - Alternatively, using `iggy`: `mina-indexer-block-util `

TODO!!!
