use std::{net::SocketAddr, path::PathBuf};

use byte_unit::Byte;
use clap::{Parser, ValueEnum};
use ipis::core::account::AccountRef;
use serde::{Deserialize, Serialize};

#[derive(Debug, Parser)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(flatten)]
    pub ipiis: ArgsIpiis,
    #[clap(flatten)]
    pub inputs: ArgsInputs,
}

#[derive(Debug, Parser)]
pub struct ArgsIpiis {
    /// Account of the target server
    #[clap(long, env = "ipiis_client_account_primary")]
    pub account: AccountRef,

    /// Address of the target server
    #[clap(long, env = "ipiis_client_account_primary_address")]
    pub address: SocketAddr,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Parser)]
pub struct ArgsInputs {
    /// Protocol of benchmarking stream
    #[clap(value_enum)]
    #[clap(short, long, env = "PROTOCOL", default_value_t = ArgsProtocol::Ipiis)]
    pub protocol: ArgsProtocol,

    /// Size of benchmarking stream
    #[clap(short, long, env = "DATA_SIZE", default_value_t = Byte::from_bytes(64_000_000))]
    pub size: Byte,

    /// Number of iteration
    #[clap(short, long, env = "NUM_ITERATIONS", default_value_t = 30)]
    pub iter: u32,

    /// Number of threads
    #[clap(long, env = "NUM_THREADS", default_value_t = 1)]
    pub num_threads: u32,

    /// Whether to cleanup all testing data
    #[clap(long, env = "CLEAN", default_value_t = true)]
    pub clean: bool,

    /// Directory to save the results (filename is hashed by protocol and starting time)
    #[clap(long, env = "SAVE_DIR")]
    pub save_dir: Option<PathBuf>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum ArgsProtocol {
    #[cfg(feature = "ipiis")]
    Ipiis,
    #[cfg(feature = "ipfs")]
    Ipfs,
    #[cfg(feature = "local")]
    Local,
    #[cfg(feature = "s3")]
    S3,
}
