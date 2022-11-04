use std::path::PathBuf;

use byte_unit::Byte;
use clap::{Parser, ValueEnum};
use ipiis_modules_bench_simulation::ipnet::IpNet;
use ipis::core::account::AccountRef;
use serde::{Deserialize, Serialize};

#[derive(Debug, Parser)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(flatten)]
    pub ipiis: ArgsIpiis,
    #[clap(flatten)]
    pub inputs: ArgsInputs,
    #[clap(flatten)]
    pub simulation: ArgsSimulation,
}

#[derive(Debug, Parser)]
pub struct ArgsIpiis {
    /// Account of the target server
    #[clap(long, env = "ipiis_client_account_primary")]
    pub account: AccountRef,

    /// Address of the target server for reading
    #[clap(long, env = "ipiis_client_account_primary_address_read")]
    pub address_read: String,

    /// Address of the target server for writing
    #[clap(long, env = "ipiis_client_account_primary_address_write")]
    pub address_write: String,
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
    #[clap(short, long, env = "NUM_ITERATIONS", default_value_t = Byte::from_bytes(30))]
    pub iter: Byte,

    /// Number of threads
    #[clap(long, env = "NUM_THREADS", default_value_t = 1)]
    pub num_threads: u32,

    /// Whether not to cleanup all testing data
    #[clap(long, env = "NO_CLEAN")]
    pub no_clean: bool,

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

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Parser)]
pub struct ArgsSimulation {
    /// Manual network delay in milliseconds
    #[clap(long, env = "SIMULATION_NETWORK_DELAY_MS")]
    pub network_delay_ms: Option<u64>,

    /// Manual network delay subnet
    #[clap(long, env = "SIMULATION_NETWORK_DELAY_SUBNET")]
    pub network_delay_subnet: Option<IpNet>,
}
