mod io;
mod protocol;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use byte_unit::Byte;
use clap::Parser;
use ipiis_modules_bench_simulation::Simulator;
use ipis::{
    core::{anyhow::Result, chrono::Utc, value::hash::Hash},
    futures,
    log::info,
    path::Path,
    tokio,
};
use rand::{distributions::Uniform, Rng};

#[tokio::main]
async fn main() -> Result<()> {
    // init logger
    ::ipis::logger::init_once();

    // parse the command-line arguments
    let args = self::io::Args::parse();

    // log starting time
    let timestamp = Utc::now();
    info!("- Starting Time: {timestamp:?}");

    // init protocol
    let protocol = self::protocol::select(&args).await?;
    let protocol_name = protocol.to_string().await?;

    // print the configuration
    info!("- Account: {}", args.ipiis.account.to_string());
    info!("- Address (Read): {}", &args.ipiis.address_read);
    info!("- Address (Write): {}", &args.ipiis.address_write);
    info!("- Data Size: {}", args.inputs.size);
    info!("- Number of Iteration: {}", args.inputs.iter);
    info!("- Number of Threads: {}", args.inputs.num_threads);
    info!("- Protocol: {protocol_name}");

    // compose simulation environment
    let mut simulator = Simulator::default();
    if let Some(delay) = args.simulation.network_delay_ms.map(Duration::from_millis) {
        if let Some(subnet) = args.simulation.network_delay_subnet {
            info!("- Simulation :: Network Delay: {delay:?}");
            info!("- Simulation :: Network Delay on Subnet: {subnet}");
            simulator.apply_network_delay(delay, subnet)?;
        }
    }

    let size_bytes: usize = args.inputs.size.get_bytes().try_into()?;
    let num_iteration: usize = args.inputs.iter.get_bytes().try_into()?;
    let num_threads: usize = args.inputs.num_threads.try_into()?;

    let simulation = args.simulation;

    // init data
    info!("- Initializing...");
    let range = Uniform::from(0..=255);
    let data: Arc<[_]> = ::rand::thread_rng()
        .sample_iter(&range)
        .take(size_bytes + num_iteration)
        .collect::<Vec<u8>>()
        .into();

    // construct dataset
    info!("- Generating Dataset ...");
    let dataset: Arc<[_]> = (0..num_iteration)
        .map(|iter| (iter..iter + size_bytes))
        .map(|range| {
            (
                Path {
                    value: Hash::with_bytes(&data[range.clone()]),
                    len: size_bytes
                        .try_into()
                        .expect("this size of dataset is unsupported in this architecture"),
                },
                range,
            )
        })
        .collect::<Vec<_>>()
        .into();

    // begin benchmaring - Writing
    let duration_write = {
        info!("- Benchmarking Writing ...");

        let instant = Instant::now();
        futures::future::try_join_all(
            (0..args.inputs.num_threads)
                .map(|offset| crate::protocol::BenchmarkCtx {
                    num_threads,
                    size_bytes,
                    simulation,

                    offset,
                    dataset: dataset.clone(),
                    data: data.clone(),
                })
                .map(|ctx| protocol.write(ctx)),
        )
        .await?;
        instant.elapsed()
    };

    // begin benchmaring - Reading
    let duration_read = {
        info!("- Benchmarking Reading ...");

        let instant = Instant::now();
        futures::future::try_join_all(
            (0..args.inputs.num_threads)
                .map(|offset| crate::protocol::BenchmarkCtx {
                    num_threads,
                    size_bytes,
                    simulation,

                    offset,
                    dataset: dataset.clone(),
                    data: data.clone(),
                })
                .map(|ctx| protocol.read(ctx)),
        )
        .await?;
        instant.elapsed()
    };

    // begin benchmaring - Cleanup
    let duration_cleanup = if !args.inputs.no_clean {
        info!("- Benchmarking Cleanup ...");

        let instant = Instant::now();
        futures::future::try_join_all(
            (0..args.inputs.num_threads)
                .map(|offset| crate::protocol::BenchmarkCtx {
                    num_threads,
                    size_bytes,
                    simulation,

                    offset,
                    dataset: dataset.clone(),
                    data: data.clone(),
                })
                .map(|ctx| protocol.cleanup(ctx)),
        )
        .await?;
        Some(instant.elapsed())
    } else {
        None
    };

    // collect results
    info!("- Collecting results ...");
    let outputs = self::io::ResultsOutputs {
        protocol: protocol_name.clone(),
        read: self::io::ResultsOutputsMetric {
            elapsed_time_s: duration_read.as_secs_f64(),
            iops: num_iteration as f64 / duration_read.as_secs_f64(),
            speed_bps: (8 * size_bytes * num_iteration) as f64 / duration_read.as_secs_f64(),
        },
        write: self::io::ResultsOutputsMetric {
            elapsed_time_s: duration_write.as_secs_f64(),
            iops: num_iteration as f64 / duration_write.as_secs_f64(),
            speed_bps: (8 * size_bytes * num_iteration) as f64 / duration_write.as_secs_f64(),
        },
        cleanup: duration_cleanup.map(|duration_cleanup| self::io::ResultsOutputsMetric {
            elapsed_time_s: duration_cleanup.as_secs_f64(),
            iops: num_iteration as f64 / duration_cleanup.as_secs_f64(),
            speed_bps: (8 * size_bytes * num_iteration) as f64 / duration_cleanup.as_secs_f64(),
        }),
    };

    // save results to a file
    if let Some(mut save_dir) = args.inputs.save_dir.clone() {
        let timestamp = timestamp.to_rfc3339();
        let filename = format!("benchmark-ipsis-{protocol_name}-{timestamp}.json");
        let filepath = {
            save_dir.push(filename);
            save_dir
        };

        info!("- Saving results to {filepath:?} ...");
        let results = self::io::Results {
            ipiis: self::io::ArgsIpiisPublic {
                account: args.ipiis.account.to_string(),
                address_read: args.ipiis.address_read,
                address_write: args.ipiis.address_write,
            },
            inputs: args.inputs,
            outputs: outputs.clone(),
            simulation,
        };
        let file = ::std::fs::File::create(filepath)?;
        ::serde_json::to_writer(file, &results)?;
    }

    // print the output
    info!("- Finished!");
    info!("- Elapsed Time (Read): {:?}", outputs.read.elapsed_time_s);
    info!("- Elapsed Time (Write): {:?}", outputs.write.elapsed_time_s);
    if let Some(cleanup) = outputs.cleanup.as_ref() {
        info!("- Elapsed Time (Cleanup): {:?}", cleanup.elapsed_time_s);
    }
    info!("- IOPS (Read): {}", outputs.read.iops);
    info!("- IOPS (Write): {}", outputs.write.iops);
    if let Some(cleanup) = outputs.cleanup.as_ref() {
        info!("- IOPS (Cleanup): {}", cleanup.iops);
    }
    info!("- Speed (Read): {}bps", {
        let mut speed = Byte::from_bytes(outputs.read.speed_bps as u128)
            .get_appropriate_unit(false)
            .to_string();
        speed.pop();
        speed
    });
    info!("- Speed (Write): {}bps", {
        let mut speed = Byte::from_bytes(outputs.write.speed_bps as u128)
            .get_appropriate_unit(false)
            .to_string();
        speed.pop();
        speed
    });
    if let Some(cleanup) = outputs.cleanup.as_ref() {
        info!("- Speed (Cleanup): {}bps", {
            let mut speed = Byte::from_bytes(cleanup.speed_bps as u128)
                .get_appropriate_unit(false)
                .to_string();
            speed.pop();
            speed
        });
    }

    Ok(())
}
