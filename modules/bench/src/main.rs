mod io;

use std::{sync::Arc, time::Instant};

use byte_unit::Byte;
use clap::Parser;
use ipiis_api::{client::IpiisClient, common::Ipiis};
use ipis::{
    core::{anyhow::Result, chrono::Utc, value::hash::Hash},
    env::Infer,
    futures,
    log::info,
    path::Path,
    tokio,
};
use ipsis_common::{Ipsis, KIND};
use rand::{distributions::Uniform, Rng};
use tokio::io::AsyncReadExt;

#[tokio::main]
async fn main() -> Result<()> {
    // init logger
    ::ipis::logger::init_once();

    // parse the command-line arguments
    let args = self::io::Args::parse();

    // log starting time
    let timestamp = Utc::now();
    info!("- Starting Time: {timestamp:?}");

    // create a client
    let client: Arc<_> = IpiisClient::try_infer().await?.into();

    // registre the server account as primary
    client
        .set_account_primary(KIND.as_ref(), &args.ipiis.account)
        .await?;
    client
        .set_address(KIND.as_ref(), &args.ipiis.account, &args.ipiis.address)
        .await?;

    // print the configuration
    info!("- Account: {}", args.ipiis.account.to_string());
    info!("- Address: {}", &args.ipiis.address);
    info!("- Data Size: {}", args.inputs.size);
    info!("- Number of Iteration: {}", args.inputs.iter);
    info!("- Number of Threads: {}", args.inputs.num_threads);

    let size_bytes: usize = args.inputs.size.get_bytes().try_into()?;
    let num_iteration: usize = args.inputs.iter.try_into()?;

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
        .collect();

    // begin benchmaring - Writing
    info!("- Benchmarking Writing ...");
    let duration_write = {
        let instant = Instant::now();
        futures::future::try_join_all(
            (0..args.inputs.num_threads)
                .map(|offset| (offset, client.clone(), dataset.clone(), data.clone()))
                .map(|(offset, client, dataset, data)| async move {
                    for (path, range) in dataset
                        .iter()
                        .skip(offset as usize)
                        .step_by(args.inputs.num_threads as usize)
                    {
                        let data = unsafe {
                            ::core::slice::from_raw_parts(
                                data.as_ptr().add(range.start),
                                size_bytes,
                            )
                        };
                        client.put_raw(path, data).await?;
                    }
                    Result::<_, ::ipis::core::anyhow::Error>::Ok(())
                }),
        )
        .await?;
        instant.elapsed()
    };

    // begin benchmaring - Reading
    info!("- Benchmarking Reading ...");
    let duration_read = {
        let instant = Instant::now();
        futures::future::try_join_all(
            (0..args.inputs.num_threads)
                .map(|offset| (offset, client.clone(), dataset.clone()))
                .map(|(offset, client, dataset)| async move {
                    for (path, _) in dataset
                        .iter()
                        .skip(offset as usize)
                        .step_by(args.inputs.num_threads as usize)
                    {
                        let mut recv = client.get_raw(path).await?;

                        let len = recv.read_u64().await?;
                        assert_eq!(len as usize, size_bytes);

                        tokio::io::copy(&mut recv, &mut tokio::io::sink()).await?;
                    }
                    Result::<_, ::ipis::core::anyhow::Error>::Ok(())
                }),
        )
        .await?;
        instant.elapsed()
    };

    // cleanup
    info!("- Cleaning Up ...");
    if args.inputs.clean {
        for (path, _) in dataset.iter() {
            client.delete(path).await?;
        }
    }

    // collect results
    info!("- Collecting results ...");
    let outputs = self::io::ResultsOutputs {
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
    };

    // save results to a file
    if let Some(mut save_dir) = args.inputs.save_dir.clone() {
        let protocol = client.protocol().await?;
        let timestamp = timestamp.to_rfc3339();
        let filename = format!("ipwis-{protocol}-{timestamp}.json");
        let filepath = {
            save_dir.push(filename);
            save_dir
        };

        info!("- Saving results to {filepath:?} ...");
        let results = self::io::Results {
            ipiis: self::io::ArgsIpiisPublic {
                account: args.ipiis.account.to_string(),
                address: args.ipiis.address,
            },
            inputs: args.inputs,
            outputs: outputs.clone(),
        };
        let file = ::std::fs::File::create(filepath)?;
        ::serde_json::to_writer(file, &results)?;
    }

    // print the output
    info!("- Finished!");
    info!("- Elapsed Time (Read): {:?}", outputs.read.elapsed_time_s);
    info!("- Elapsed Time (Write): {:?}", outputs.write.elapsed_time_s);
    info!("- IOPS (Read): {}", outputs.read.iops);
    info!("- IOPS (Write): {}", outputs.write.iops);
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

    Ok(())
}
