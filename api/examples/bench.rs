use ipiis_api::{client::IpiisClient, common::Ipiis, server::IpiisServer};
use ipis::{
    core::{anyhow::Result, value::hash::Hash},
    env::Infer,
    path::Path,
    tokio::{self, io::AsyncReadExt},
};
use ipsis_api::{
    client::IpsisClient,
    common::{Ipsis, KIND},
    server::IpsisServer,
};
use ipsis_modules_gdown::IpsisGdown;

#[tokio::main]
async fn main() -> Result<()> {
    // define the hyperparameters
    const COUNT: u32 = 100;

    // deploy a server
    let server = IpsisServer::try_infer().await?;
    let server_account = {
        let server: &IpiisServer = server.as_ref();
        *server.account_ref()
    };
    tokio::spawn(async move { server.run().await });

    // create clients
    let client_local = IpsisClient::try_infer().await?;
    let client_remote = IpiisClient::try_infer().await?;
    client_remote
        .set_account_primary(KIND.as_ref(), &server_account)
        .await?;
    client_remote
        .set_address(KIND.as_ref(), &server_account, &"127.0.0.1:5001".parse()?)
        .await?;

    // download a model (deepset/roberta-base-squad2.onnx)
    // NOTE: you can generate manually from: "https://github.com/kerryeon/huggingface-onnx-tutorial.git"
    let id = "1gICu4NshBMQyUNgWsc2kydLBPpasIMNF";
    let path = Path {
        value: "bafybeie52ly6uafpr4h3ih24mqa4twtojppo6366kyi74ejtd4sxv2fezm".parse()?,
        len: 496_300_196,
    };
    println!("- Downloading data...");
    client_local.gdown_static(id, &path).await?;
    client_remote.gdown_static(id, &path).await?;
    println!("- Downloaded data");
    println!();

    // get canonical path
    #[cfg(feature = "s3")]
    let path_canonical = to_path_canonical(client_local.ipiis.account_ref(), &path);

    {
        let mut buf = Vec::with_capacity(path.len.try_into()?);

        // download a file via `s3`
        #[cfg(feature = "s3")]
        {
            let time_total = std::time::Instant::now();
            for _ in 0..COUNT {
                {
                    buf.clear();
                    bucket.get_object_stream(&path_canonical, &mut buf).await?;
                }
                assert_eq!(buf.len() as u64, path.len);
            }
            assert_eq!(Hash::with_bytes(&buf), path.value);
            println!(
                "- Average elapsed time for download via `s3`: {:?}",
                time_total.elapsed() / COUNT,
            );
        }

        // download a file via `s3 (parallel)`
        #[cfg(feature = "s3")]
        {
            let time_total = std::time::Instant::now();
            for _ in 0..COUNT {
                {
                    buf.clear();
                    bucket
                        .get_object_stream_parallel(&path_canonical, &mut buf)
                        .await?;
                }
                assert_eq!(buf.len() as u64, path.len);
            }
            assert_eq!(Hash::with_bytes(&buf), path.value);
            println!(
                "- Average elapsed time for download via `s3 (parallel)`: {:?}",
                time_total.elapsed() / COUNT,
            );
        }

        // download a file via `Ipsis (local)`
        let time_total = std::time::Instant::now();
        for _ in 0..COUNT {
            {
                buf.clear();
                let mut recv = client_local.get_raw(&path).await?;
                AsyncReadExt::read_to_end(&mut recv, &mut buf).await?;
            }
            assert_eq!((buf.len() - ::core::mem::size_of::<u64>()) as u64, path.len);
        }
        assert_eq!(Hash::with_bytes(&buf[8..]), path.value);
        println!(
            "- Average elapsed time for download via `Ipsis (local)`: {:?}",
            time_total.elapsed() / COUNT,
        );

        // download a file via `Ipsis (remote)`
        let time_total = std::time::Instant::now();
        for _ in 0..COUNT {
            {
                buf.clear();
                let mut recv = client_remote.get_raw(&path).await?;
                AsyncReadExt::read_to_end(&mut recv, &mut buf).await?;
            }
            assert_eq!((buf.len() - ::core::mem::size_of::<u64>()) as u64, path.len);
        }
        assert_eq!(Hash::with_bytes(&buf[8..]), path.value);
        println!(
            "- Average elapsed time for download via `Ipsis (remote)`: {:?}",
            time_total.elapsed() / COUNT,
        );
    }

    Ok(())
}

#[cfg(feature = "s3")]
fn to_path_canonical(account: &::ipis::core::account::AccountRef, path: &Path) -> String {
    format!("{}/{}", account.to_string(), path.value.to_string())
}
