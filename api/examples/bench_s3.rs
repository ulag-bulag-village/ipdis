use ipiis_api::{client::IpiisClient, common::Ipiis, server::IpiisServer};
use ipis::{
    core::{account::AccountRef, anyhow::Result, value::hash::Hash},
    env::Infer,
    path::Path,
    tokio::{self, io::AsyncReadExt},
};
use ipsis_api::{
    client::IpsisClient,
    common::{Ipsis, KIND},
    server::IpsisServer,
};

#[cfg(feature = "s3")]
#[tokio::main]
async fn main() -> Result<()> {
    // define the hyperparameters
    const COUNT: u32 = 100;

    // deploy a server
    let server = IpsisServer::try_infer().await?;
    let server_account = {
        let server: &IpiisServer = server.as_ref();
        server.account_me().account_ref()
    };
    tokio::spawn(async move { server.run().await });

    // create clients
    let bucket = IpsisClient::new_bucket()?;
    let client_local = IpsisClient::try_infer().await?;
    let client_remote = IpiisClient::try_infer().await?;
    client_remote
        .set_account_primary(KIND.as_ref(), &server_account)
        .await?;
    client_remote
        .set_address(KIND.as_ref(), &server_account, &"127.0.0.1:5001".parse()?)
        .await?;

    // get canonical path
    let path = Path {
        value: "FjL3dTmyrudvLxFcezJ7b3oGq7Q48ZUS8HH5e4wajVL7".parse()?,
        len: 496_300_196,
    };
    let path_canonical = to_path_canonical(client_local.ipiis.account_me().account_ref(), &path);

    {
        let mut buf = Vec::with_capacity(path.len.try_into()?);

        // download a file via `s3`
        let time_total = std::time::Instant::now();
        for _ in 0..COUNT {
            let () = {
                buf.clear();
                bucket.get_object_stream(&path_canonical, &mut buf).await?;
            };
            assert_eq!(buf.len() as u64, path.len);
        }
        assert_eq!(Hash::with_bytes(&buf), path.value);
        println!(
            "- Average elapsed time for download via `s3`: {:?}",
            time_total.elapsed() / COUNT,
        );

        // download a file via `s3 (parallel)`
        let time_total = std::time::Instant::now();
        for _ in 0..COUNT {
            let () = {
                buf.clear();
                bucket
                    .get_object_stream_parallel(&path_canonical, &mut buf)
                    .await?;
            };
            assert_eq!(buf.len() as u64, path.len);
        }
        assert_eq!(Hash::with_bytes(&buf), path.value);
        println!(
            "- Average elapsed time for download via `s3 (parallel)`: {:?}",
            time_total.elapsed() / COUNT,
        );

        // download a file via `Ipsis (local)`
        let time_total = std::time::Instant::now();
        for _ in 0..COUNT {
            let () = {
                buf.clear();
                let mut recv = client_local.get_raw(&path).await?;
                AsyncReadExt::read_to_end(&mut recv, &mut buf).await?;
            };
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
            let () = {
                buf.clear();
                let mut recv = client_remote.get_raw(&path).await?;
                AsyncReadExt::read_to_end(&mut recv, &mut buf).await?;
            };
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

fn to_path_canonical(account: AccountRef, path: &Path) -> String {
    format!("{}/{}", account.to_string(), path.value.to_string())
}
