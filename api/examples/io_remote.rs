use bytecheck::CheckBytes;
use ipiis_api::{client::IpiisClient, common::Ipiis, server::IpiisServer};
use ipis::{
    class::Class,
    core::anyhow::{bail, Result},
    env::Infer,
    tokio,
};
use ipsis_api::{
    common::{Ipsis, KIND},
    server::IpsisServer,
};
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Class, Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug, PartialEq))]
pub struct MyData {
    name: String,
    age: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    // deploy a server
    let server = IpsisServer::genesis(5001).await?;
    let server_account = {
        let server: &IpiisServer = server.as_ref();
        server.account_me().account_ref()
    };
    tokio::spawn(async move { server.run().await });

    // create a client
    let client = IpiisClient::genesis(None).await?;
    client
        .set_account_primary(KIND.as_ref(), &server_account)
        .await?;
    client
        .set_address(&server_account, &"127.0.0.1:5001".parse()?)
        .await?;

    // let's make a data we want to store
    let mut data = MyData {
        name: "Alice".to_string(),
        age: 24,
    };

    // CREATE
    let path_create = client.put(&data, None).await?;

    // UPDATE (identity)
    let path_update_identity = client.put(&data, None).await?;
    assert_eq!(&path_create, &path_update_identity); // SAME Path

    // let's modify the data so that it has a different path
    data.name = "Bob".to_string();

    // UPDATE (changed)
    let path_update_changed = client.put(&data, None).await?;
    assert_ne!(&path_create, &path_update_changed); // CHANGED Path

    // READ
    let data_from_storage: MyData = client.get(&path_update_changed).await?;
    assert_eq!(&data, &data_from_storage);

    // DELETE
    let () = client.delete(&path_create).await?;
    let () = client.delete(&path_update_changed).await?;

    // data is not exist after DELETE
    match client.get::<MyData>(&path_update_changed).await {
        Ok(_) => bail!("data not deleted!"),
        Err(_) => Ok(()),
    }
}
