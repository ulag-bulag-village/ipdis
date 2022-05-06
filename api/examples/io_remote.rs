use bytecheck::CheckBytes;
use ipdis_api::{
    common::{ipiis_api::client::IpiisClient, Ipdis},
    server::IpdisServer,
};
use ipis::{
    class::Class,
    core::anyhow::{bail, Result},
    env::Infer,
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
    let server = IpdisServer::infer()?;
    tokio::spawn(async move { server.run().await });

    // create a client
    let client = IpiisClient::infer()?;

    // let's make a data we want to store
    let mut data = MyData {
        name: "Alice".to_string(),
        age: 42,
    };

    // CREATE
    let path_create = client.put_permanent(&data).await?;

    // UPDATE (identity)
    let path_update_identity = client.put_permanent(&data).await?;
    assert_eq!(&path_create, &path_update_identity); // SAME Path

    // let's modify the data so that it has a different path
    data.name = "Bob".to_string();

    // UPDATE (changed)
    let path_update_changed = client.put_permanent(&data).await?;
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
