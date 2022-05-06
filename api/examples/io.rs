use bytecheck::CheckBytes;
use ipdis_api::{client::IpdisClient, common::Ipdis};
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
    let client = IpdisClient::infer()?;

    let mut data = MyData {
        name: "Alice".to_string(),
        age: 42,
    };

    // CREATE
    let path_create = client.put_permanent(&data).await?;

    // UPDATE (identity)
    let path_update_identity = client.put_permanent(&data).await?;
    assert_eq!(&path_create, &path_update_identity); // SAME Path

    // UPDATE (changed)
    data.name = "Bob".to_string();

    let path_update_changed = client.put_permanent(&data).await?;
    assert_ne!(&path_create, &path_update_changed); // CHANGED Path

    let path = path_update_changed;

    // READ
    let data_from_storage: MyData = client.get(&path).await?;
    assert_eq!(&data, &data_from_storage);

    // DELETE
    let () = client.delete(&path_create).await?;
    let () = client.delete(&path).await?;

    // data is not exist after DELETE
    match client.get::<MyData>(&path).await {
        Ok(_) => bail!("data not deleted!"),
        Err(_) => Ok(()),
    }
}
