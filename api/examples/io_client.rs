use bytecheck::CheckBytes;
use ipiis_api::{client::IpiisClient, common::Ipiis};
use ipis::{
    class::Class,
    core::{
        anyhow::{bail, Result},
        signed::IsSigned,
    },
    env::Infer,
    tokio,
};
use ipsis_api::common::Ipsis;
use ipsis_common::KIND;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Class, Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug, PartialEq))]
pub struct MyData {
    name: String,
    age: u32,
}

impl IsSigned for MyData {}

#[tokio::main]
async fn main() -> Result<()> {
    // create a client
    let client = IpiisClient::infer().await;
    client
        .set_account_primary(KIND.as_ref(), &client.account_me().account_ref())
        .await?;
    client
        .set_address(
            KIND.as_ref(),
            &client.account_me().account_ref(),
            &"127.0.0.1:5001".parse()?,
        )
        .await?;

    // let's make a data we want to store
    let mut data = MyData {
        name: "Alice".to_string(),
        age: 24,
    };

    // CREATE
    let path_create = client.put(&data).await?;
    assert!(client.contains(&path_create).await?);

    // UPDATE (identity)
    let path_update_identity = client.put(&data).await?;
    assert_eq!(&path_create, &path_update_identity); // SAME Path

    // let's modify the data so that it has a different path
    data.name = "Bob".to_string();

    // UPDATE (changed)
    let path_update_changed = client.put(&data).await?;
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
        Err(_) => {
            assert!(!client.contains(&path_create).await?);
            assert!(!client.contains(&path_update_changed).await?);
            Ok(())
        }
    }
}
