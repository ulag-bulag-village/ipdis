use bytecheck::CheckBytes;
use ipdis_api::common::{ipiis_api::client::IpiisClient, Ipdis};
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
    // set environment variables
    ::std::env::set_var(
        "ipis_account_me",
        // NOTE: please hide it if you want to use it for production
        "32GLwJuG6igvTGtbXzjAG7iPMB4zoVY7jTZndR6kSdwSZiciLGozKKkTEhawcJKjzNcpLLLarmscB72m2M4u4sSw",
    );

    // create a client
    let client = IpiisClient::infer();
    client.add_address(
        "6p6fFPycxEFDwFNFF1gPPCoYRYGUtzrqFFavPis6dF61".parse()?,
        "127.0.0.1:5001".parse()?,
    )?;

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
