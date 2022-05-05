use bytecheck::CheckBytes;
use ipdis_api::{common::Ipdis, IpdisClient};
use ipis::{
    class::Class,
    core::anyhow::{bail, Result},
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

    let data = MyData {
        name: "Alice".to_string(),
        age: 42,
    };

    // PUT
    let path = client.put_permanent(&data).await?;

    // GET
    let data_from_storage: MyData = client.get(&path).await?;
    assert_eq!(&data, &data_from_storage);

    // DELETE
    let () = client.delete(&path).await?;

    // data is not exist after DELETE
    match client.get::<MyData>(&path).await {
        Ok(_) => bail!("data not deleted!"),
        Err(_) => Ok(()),
    }
}
