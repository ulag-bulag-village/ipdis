#![feature(more_qualified_paths)]

use bytecheck::CheckBytes;
use ipiis_common::{external_call, Ipiis, Serializer};
use ipis::{
    async_trait::async_trait,
    class::Class,
    core::{
        account::GuaranteeSigned, anyhow::Result, metadata::Metadata,
        signature::SignatureSerializer, value::chrono::DateTime,
    },
    path::Path,
    pin::PinnedInner,
};
use rkyv::{
    de::deserializers::SharedDeserializeMap, validation::validators::DefaultValidator, Archive,
    Deserialize, Serialize,
};

#[async_trait]
pub trait Ipsis {
    async fn get<Res>(&self, path: &Path) -> Result<Res>
    where
        Res: Class
            + Archive
            + Serialize<SignatureSerializer>
            + ::core::fmt::Debug
            + PartialEq
            + Send,
        <Res as Archive>::Archived: for<'a> CheckBytes<DefaultValidator<'a>>
            + Deserialize<Res, SharedDeserializeMap>
            + ::core::fmt::Debug
            + PartialEq
            + Send,
    {
        {
            self.get_raw(path)
                .await
                .and_then(PinnedInner::deserialize_owned)
        }
    }

    async fn get_raw(&self, path: &Path) -> Result<Vec<u8>>;

    async fn put<Req>(&self, data: &Req, expiration_date: Option<DateTime>) -> Result<Path>
    where
        Req: Serialize<Serializer> + Send + Sync,
    {
        let data = ::rkyv::to_bytes(data)?.to_vec();

        self.put_raw(data, expiration_date).await
    }

    async fn put_raw(&self, data: Vec<u8>, expiration_date: Option<DateTime>) -> Result<Path>;

    async fn contains(&self, path: &Path) -> Result<bool>;

    async fn delete(&self, path: &Path) -> Result<()>;
}

#[async_trait]
impl<IpiisClient> Ipsis for IpiisClient
where
    IpiisClient: Ipiis + Send + Sync,
{
    async fn get_raw(&self, path: &Path) -> Result<Vec<u8>> {
        // next target
        let target = self.get_account_primary(KIND.as_ref()).await?;

        // pack request
        let req = RequestType::Get { path: *path };

        // external call
        let (data,) = external_call!(
            call: self
                .call_permanent_deserialized(&target, req)
                .await?,
            response: Response => Get,
            items: { data },
        );

        // unpack response
        Ok(data)
    }

    async fn put_raw(&self, data: Vec<u8>, expiration_date: Option<DateTime>) -> Result<Path> {
        // next target
        let target = self.get_account_primary(KIND.as_ref()).await?;

        // pack request
        let req = RequestType::Put { data };

        // sign request
        let req = {
            let mut builder = Metadata::builder();

            if let Some(expiration_date) = expiration_date {
                builder = builder.expiration_date(expiration_date);
            }

            builder.build(self.account_me(), target, req)?
        };

        // external call
        let (path,) = external_call!(
            call: self
                .call_deserialized(&target, req)
                .await?,
            response: Response => Put,
            items: { path },
        );

        // unpack response
        Ok(path)
    }

    async fn contains(&self, path: &Path) -> Result<bool> {
        // next target
        let target = self.get_account_primary(KIND.as_ref()).await?;

        // pack request
        let req = RequestType::Contains { path: *path };

        // external call
        let (contains,) = external_call!(
            call: self
                .call_permanent_deserialized(&target, req)
                .await?,
            response: Response => Contains,
            items: { contains },
        );

        // unpack response
        Ok(contains)
    }

    async fn delete(&self, path: &Path) -> Result<()> {
        // next target
        let target = self.get_account_primary(KIND.as_ref()).await?;

        // pack request
        let req = RequestType::Delete { path: *path };

        // external call
        let () = external_call!(
            call: self
                .call_permanent_deserialized(&target, req)
                .await?,
            response: Response => Delete,
        );

        // unpack response
        Ok(())
    }
}

pub type Request = GuaranteeSigned<RequestType>;

#[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug, PartialEq))]
pub enum RequestType {
    Get { path: Path },
    Put { data: Vec<u8> },
    Contains { path: Path },
    Delete { path: Path },
}

#[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug, PartialEq))]
pub enum Response {
    Get { data: Vec<u8> },
    Put { path: Path },
    Contains { contains: bool },
    Delete,
}

::ipis::lazy_static::lazy_static! {
    pub static ref KIND: Option<::ipis::core::value::hash::Hash> = Some(
        ::ipis::core::value::hash::Hash::with_str("__ipis__ipsis__"),
    );
}
