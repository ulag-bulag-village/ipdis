#![feature(more_qualified_paths)]

pub extern crate ipiis_api;

use bytecheck::CheckBytes;
use ipiis_api::{
    client::IpiisClient,
    common::{external_call, opcode::Opcode, Ipiis, Serializer},
};
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
pub trait Ipdis {
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

    async fn put<Req>(&self, msg: &Req, expiration_date: DateTime) -> Result<Path>
    where
        Req: Serialize<Serializer> + Send + Sync;

    async fn put_permanent<Req>(&self, msg: &Req) -> Result<Path>
    where
        Req: Serialize<Serializer> + Send + Sync;

    async fn delete(&self, path: &Path) -> Result<()>;
}

#[async_trait]
impl Ipdis for IpiisClient {
    async fn get_raw(&self, path: &Path) -> Result<Vec<u8>> {
        // next target
        let target = self.account_primary()?;

        // pack request
        let req = RequestType::Get { path: *path };

        // external call
        let (data,) = external_call!(
            account: self.account_me().account_ref(),
            call: self
                .call_permanent_deserialized(Opcode::TEXT, &target, req)
                .await?,
            response: Response => Get,
            items: { data },
        );

        // unpack response
        Ok(data)
    }

    async fn put<Req>(&self, msg: &Req, expiration_date: DateTime) -> Result<Path>
    where
        Req: Serialize<Serializer> + Send + Sync,
    {
        // next target
        let target = self.account_primary()?;

        // pack request
        let req = RequestType::Put {
            data: ::rkyv::to_bytes(msg)?.to_vec(),
        };

        // sign request
        let req = Metadata::builder().expiration_date(expiration_date).build(
            self.account_me(),
            target,
            req,
        )?;

        // external call
        let (path,) = external_call!(
            account: self.account_me().account_ref(),
            call: self
                .call_deserialized(Opcode::TEXT, &target, req)
                .await?,
            response: Response => Put,
            items: { path },
        );

        // unpack response
        Ok(path)
    }

    async fn put_permanent<Req>(&self, msg: &Req) -> Result<Path>
    where
        Req: Serialize<Serializer> + Send + Sync,
    {
        // next target
        let target = self.account_primary()?;

        // pack request
        let req = RequestType::Put {
            data: ::rkyv::to_bytes(msg)?.to_vec(),
        };

        // external call
        let (path,) = external_call!(
            account: self.account_me().account_ref(),
            call: self
                .call_permanent_deserialized(Opcode::TEXT, &target, req)
                .await?,
            response: Response => Put,
            items: { path },
        );

        // unpack response
        Ok(path)
    }

    async fn delete(&self, path: &Path) -> Result<()> {
        // next target
        let target = self.account_primary()?;

        // pack request
        let req = RequestType::Delete { path: *path };

        // external call
        let () = external_call!(
            account: self.account_me().account_ref(),
            call: self
                .call_permanent_deserialized(Opcode::TEXT, &target, req)
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
    Delete { path: Path },
}

#[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug, PartialEq))]
pub enum Response {
    Get { data: Vec<u8> },
    Put { path: Path },
    Delete,
}
