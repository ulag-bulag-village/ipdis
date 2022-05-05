pub extern crate ipiis_api;

use bytecheck::CheckBytes;
use ipiis_api::{
    client::IpiisClient,
    common::{opcode::Opcode, Ipiis, Serializer},
};
use ipis::{
    async_trait::async_trait,
    class::Class,
    core::{
        account::GuaranteeSigned, anyhow::Result, signature::SignatureSerializer, value::hash::Hash,
    },
    path::Path,
    pin::Pinned,
};
use rkyv::{
    de::deserializers::SharedDeserializeMap, validation::validators::DefaultValidator, Archive,
    Deserialize, Serialize,
};

#[async_trait]
pub trait Ipdis {
    async fn get_permanent<Res>(&self, path: &Path) -> Result<Pinned<GuaranteeSigned<Res>>>
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
            + Send;

    async fn put_permanent<Req>(&self, msg: &Req) -> Result<GuaranteeSigned<Hash>>
    where
        Req: Serialize<Serializer> + Send + Sync;

    async fn delete(&self, path: &Path) -> Result<GuaranteeSigned<Hash>>;
}

#[async_trait]
impl Ipdis for IpiisClient {
    async fn get_permanent<Res>(&self, path: &Path) -> Result<Pinned<GuaranteeSigned<Res>>>
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
        // next target
        let target = self.account_primary()?;

        // pack request
        let req = RequestType::Get { path: *path };

        // external call
        self.call_permanent(Opcode::TEXT, &target, req).await
    }

    async fn put_permanent<Req>(&self, msg: &Req) -> Result<GuaranteeSigned<Hash>>
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
        self.call_permanent_deserialized(Opcode::TEXT, &target, req)
            .await
    }

    async fn delete(&self, path: &Path) -> Result<GuaranteeSigned<Hash>> {
        // next target
        let target = self.account_primary()?;

        // pack request
        let req = RequestType::Delete { path: *path };

        // external call
        self.call_permanent_deserialized(Opcode::TEXT, &target, req)
            .await
    }
}

pub type Request = GuaranteeSigned<RequestType>;

#[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(Debug, PartialEq))]
pub enum RequestType {
    Get { path: Path },
    Put { data: Vec<u8> },
    Delete { path: Path },
}
