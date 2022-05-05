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
        account::{GuaranteeSigned, Verifier},
        anyhow::Result,
        metadata::Metadata,
        signature::SignatureSerializer,
        value::chrono::DateTime,
    },
    path::Path,
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
            + Send;

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
        // next target
        let target = self.account_primary()?;

        // pack request
        let req = RequestType::Get { path: *path };

        // external call
        let res: GuaranteeSigned<Res> = self
            .call_permanent_deserialized(Opcode::TEXT, &target, req)
            .await?;

        // verify response
        let () = res.verify(Some(self.account_me().account_ref()))?;

        Ok(res.data.data)
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
        let res: GuaranteeSigned<Path> = self
            .call_permanent_deserialized(Opcode::TEXT, &target, req)
            .await?;

        // verify response
        let () = res.verify(Some(self.account_me().account_ref()))?;

        Ok(res.data.data)
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
        let res: GuaranteeSigned<Path> = self
            .call_permanent_deserialized(Opcode::TEXT, &target, req)
            .await?;

        // verify response
        let () = res.verify(Some(self.account_me().account_ref()))?;

        Ok(res.data.data)
    }

    async fn delete(&self, path: &Path) -> Result<()> {
        // next target
        let target = self.account_primary()?;

        // pack request
        let req = RequestType::Delete { path: *path };

        // external call
        let res: GuaranteeSigned<()> = self
            .call_permanent_deserialized(Opcode::TEXT, &target, req)
            .await?;

        // verify response
        let () = res.verify(Some(self.account_me().account_ref()))?;

        Ok(())
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
