use bytecheck::CheckBytes;
use ipiis_api::{
    client::IpiisClient,
    common::{opcode::Opcode, Ipiis, Serializer},
};
use ipis::{
    async_trait::async_trait,
    class::Class,
    core::{anyhow::Result, value::hash::Hash},
    pin::Pinned,
};
use rkyv::{validation::validators::DefaultValidator, Archive, Deserialize, Infallible, Serialize};

#[async_trait]
pub trait Ipdis {
    async fn get<Res>(&self, hash: &Hash) -> Result<Pinned<Res>>
    where
        Res: Class + Archive + Send,
        <Res as Archive>::Archived:
            for<'a> CheckBytes<DefaultValidator<'a>> + Deserialize<Res, Infallible>;

    async fn put<Req>(&self, msg: &Req) -> Result<Hash>
    where
        Req: Serialize<Serializer> + Send + Sync;
}

#[async_trait]
impl Ipdis for IpiisClient {
    async fn get<Res>(&self, hash: &Hash) -> Result<Pinned<Res>>
    where
        Res: Class + Archive + Send,
        <Res as Archive>::Archived:
            for<'a> CheckBytes<DefaultValidator<'a>> + Deserialize<Res, Infallible>,
    {
        // next target
        let target = self.account_primary()?;

        // pack request
        let req = Request::Get { hash: *hash };

        // external call
        self.call(Opcode::TEXT, &target, &req).await
    }

    async fn put<Req>(&self, msg: &Req) -> Result<Hash>
    where
        Req: Serialize<Serializer> + Send + Sync,
    {
        // next target
        let target = self.account_primary()?;

        // pack request
        let req = Request::Put {
            data: ::rkyv::to_bytes(msg)?.to_vec(),
        };

        // external call
        self.call_deserialized(Opcode::TEXT, &target, &req).await
    }
}

#[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(Debug, PartialEq))]
pub enum Request {
    Get { hash: Hash },
    Put { data: Vec<u8> },
}
