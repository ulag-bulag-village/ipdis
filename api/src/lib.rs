use ipdis_common::{
    ipiis_api::{client::IpiisClient, common::Serializer},
    Ipdis,
};
use ipis::{
    async_trait::async_trait,
    bytecheck::CheckBytes,
    class::Class,
    core::{
        account::GuaranteeSigned, anyhow::Result, signature::SignatureSerializer, value::hash::Hash,
    },
    path::Path,
    pin::Pinned,
    rkyv::{
        de::deserializers::SharedDeserializeMap, validation::validators::DefaultValidator, Archive,
        Deserialize, Serialize,
    },
};

pub struct IpdisClient {
    ipiis: IpiisClient,
}

impl AsRef<IpiisClient> for IpdisClient {
    fn as_ref(&self) -> &IpiisClient {
        &self.ipiis
    }
}

#[async_trait]
impl Ipdis for IpdisClient {
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
        todo!()
    }

    async fn put_permanent<Req>(&self, msg: &Req) -> Result<GuaranteeSigned<Hash>>
    where
        Req: Serialize<Serializer> + Send + Sync,
    {
        todo!()
    }

    async fn delete(&self, path: &Path) -> Result<GuaranteeSigned<Hash>> {
        todo!()
    }
}
