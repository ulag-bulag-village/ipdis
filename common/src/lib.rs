use std::io::Cursor;

use bytecheck::CheckBytes;
use ipiis_common::{define_io, external_call, Ipiis, ServerResult};
use ipis::{
    async_trait::async_trait,
    class::Class,
    core::{
        account::{GuaranteeSigned, GuarantorSigned, Verifier},
        anyhow::Result,
        signature::SignatureSerializer,
        signed::{IsSigned, Serializer},
        value::hash::Hash,
    },
    futures::TryFutureExt,
    path::Path,
    stream::DynStream,
    tokio::io::AsyncRead,
};
use rkyv::{
    de::deserializers::SharedDeserializeMap, validation::validators::DefaultValidator, Archive,
    Deserialize, Serialize,
};

#[async_trait]
pub trait Ipsis {
    type Reader: AsyncRead + Send + Unpin + 'static;

    async fn get<Res>(&self, path: &Path) -> Result<Res>
    where
        Res: Class
            + Archive
            + Serialize<SignatureSerializer>
            + Serialize<Serializer>
            + IsSigned
            + Clone
            + ::core::fmt::Debug
            + PartialEq
            + Send
            + Sync
            + 'static,
        <Res as Archive>::Archived: for<'a> CheckBytes<DefaultValidator<'a>>
            + Deserialize<Res, SharedDeserializeMap>
            + ::core::fmt::Debug
            + PartialEq
            + Send,
    {
        self.get_raw(path)
            .and_then(|stream| async { DynStream::recv(stream).await?.into_owned().await })
            .await
    }

    async fn get_raw(&self, path: &Path) -> Result<<Self as Ipsis>::Reader>;

    async fn put<Req>(&self, data: &Req) -> Result<Path>
    where
        Req: Serialize<Serializer> + IsSigned + Send + Sync,
    {
        let data = data.to_bytes()?;
        let path = Path {
            value: Hash::with_bytes(&data),
            len: data.len().try_into()?,
        };

        self.put_raw(&path, Cursor::new(data)).await?;
        Ok(path)
    }

    async fn put_raw<R>(&self, path: &Path, data: R) -> Result<()>
    where
        R: AsyncRead + Send + Unpin + 'static;

    async fn contains(&self, path: &Path) -> Result<bool>;

    async fn delete(&self, path: &Path) -> Result<()>;
}

#[async_trait]
impl<IpiisClient> Ipsis for IpiisClient
where
    IpiisClient: Ipiis + Send + Sync,
{
    type Reader = <IpiisClient as Ipiis>::Reader;

    async fn get_raw(&self, path: &Path) -> Result<<Self as Ipsis>::Reader> {
        // next target
        let target = self.get_account_primary(KIND.as_ref()).await?;

        // external call
        let mut recv = external_call!(
            client: self,
            target: KIND.as_ref() => &target,
            request: crate::io => Get,
            sign: self.sign(target, *path)?,
            inputs: { },
            outputs: send,
        );

        // recv sign
        let sign: GuarantorSigned<Path> = DynStream::recv(&mut recv).await?.into_owned().await?;

        // verify sign
        let _ = sign.verify(Some(target))?;

        Ok(recv)
    }

    async fn put_raw<R>(&self, path: &Path, data: R) -> Result<()>
    where
        R: AsyncRead + Send + Unpin + 'static,
    {
        // next target
        let target = self.get_account_primary(KIND.as_ref()).await?;

        // external call
        external_call!(
            client: self,
            target: KIND.as_ref() => &target,
            request: crate::io => Put,
            sign: self.sign(target, *path)?,
            inputs: {
                data: DynStream::Stream {
                    len: path.len,
                    recv: Box::pin(data),
                },
            },
            inputs_mode: none,
            outputs: { },
        );

        Ok(())
    }

    async fn contains(&self, path: &Path) -> Result<bool> {
        // next target
        let target = self.get_account_primary(KIND.as_ref()).await?;

        // external call
        let (contains,) = external_call!(
            client: self,
            target: KIND.as_ref() => &target,
            request: crate::io => Contains,
            sign: self.sign(target, *path)?,
            inputs: { },
            outputs: { contains, },
        );

        // unpack response
        Ok(contains)
    }

    async fn delete(&self, path: &Path) -> Result<()> {
        // next target
        let target = self.get_account_primary(KIND.as_ref()).await?;

        // external call
        external_call!(
            client: self,
            target: KIND.as_ref() => &target,
            request: crate::io => Delete,
            sign: self.sign(target, *path)?,
            inputs: { },
            outputs: { },
        );

        // unpack response
        Ok(())
    }
}

define_io! {
    Get {
        inputs: { },
        input_sign: GuaranteeSigned<Path>,
        outputs: {
            data: Vec<u8>,
        },
        output_sign: GuarantorSigned<Path>,
        generics: { },
    },
    Put {
        inputs: {
            data: Vec<u8>,
        },
        input_sign: GuaranteeSigned<Path>,
        outputs: { },
        output_sign: GuarantorSigned<Path>,
        generics: { },
    },
    Contains {
        inputs: { },
        input_sign: GuaranteeSigned<Path>,
        outputs: {
            contains: bool,
        },
        output_sign: GuarantorSigned<Path>,
        generics: { },
    },
    Delete {
        inputs: { },
        input_sign: GuaranteeSigned<Path>,
        outputs: { },
        output_sign: GuarantorSigned<Path>,
        generics: { },
    },
}

::ipis::lazy_static::lazy_static! {
    pub static ref KIND: Option<::ipis::core::value::hash::Hash> = Some(
        ::ipis::core::value::hash::Hash::with_str("__ipis__ipsis__"),
    );
}
