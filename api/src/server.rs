use std::sync::Arc;

use ipiis_api::{
    client::IpiisClient,
    common::{handle_external_call, Ipiis, ServerResult},
    server::IpiisServer,
};
use ipis::{
    async_trait::async_trait,
    core::{
        account::GuaranteeSigned,
        anyhow::{bail, Result},
        data::Data,
    },
    env::Infer,
    path::Path,
    stream::DynStream,
    tokio::io::{AsyncRead, AsyncReadExt},
};
use ipsis_common::Ipsis;

type IpsisClientInner =
    ::ipsis_api_common::client::IpsisClientInner<IpiisServer, super::IpsisPersistentStorageImpl>;

pub struct IpsisServer {
    client: Arc<IpsisClientInner>,
}

impl ::core::ops::Deref for IpsisServer {
    type Target = IpsisClientInner;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

#[async_trait]
impl<'a> Infer<'a> for IpsisServer {
    type GenesisArgs = <IpsisClientInner as Infer<'a>>::GenesisArgs;
    type GenesisResult = Self;

    async fn try_infer() -> Result<Self> {
        Ok(Self {
            client: IpsisClientInner::try_infer().await?.into(),
        })
    }

    async fn genesis(
        args: <Self as Infer<'a>>::GenesisArgs,
    ) -> Result<<Self as Infer<'a>>::GenesisResult> {
        Ok(Self {
            client: IpsisClientInner::genesis(args).await?.into(),
        })
    }
}

handle_external_call!(
    server: IpsisServer => IpsisClientInner,
    name: run,
    request: ::ipsis_common::io => {
        Protocol => handle_protocol,
        Get => handle_get,
        Contains => handle_contains,
        Delete => handle_delete,
    },
    request_raw: ::ipsis_common::io => {
        Put => handle_put,
    },
);

impl IpsisServer {
    async fn handle_protocol(
        client: &IpsisClientInner,
        req: ::ipsis_common::io::request::Protocol<'static>,
    ) -> Result<::ipsis_common::io::response::Protocol<'static>> {
        // unpack sign
        let sign_as_guarantee = req.__sign.into_owned().await?;

        // handle data
        let protocol = client.protocol().await?;

        // sign data
        let server: &IpiisServer = client.as_ref();
        let sign = server.sign_as_guarantor(sign_as_guarantee)?;

        // pack data
        Ok(::ipsis_common::io::response::Protocol {
            __lifetime: Default::default(),
            __sign: ::ipis::stream::DynStream::Owned(sign),
            protocol: ::ipis::stream::DynStream::Owned(protocol),
        })
    }

    async fn handle_get(
        client: &IpsisClientInner,
        req: ::ipsis_common::io::request::Get<'static>,
    ) -> Result<::ipsis_common::io::response::Get<'static>> {
        // unpack sign
        let sign_as_guarantee = req.__sign.into_owned().await?;

        // unpack data
        let path = sign_as_guarantee.data;

        // handle data
        let mut data = client.get_raw(&path).await?;

        // validate the length
        let len = data.read_u64().await?;
        if path.len != len {
            bail!("failed to validate the length")
        }

        // sign data
        let server: &IpiisServer = client.as_ref();
        let sign = server.sign_as_guarantor(sign_as_guarantee)?;

        // pack data
        Ok(::ipsis_common::io::response::Get {
            __lifetime: Default::default(),
            __sign: ::ipis::stream::DynStream::Owned(sign),
            data: ::ipis::stream::DynStream::Stream {
                len: path.len,
                recv: Box::pin(data),
            },
        })
    }

    async fn handle_put<R>(
        client: &IpsisClientInner,
        mut recv: R,
    ) -> Result<::ipsis_common::io::response::Put<'static>>
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
    {
        // recv sign
        let sign_as_guarantee: Data<GuaranteeSigned, Path> =
            DynStream::recv(&mut recv).await?.into_owned().await?;

        // unpack data
        let path = sign_as_guarantee.data;

        // validate the length
        let len = recv.read_u64().await?;
        if path.len != len {
            bail!("failed to validate the length")
        }

        // handle data
        client.put_raw(&path, recv).await?;

        // sign data
        let server: &IpiisServer = client.as_ref();
        let sign = server.sign_as_guarantor(sign_as_guarantee)?;

        // pack data
        Ok(::ipsis_common::io::response::Put {
            __lifetime: Default::default(),
            __sign: ::ipis::stream::DynStream::Owned(sign),
        })
    }

    async fn handle_contains(
        client: &IpsisClientInner,
        req: ::ipsis_common::io::request::Contains<'static>,
    ) -> Result<::ipsis_common::io::response::Contains<'static>> {
        // unpack sign
        let sign_as_guarantee = req.__sign.into_owned().await?;

        // unpack data
        let path = sign_as_guarantee.data;

        // handle data
        let contains = client.contains(&path).await?;

        // sign data
        let server: &IpiisServer = client.as_ref();
        let sign = server.sign_as_guarantor(sign_as_guarantee)?;

        // pack data
        Ok(::ipsis_common::io::response::Contains {
            __lifetime: Default::default(),
            __sign: ::ipis::stream::DynStream::Owned(sign),
            contains: ::ipis::stream::DynStream::Owned(contains),
        })
    }

    async fn handle_delete(
        client: &IpsisClientInner,
        req: ::ipsis_common::io::request::Delete<'static>,
    ) -> Result<::ipsis_common::io::response::Delete<'static>> {
        // unpack sign
        let sign_as_guarantee = req.__sign.into_owned().await?;

        // unpack data
        let path = sign_as_guarantee.data;

        // handle data
        client.delete(&path).await?;

        // sign data
        let server: &IpiisServer = client.as_ref();
        let sign = server.sign_as_guarantor(sign_as_guarantee)?;

        // pack data
        Ok(::ipsis_common::io::response::Delete {
            __lifetime: Default::default(),
            __sign: ::ipis::stream::DynStream::Owned(sign),
        })
    }
}
