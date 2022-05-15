use std::sync::Arc;

use ipiis_api::server::IpiisServer;
use ipis::{core::anyhow::Result, env::Infer, pin::Pinned};
use ipsis_common::{Ipsis, Request, RequestType, Response};

use crate::client::IpsisClientInner;

pub struct IpsisServer {
    client: Arc<IpsisClientInner<IpiisServer>>,
}

impl ::core::ops::Deref for IpsisServer {
    type Target = IpsisClientInner<IpiisServer>;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl AsRef<::ipiis_api::client::IpiisClient> for IpsisServer {
    fn as_ref(&self) -> &::ipiis_api::client::IpiisClient {
        (*self.client).as_ref()
    }
}

impl<'a> Infer<'a> for IpsisServer {
    type GenesisArgs = <IpiisServer as Infer<'a>>::GenesisArgs;
    type GenesisResult = Self;

    fn try_infer() -> Result<Self> {
        Ok(Self {
            client: IpsisClientInner::<IpiisServer>::try_infer()?.into(),
        })
    }

    fn genesis(
        args: <Self as Infer<'a>>::GenesisArgs,
    ) -> Result<<Self as Infer<'a>>::GenesisResult> {
        Ok(Self {
            client: IpsisClientInner::<IpiisServer>::genesis(args)?.into(),
        })
    }
}

impl IpsisServer {
    pub async fn run(&self) {
        let client = self.client.clone();

        let runtime: &IpiisServer = &self.client;
        runtime.run(client, Self::handle).await
    }

    async fn handle(
        client: Arc<IpsisClientInner<IpiisServer>>,
        req: Pinned<Request>,
    ) -> Result<Response> {
        // TODO: CURD without deserializing
        let req = req.deserialize_into()?;

        match req.data.data {
            RequestType::Get { path } => Ok(Response::Get {
                data: client.get_raw(&path).await?,
            }),
            RequestType::Put { data } => Ok(Response::Put {
                path: client.put_raw(data, req.data.expiration_date).await?,
            }),
            RequestType::Contains { path } => Ok(Response::Contains {
                contains: client.contains(&path).await?,
            }),
            RequestType::Delete { path } => client.delete(&path).await.map(|()| Response::Delete),
        }
    }
}
