use std::sync::Arc;

use ipdis_common::{
    ipiis_api::{client::IpiisClient, server::IpiisServer},
    Ipdis, Request, RequestType, Response,
};
use ipis::{core::anyhow::Result, env::Infer, pin::Pinned};

use crate::client::IpdisClientInner;

pub struct IpdisServer {
    client: Arc<IpdisClientInner<IpiisServer>>,
}

impl AsRef<IpiisClient> for IpdisServer {
    fn as_ref(&self) -> &IpiisClient {
        self.client.as_ref().as_ref()
    }
}

impl AsRef<IpiisServer> for IpdisServer {
    fn as_ref(&self) -> &IpiisServer {
        self.client.as_ref().as_ref()
    }
}

impl<'a> Infer<'a> for IpdisServer {
    type GenesisArgs = <IpiisServer as Infer<'a>>::GenesisArgs;
    type GenesisResult = Self;

    fn try_infer() -> Result<Self> {
        Ok(Self {
            client: IpdisClientInner::try_infer()?.into(),
        })
    }

    fn genesis(
        args: <Self as Infer<'a>>::GenesisArgs,
    ) -> Result<<Self as Infer<'a>>::GenesisResult> {
        Ok(Self {
            client: IpdisClientInner::genesis(args)?.into(),
        })
    }
}

impl IpdisServer {
    pub async fn run(&self) {
        let client = self.client.clone();

        let runtime: &IpiisServer = self.client.as_ref().as_ref();
        runtime.run(client, Self::handle).await
    }

    async fn handle(
        client: Arc<IpdisClientInner<IpiisServer>>,
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
