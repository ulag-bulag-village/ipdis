use core::convert::Infallible;

use ipdis_common::{
    ipiis_api::{rustls::Certificate, server::IpiisServer},
    Ipdis, Request, RequestType, Response,
};
use ipis::{core::anyhow::Result, env::Infer, pin::Pinned};

use crate::client::IpdisClientInner;

pub struct IpdisServer {
    client: IpdisClientInner<IpiisServer>,
}

impl ::core::ops::Deref for IpdisServer {
    type Target = IpdisClientInner<IpiisServer>;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl<'a> Infer<'a> for IpdisServer {
    type GenesisArgs = <IpiisServer as Infer<'a>>::GenesisArgs;
    type GenesisResult = (Self, Vec<Certificate>);

    fn infer() -> Result<Self> {
        Ok(Self {
            client: IpiisServer::infer().and_then(IpdisClientInner::with_ipiis_client)?,
        })
    }

    fn genesis(
        port: <Self as Infer<'a>>::GenesisArgs,
    ) -> Result<<Self as Infer<'a>>::GenesisResult> {
        let (server, certs) = IpiisServer::genesis(port)?;

        let server = Self {
            client: IpdisClientInner::with_ipiis_client(server)?,
        };

        Ok((server, certs))
    }
}

impl IpdisServer {
    pub async fn run(&self) -> Result<Infallible> {
        self.client.run(|req| self.handler(req)).await
    }

    async fn handler(&self, req: Pinned<Request>) -> Result<Response> {
        // TODO: CURD without deserializing
        let req = req.deserialize_into()?;

        match req.data.data {
            RequestType::Get { path } => Ok(Response::Get {
                data: self.client.get_raw(&path).await?,
            }),
            RequestType::Put { data } => Ok(Response::Put {
                path: self.client.put_raw(data, req.data.expiration_date).await?,
            }),
            RequestType::Delete { path } => {
                self.client.delete(&path).await.map(|()| Response::Delete)
            }
        }
    }
}
