use core::convert::Infallible;

use ipdis_common::{ipiis_api::server::IpiisServer, Ipdis, Request, RequestType, Response};
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

impl Infer for IpdisServer {
    fn infer() -> Result<Self> {
        Ok(Self {
            client: IpdisClientInner::infer()?,
        })
    }
}

impl IpdisServer {
    pub async fn run(&self) -> Result<Infallible> {
        self.client.run(|req| self.handler(req)).await
    }

    async fn handler(&self, req: Pinned<Request>) -> Result<Response> {
        // TODO: CURD without deserializing
        let req = req.deserialize_into()?;

        match &req.data.data {
            RequestType::Get { path } => Ok(Response::Get {
                data: self.client.get_raw(path).await?,
            }),
            RequestType::Put { data } => Ok(Response::Put {
                path: match req.expiration_date {
                    Some(expiration_date) => self.client.put(data, expiration_date).await?,
                    None => self.client.put_permanent(data).await?,
                },
            }),
            RequestType::Delete { path } => {
                self.client.delete(path).await.map(|()| Response::Delete)
            }
        }
    }
}
