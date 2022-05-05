pub extern crate ipdis_common as common;

use ipdis_common::{
    ipiis_api::{
        client::IpiisClient,
        common::{Ipiis, Serializer},
    },
    Ipdis,
};
use ipis::{
    async_trait::async_trait,
    bytecheck::CheckBytes,
    class::Class,
    core::{
        account::AccountRef,
        anyhow::{bail, Result},
        signature::SignatureSerializer,
        value::{chrono::DateTime, hash::Hash},
    },
    env::infer,
    log::warn,
    path::Path,
    pin::PinnedInner,
    rkyv::{
        de::deserializers::SharedDeserializeMap, validation::validators::DefaultValidator, Archive,
        Deserialize, Serialize,
    },
};
use s3::Bucket;

pub struct IpdisClient {
    ipiis: IpiisClient,
    storage: Bucket,
}

impl AsRef<IpiisClient> for IpdisClient {
    fn as_ref(&self) -> &IpiisClient {
        &self.ipiis
    }
}

impl IpdisClient {
    pub fn infer() -> Result<Self> {
        Ok(Self {
            ipiis: IpiisClient::infer()?,
            storage: {
                let bucket_name: String = infer("ipdis_client_s3_bucket_name")?;
                let region_name = infer("ipdis_client_s3_region_name")?;
                let region = match infer::<_, String>("ipdis_client_s3_region") {
                    Ok(endpoint) => s3::Region::Custom {
                        region: region_name,
                        endpoint: match endpoint.find("://") {
                            Some(_) => endpoint,
                            None => format!("http://{}", endpoint),
                        },
                    },
                    Err(_) => region_name.parse()?,
                };
                let credentials = s3::creds::Credentials::from_env_specific(
                    Some("ipdis_client_s3_access_key"),
                    Some("ipdis_client_s3_secret_key"),
                    None,
                    None,
                )?;

                Bucket::new(&bucket_name, region, credentials)?.with_path_style()
            },
        })
    }
}

#[async_trait]
impl Ipdis for IpdisClient {
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
        // get canonical path
        let path = to_path_canonical(self.ipiis.account_me().account_ref(), path);

        // call external
        let (data, status_code) = self.storage.get_object(path).await?;

        // validate response
        let () = validate_http_status_code(status_code)?;

        // pack data
        PinnedInner::<Res>::new(data).and_then(|e| e.deserialize_into())
    }

    async fn put<Req>(&self, msg: &Req, _expiration_date: DateTime) -> Result<Path>
    where
        Req: Serialize<Serializer> + Send + Sync,
    {
        warn!("Expiration date for s3 is not supported yet!");

        self.put_permanent(msg).await
    }

    async fn put_permanent<Req>(&self, msg: &Req) -> Result<Path>
    where
        Req: Serialize<Serializer> + Send + Sync,
    {
        // serialize data
        let data = ::ipis::rkyv::to_bytes(msg)?;

        // get canonical path
        let path = Path {
            value: Hash::with_bytes(&data),
            len: data.len().try_into()?,
        };
        let path_canonical = to_path_canonical(self.ipiis.account_me().account_ref(), &path);

        // call external
        let (_, status_code) = self.storage.put_object(&path_canonical, &data).await?;

        // validate response
        let () = validate_http_status_code(status_code)?;

        // pack data
        Ok(path)
    }

    async fn delete(&self, path: &Path) -> Result<()> {
        // get canonical path
        let path = to_path_canonical(self.ipiis.account_me().account_ref(), path);

        // call external
        let (_, status_code) = self.storage.delete_object(path).await?;

        // validate response
        let () = validate_http_status_code(status_code)?;

        // pack data
        Ok(())
    }
}

fn to_path_canonical(account: AccountRef, path: &Path) -> String {
    format!("{}/{}", account.to_string(), path.value.to_string())
}

fn validate_http_status_code(status_code: u16) -> Result<()> {
    let status_code = ::http::StatusCode::from_u16(status_code)?;

    if status_code.is_success() {
        Ok(())
    } else {
        bail!("HTTP response was not successful: \"{}\"", status_code)
    }
}
