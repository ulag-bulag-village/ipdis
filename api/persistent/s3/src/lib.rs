use ipdis_common::{ipiis_api::common::Ipiis, Ipdis};
use ipis::{
    async_trait::async_trait,
    core::{
        account::AccountRef,
        anyhow::{bail, Result},
        value::{chrono::DateTime, hash::Hash},
    },
    env::{infer, Infer},
    log::warn,
    path::Path,
};
use s3::Bucket;

pub type IpdisClient = IpdisClientInner<::ipdis_common::ipiis_api::client::IpiisClient>;

pub struct IpdisClientInner<IpiisClient> {
    pub ipiis: IpiisClient,
    storage: Bucket,
}

impl<IpiisClient> AsRef<::ipdis_common::ipiis_api::client::IpiisClient>
    for IpdisClientInner<IpiisClient>
where
    IpiisClient: AsRef<::ipdis_common::ipiis_api::client::IpiisClient>,
{
    fn as_ref(&self) -> &::ipdis_common::ipiis_api::client::IpiisClient {
        self.ipiis.as_ref()
    }
}

impl<IpiisClient> AsRef<::ipdis_common::ipiis_api::server::IpiisServer>
    for IpdisClientInner<IpiisClient>
where
    IpiisClient: AsRef<::ipdis_common::ipiis_api::server::IpiisServer>,
{
    fn as_ref(&self) -> &::ipdis_common::ipiis_api::server::IpiisServer {
        self.ipiis.as_ref()
    }
}

impl<'a, IpiisClient> Infer<'a> for IpdisClientInner<IpiisClient>
where
    IpiisClient: Infer<'a, GenesisResult = IpiisClient>,
    <IpiisClient as Infer<'a>>::GenesisArgs: Sized,
{
    type GenesisArgs = <IpiisClient as Infer<'a>>::GenesisArgs;
    type GenesisResult = Self;

    fn try_infer() -> Result<Self> {
        IpiisClient::try_infer().and_then(Self::with_ipiis_client)
    }

    fn genesis(
        args: <Self as Infer<'a>>::GenesisArgs,
    ) -> Result<<Self as Infer<'a>>::GenesisResult> {
        IpiisClient::genesis(args).and_then(Self::with_ipiis_client)
    }
}

impl<IpiisClient> IpdisClientInner<IpiisClient> {
    pub fn with_ipiis_client(ipiis: IpiisClient) -> Result<Self> {
        Ok(Self {
            ipiis,
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
impl<IpiisClient> Ipdis for IpdisClientInner<IpiisClient>
where
    IpiisClient: Ipiis + Send + Sync,
{
    async fn get_raw(&self, path: &Path) -> Result<Vec<u8>> {
        // get canonical path
        let path = to_path_canonical(self.ipiis.account_me().account_ref(), path);

        // external call
        let (data, status_code) = self.storage.get_object(path).await?;

        // validate response
        let () = validate_http_status_code(status_code)?;

        // pack data
        Ok(data)
    }

    async fn put_raw(&self, data: Vec<u8>, expiration_date: Option<DateTime>) -> Result<Path> {
        if expiration_date.is_some() {
            warn!("Expiration date for s3 is not supported yet!");
        }

        // get canonical path
        let path = Path {
            value: Hash::with_bytes(&data),
            len: data.len().try_into()?,
        };
        let path_canonical = to_path_canonical(self.ipiis.account_me().account_ref(), &path);

        // external call
        let (_, status_code) = self.storage.put_object(&path_canonical, &data).await?;

        // validate response
        let () = validate_http_status_code(status_code)?;

        // pack data
        Ok(path)
    }

    async fn delete(&self, path: &Path) -> Result<()> {
        // get canonical path
        let path = to_path_canonical(self.ipiis.account_me().account_ref(), path);

        // external call
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
        bail!("HTTP response was not successful: \"{status_code}\"")
    }
}
