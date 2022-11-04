use ipis::{
    async_trait::async_trait,
    core::{
        account::AccountRef,
        anyhow::{bail, Result},
    },
    env::{infer, Infer},
    path::Path,
    tokio::io::{AsyncRead, AsyncWrite},
};
use ipsis_api_persistent_common::IpsisPersistentStorage;
use s3::Bucket;

pub struct IpsisPersistentStorageImpl {
    bucket: Bucket,
}

#[async_trait]
impl<'a> Infer<'a> for IpsisPersistentStorageImpl
where
    Self: Send,
{
    type GenesisArgs = ();
    type GenesisResult = Self;

    async fn try_infer() -> Result<Self> {
        Self::try_new()
    }

    async fn genesis(
        (): <Self as Infer<'a>>::GenesisArgs,
    ) -> Result<<Self as Infer<'a>>::GenesisResult> {
        Self::try_new()
    }
}

impl IpsisPersistentStorageImpl {
    pub fn try_new() -> Result<Self> {
        let bucket_name: String = infer("ipsis_client_s3_bucket_name")?;
        let region_name = infer("ipsis_client_s3_region_name")?;
        let region = match infer::<_, String>("ipsis_client_s3_region") {
            Ok(endpoint) => s3::Region::Custom {
                region: region_name,
                endpoint: match endpoint.find("://") {
                    Some(_) => endpoint,
                    None => {
                        let port = infer::<_, u16>("ipsis_client_s3_region_port").unwrap_or(80);
                        format!("http://{endpoint}:{port}")
                    }
                },
            },
            Err(_) => region_name.parse()?,
        };
        let credentials = s3::creds::Credentials::from_env_specific(
            Some("ipsis_client_s3_access_key"),
            Some("ipsis_client_s3_secret_key"),
            None,
            None,
        )?;

        Ok(Self {
            bucket: Bucket::new(&bucket_name, region, credentials)?.with_path_style(),
        })
    }

    pub fn bucket(&self) -> &Bucket {
        &self.bucket
    }

    pub fn to_path_canonical(&self, account: &AccountRef, path: &Path) -> String {
        format!("{}/{}", account.to_string(), path.value.to_string())
    }
}

#[async_trait]
impl IpsisPersistentStorage for IpsisPersistentStorageImpl {
    const PROTOCOL: &'static str = "s3";
    const USE_HASH_AS_NATIVE: bool = false;

    async fn get_raw<W>(&self, account: &AccountRef, path: &Path, writer: &mut W) -> Result<()>
    where
        W: AsyncWrite + Send + Unpin + 'static,
    {
        // get canonical path
        let path = *path;
        let path_canonical = self.to_path_canonical(account, &path);

        // external call
        let bucket = self.bucket.clone();
        let status_code = bucket.get_object_stream(path_canonical, writer).await?;

        // validate response
        validate_http_status_code(status_code)
    }

    async fn put_raw<R>(
        &self,
        account: &AccountRef,
        path: &Path,
        reader: &mut R,
    ) -> Result<Result<(), Path>>
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
    {
        // get canonical path
        let path = *path;
        let path_canonical = self.to_path_canonical(account, &path);

        // external call
        let status_code = self
            .bucket
            .put_object_stream(reader, &path_canonical)
            .await?;

        // validate response
        validate_http_status_code(status_code).map(Ok)
    }

    async fn contains(&self, account: &AccountRef, path: &Path) -> Result<bool> {
        // get canonical path
        let path = self.to_path_canonical(account, path);

        // external call
        let (_, status_code) = self.bucket.head_object(path).await?;

        // validate response
        if status_code != 404 {
            validate_http_status_code(status_code)?;
        }

        // pack data
        Ok(status_code == 200)
    }

    async fn delete(&self, account: &AccountRef, path: &Path) -> Result<()> {
        // get canonical path
        let path = self.to_path_canonical(account, path);

        // external call
        let result = self.bucket.delete_object(path).await?;

        // validate response
        validate_http_status_code(result.status_code())
    }
}

fn validate_http_status_code(status_code: u16) -> Result<()> {
    let status_code = ::http::StatusCode::from_u16(status_code)?;

    if status_code.is_success() {
        Ok(())
    } else {
        bail!("HTTP response was not successful: \"{status_code}\"")
    }
}
