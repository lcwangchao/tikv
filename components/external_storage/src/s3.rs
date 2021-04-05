// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use rusoto_core::request::HttpClient;
use rusoto_credential::{ProvideAwsCredentials, StaticProvider};
use std::{io, sync::{Arc, Mutex}};

use futures::TryFutureExt;
use futures_util::{
    future::FutureExt,
    io::{AsyncRead, AsyncReadExt},
    stream::TryStreamExt,
};

use rusoto_core::{
    request::DispatchSignedRequest,
    {ByteStream, RusotoError},
};
use rusoto_s3::*;

use super::{AsyncExternalStorage, AsyncResult, AsyncUploader, ExternalStorage};
use kvproto::backup::S3 as Config;
use tikv_util::stream::{block_on_external_io, error_stream, retry};

/// S3 compatible storage
#[derive(Clone)]
pub struct S3Storage {
    config: Config,
    client: Arc<S3Client>,
    // This should be safe to remove now that we don't use the global client/dispatcher
    // See https://github.com/tikv/tikv/issues/7236.
    // _not_send: PhantomData<*const ()>,
}

impl S3Storage {
    /// Create a new S3 storage for the given config.
    pub fn new(config: &Config) -> io::Result<S3Storage> {
        // Need to explicitly create a dispatcher
        // See https://github.com/tikv/tikv/issues/7236.
        let dispatcher = HttpClient::new()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}", e)))?;
        Self::with_request_dispatcher(&config, dispatcher)
    }

    fn new_creds_dispatcher<Creds, Dispatcher>(
        config: &Config,
        dispatcher: Dispatcher,
        credentials_provider: Creds,
    ) -> io::Result<S3Storage>
    where
        Creds: ProvideAwsCredentials + Send + Sync + 'static,
        Dispatcher: DispatchSignedRequest + Send + Sync + 'static,
    {
        Self::check_config(config)?;
        let region = rusoto_util::get_region(config.region.as_ref(), config.endpoint.as_ref())?;
        let client = S3Client::new_with(dispatcher, credentials_provider, region);
        Ok(S3Storage {
            config: config.clone(),
            client: Arc::new(client)
        })
    }

    pub fn with_request_dispatcher<D>(config: &Config, dispatcher: D) -> io::Result<S3Storage>
    where
        D: DispatchSignedRequest + Send + Sync + 'static,
    {
        Self::check_config(config)?;
        // TODO: this should not be supported.
        // It implies static AWS credentials.
        if !config.access_key.is_empty() && !config.secret_access_key.is_empty() {
            let cred_provider = StaticProvider::new_minimal(
                config.access_key.to_owned(),
                config.secret_access_key.to_owned(),
            );
            Self::new_creds_dispatcher(config, dispatcher, cred_provider)
        } else {
            let cred_provider = rusoto_util::CredentialsProvider::new()?;
            Self::new_creds_dispatcher(config, dispatcher, cred_provider)
        }
    }

    fn check_config(config: &Config) -> io::Result<()> {
        if config.bucket.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "missing bucket name",
            ));
        }
        Ok(())
    }

    fn maybe_prefix_key(&self, key: &str) -> String {
        if !self.config.prefix.is_empty() {
            return format!("{}/{}", self.config.prefix, key);
        }
        key.to_owned()
    }
}

/// A helper for uploading a large files to S3 storage.
///
/// Note: this uploader does not support uploading files larger than 19.5 GiB.
struct S3Uploader {
    client: Arc<S3Client>,

    bucket: String,
    key: String,
    acl: Option<String>,
    server_side_encryption: Option<String>,
    ssekms_key_id: Option<String>,
    storage_class: Option<String>,

    upload_id: Mutex<String>,
    parts: Mutex<Vec<CompletedPart>>,
}

/// Specifies the minimum size to use multi-part upload.
/// AWS S3 requires each part to be at least 5 MiB.
const MINIMUM_PART_SIZE: usize = 5 * 1024 * 1024;

impl S3Uploader {
    /// Creates a new uploader with a given target location and upload configuration.
    fn new(client: Arc<S3Client>, config: &Config, key: String) -> Self {
        fn get_var(s: &str) -> Option<String> {
            if s.is_empty() {
                None
            } else {
                Some(s.to_owned())
            }
        }

        Self {
            client,
            bucket: config.bucket.clone(),
            key,
            acl: get_var(&config.acl),
            server_side_encryption: get_var(&config.sse),
            ssekms_key_id: get_var(&config.sse_kms_key_id),
            storage_class: get_var(&config.storage_class),
            upload_id: Mutex::new("".to_owned()),
            parts: Mutex::new(Vec::new()),
        }
    }

    fn get_upload_id(&self) -> String {
        self.upload_id.lock().unwrap().clone()
    }

    fn set_upload_id(&self, upload_id: String) {
        let mut upload_id_guard = self.upload_id.lock().unwrap();
        *upload_id_guard = upload_id;
    }

    fn push_part(&self, part_number: i64, part: CompletedPart) -> Result<(), io::Error> {
        let mut parts = self.parts.lock().unwrap();
        if part_number - 1 != parts.len() as i64 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "Expected part number {}, but {}",
                    parts.len() + 1,
                    part_number
                ),
            ));
        }
        parts.push(part);
        Ok(())
    }

    fn get_parts(&self) -> Vec<CompletedPart> {
        self.parts.lock().unwrap().clone()
    }
 
    /// Executes the upload process.
    async fn run(
        self,
        reader: &mut (dyn AsyncRead + Unpin + Send),
        est_len: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if est_len <= MINIMUM_PART_SIZE as u64 {
            // For short files, execute one put_object to upload the entire thing.
            let mut data = Vec::with_capacity(est_len as usize);
            reader.read_to_end(&mut data).await?;
            retry(|| self.upload(&data)).await?;
            Ok(())
        } else {
            // Otherwise, use multipart upload to improve robustness.
            let upload_id = retry(|| self.begin()).await?;
            self.set_upload_id(upload_id);
            let upload_res: Result<(), Box<dyn std::error::Error + Send>> = async {
                let mut buf = vec![0; MINIMUM_PART_SIZE];
                let mut part_number = 1;
                loop {
                    let read_res = reader.read(&mut buf).await;
                    if let Err(err) = read_res {
                        return Err(Box::new(err) as Box<dyn std::error::Error + Send>);
                    }
                    let data_size = read_res.unwrap();
                    if data_size == 0 {
                        break;
                    }

                    let part_res = retry(|| self.upload_part(part_number, &buf[..data_size])).await;
                    if let Err(err) = part_res {
                        return Err(Box::new(err));
                    }

                    let push_part_res = self.push_part(part_number, part_res.unwrap());
                    if let Err(err) = push_part_res {
                        return Err(Box::new(err));
                    }

                    part_number += 1;
                }
                Ok(())
            }
            .await;

            if upload_res.is_ok() {
                retry(|| self.complete()).await?;
                Ok(())
            } else {
                let _ = retry(|| self.abort()).await;
                Err(upload_res.err().unwrap())
            }
        }
    }

    /// Starts a multipart upload process.
    async fn begin(&self) -> Result<String, RusotoError<CreateMultipartUploadError>> {
        let output = self
            .client
            .create_multipart_upload(CreateMultipartUploadRequest {
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                acl: self.acl.clone(),
                server_side_encryption: self.server_side_encryption.clone(),
                ssekms_key_id: self.ssekms_key_id.clone(),
                storage_class: self.storage_class.clone(),
                ..Default::default()
            })
            .await?;
        output.upload_id.ok_or_else(|| {
            RusotoError::ParseError("missing upload-id from create_multipart_upload()".to_owned())
        })
    }

    /// Completes a multipart upload process, asking S3 to join all parts into a single file.
    async fn complete(&self) -> Result<(), RusotoError<CompleteMultipartUploadError>> {
        self.client
            .complete_multipart_upload(CompleteMultipartUploadRequest {
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                upload_id: self.get_upload_id(),
                multipart_upload: Some(CompletedMultipartUpload {
                    parts: Some(self.get_parts()),
                }),
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    /// Aborts the multipart upload process, deletes all uploaded parts.
    async fn abort(&self) -> Result<(), RusotoError<AbortMultipartUploadError>> {
        self.client
            .abort_multipart_upload(AbortMultipartUploadRequest {
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                upload_id: self.get_upload_id(),
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    /// Uploads a part of the file.
    ///
    /// The `part_number` must be between 1 to 10000.
    async fn upload_part(
        &self,
        part_number: i64,
        data: &[u8],
    ) -> Result<CompletedPart, RusotoError<UploadPartError>> {
        let part = self
            .client
            .upload_part(UploadPartRequest {
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                upload_id: self.get_upload_id(),
                part_number,
                content_length: Some(data.len() as i64),
                body: Some(data.to_vec().into()),
                ..Default::default()
            })
            .await?;
        Ok(CompletedPart {
            e_tag: part.e_tag,
            part_number: Some(part_number),
        })
    }

    /// Uploads a file atomically.
    ///
    /// This should be used only when the data is known to be short, and thus relatively cheap to
    /// retry the entire upload.
    async fn upload(&self, data: &[u8]) -> Result<(), RusotoError<PutObjectError>> {
        self.client
            .put_object(PutObjectRequest {
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                acl: self.acl.clone(),
                server_side_encryption: self.server_side_encryption.clone(),
                ssekms_key_id: self.ssekms_key_id.clone(),
                storage_class: self.storage_class.clone(),
                content_length: Some(data.len() as i64),
                body: Some(data.to_vec().into()),
                ..Default::default()
            })
            .await?;
        Ok(())
    }
}

impl AsyncUploader for S3Uploader {
    fn begin_async<'a>(&'a self) -> AsyncResult<'a, ()> {
        Box::pin(async move {
            let upload_id = self
                .begin()
                .map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("failed to begin upload {}", err),
                    )
                })
                .await?;
            self.set_upload_id(upload_id);
            Ok(())
        })
    }

    fn upload_part_async<'a>(
        &'a self,
        part_number: i64,
        data: &'a [u8],
    ) -> AsyncResult<'a, ()> {
        Box::pin(async move {
            let part = self
                .upload_part(part_number, data)
                .map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("failed to begin upload {}", err),
                    )
                })
                .await?;

            self.push_part(part_number, part)?;
            Ok(())
        })
    }

    fn complete_async<'a>(&'a self) -> AsyncResult<'a, ()> {
        Box::pin(async move {
            self.complete()
                .map_err(|err| {
                    io::Error::new(io::ErrorKind::Other, format!("failed to complete {}", err))
                })
                .await?;

            Ok(())
        })
    }

    fn abort_async<'a>(&'a self) -> AsyncResult<'a, ()> {
        Box::pin(async move {
            self.abort()
                .map_err(|err| {
                    io::Error::new(io::ErrorKind::Other, format!("failed to abort {}", err))
                })
                .await?;

            Ok(())
        })
    }
}

impl AsyncExternalStorage for S3Storage {
    fn create_uploader(&self, name: &str) -> Arc<dyn AsyncUploader> {
        let key = self.maybe_prefix_key(name);
        Arc::new(S3Uploader::new(self.client.clone(), &self.config, key))
    }

    fn write_async<'a>(
        &'a self,
        name: &'a str,
        mut reader: Box<dyn AsyncRead + Send + Unpin + 'a>,
        content_length: u64,
    ) -> AsyncResult<'a, ()> {
        let task = async move {
            let key = self.maybe_prefix_key(name);
            let uploader = S3Uploader::new(self.client.clone(), &self.config, key.clone());
            debug!("save file to s3 storage"; "key" => %key);
            uploader.run(&mut *reader, content_length).await?;
            Ok(())
        }
        .map_err(|e: Box<dyn std::error::Error>| {
            io::Error::new(io::ErrorKind::Other, format!("failed to put object {}", e))
        });

        Box::pin(task)
    }
}

impl ExternalStorage for S3Storage {
    fn write(
        &self,
        name: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        content_length: u64,
    ) -> io::Result<()> {
        block_on_external_io(self.write_async(name, reader, content_length))
    }

    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_> {
        let key = self.maybe_prefix_key(name);
        let bucket = self.config.bucket.clone();
        debug!("read file from s3 storage"; "key" => %key);
        let req = GetObjectRequest {
            key,
            bucket: bucket.clone(),
            ..Default::default()
        };
        Box::new(
            self.client
                .get_object(req)
                .map(move |future| match future {
                    Ok(out) => out.body.unwrap(),
                    Err(RusotoError::Service(GetObjectError::NoSuchKey(key))) => {
                        ByteStream::new(error_stream(io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("no key {} at bucket {}", key, bucket),
                        )))
                    }
                    Err(e) => ByteStream::new(error_stream(io::Error::new(
                        io::ErrorKind::Other,
                        format!("failed to get object {}", e),
                    ))),
                })
                .flatten_stream()
                .into_async_read(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::io::AsyncReadExt;
    use rusoto_core::signature::SignedRequest;
    use rusoto_mock::MockRequestDispatcher;

    #[test]
    fn test_s3_config() {
        let config = Config {
            region: "ap-southeast-2".to_string(),
            bucket: "mybucket".to_string(),
            prefix: "myprefix".to_string(),
            access_key: "abc".to_string(),
            secret_access_key: "xyz".to_string(),
            ..Default::default()
        };
        let cases = vec![
            // bucket is empty
            Config {
                bucket: "".to_owned(),
                ..config.clone()
            },
        ];
        for case in cases {
            let r = S3Storage::new(&case);
            assert!(r.is_err());
        }
        assert!(S3Storage::new(&config).is_ok());
    }

    #[test]
    fn test_s3_storage() {
        let magic_contents = "5678";
        let config = Config {
            region: "ap-southeast-2".to_string(),
            bucket: "mybucket".to_string(),
            prefix: "myprefix".to_string(),
            ..Default::default()
        };
        let dispatcher = MockRequestDispatcher::with_status(200).with_request_checker(
            move |req: &SignedRequest| {
                assert_eq!(req.region.name(), "ap-southeast-2");
                assert_eq!(req.path(), "/mybucket/myprefix/mykey");
                // PutObject is translated to HTTP PUT.
                assert_eq!(req.payload.is_some(), req.method() == "PUT");
            },
        );
        let credentials_provider =
            StaticProvider::new_minimal("abc".to_string(), "xyz".to_string());
        let s = S3Storage::new_creds_dispatcher(&config, dispatcher, credentials_provider).unwrap();
        s.write(
            "mykey",
            Box::new(magic_contents.as_bytes()),
            magic_contents.len() as u64,
        )
        .unwrap();
        let mut reader = s.read("mykey");
        let mut buf = Vec::new();
        let ret = block_on_external_io(reader.read_to_end(&mut buf));
        assert!(ret.unwrap() == 0);
        assert!(buf.is_empty());
    }

    #[test]
    #[cfg(FALSE)]
    // FIXME: enable this (or move this to an integration test) if we've got a
    // reliable way to test s3 (rusoto_mock requires custom logic to verify the
    // body stream which itself can have bug)
    fn test_real_s3_storage() {
        use std::f64::INFINITY;
        use tikv_util::time::Limiter;

        let mut s3 = Config::default();
        s3.set_endpoint("http://127.0.0.1:9000".to_owned());
        s3.set_bucket("bucket".to_owned());
        s3.set_prefix("prefix".to_owned());
        s3.set_access_key("93QZ01QRBYQQXC37XHZV".to_owned());
        s3.set_secret_access_key("N2VcI4Emg0Nm7fDzGBMJvguHHUxLGpjfwt2y4+vJ".to_owned());
        s3.set_force_path_style(true);

        let limiter = Limiter::new(INFINITY);

        let storage = S3Storage::new(&s3).unwrap();
        const LEN: usize = 1024 * 1024 * 4;
        static CONTENT: [u8; LEN] = [50_u8; LEN];
        storage
            .write(
                "huge_file",
                Box::new(limiter.limit(&CONTENT[..])),
                LEN as u64,
            )
            .unwrap();

        let mut reader = storage.read("huge_file");
        let mut buf = Vec::new();
        block_on_external_io(reader.read_to_end(&mut buf)).unwrap();
        assert_eq!(buf.len(), LEN);
        assert_eq!(buf.iter().position(|b| *b != 50_u8), None);
    }
}
