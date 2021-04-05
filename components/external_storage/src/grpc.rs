// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::ExternalStorage;
use futures::{AsyncRead, AsyncReadExt};
use grpcio::{ChannelBuilder, EnvBuilder};
use std::io;
use std::sync::Arc;
use tikv_util::stream::{block_on_external_io, retry};
use tiri_api::etspb::*;
use tiri_api::etspb_grpc::ExternalStorageClient;

const MINIMUM_PART_SIZE: usize = 5 * 1024 * 1024;

/// A storage write file data to remote via grpc
#[derive(Clone)]
pub struct GrpcStorage {
    store_name: String,
    store_id: String,
    client: ExternalStorageClient,
}

impl GrpcStorage {
    pub fn new_from_client(store_name: &str, client: ExternalStorageClient) -> Self {
        let store_name = String::from(store_name);

        let mut req = GetStoreRequest::default();
        req.set_store_name(store_name.clone());
        let resp = client.get_store(&req).unwrap();
        let store_id = String::from(resp.get_store().get_id());

        GrpcStorage {
            store_name,
            store_id,
            client,
        }
    }

    pub fn new(store_name: &str, addr: &str) -> Self {
        let env = Arc::new(EnvBuilder::new().build());
        let ch = ChannelBuilder::new(env).connect(addr);
        let client = ExternalStorageClient::new(ch);
        return GrpcStorage::new_from_client(store_name, client);
    }
}

struct GrpcUploader<'client> {
    store_id: String,
    filepath: String,
    writer_id: String,
    client: &'client ExternalStorageClient,
}

impl<'client> GrpcUploader<'client> {
    fn new(client: &'client ExternalStorageClient, store_id: String, filepath: String) -> Self {
        Self {
            store_id,
            filepath,
            client,
            writer_id: String::from(""),
        }
    }

    async fn run(
        mut self,
        reader: &mut (dyn AsyncRead + Unpin),
        est_len: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if est_len <= MINIMUM_PART_SIZE as u64 {
            // For short files, execute one put_object to upload the entire thing.
            let mut data = Vec::with_capacity(est_len as usize);
            reader.read_to_end(&mut data).await?;
            retry(|| self.upload(&data)).await?;
            Ok(())
        } else {
            self.writer_id = self.begin().await?;
            let resp = async {
                let mut buf = vec![0; MINIMUM_PART_SIZE];
                loop {
                    let data_size = reader.read(&mut buf).await?;
                    if data_size == 0 {
                        break;
                    }
                    self.upload_part(&buf[..data_size]).await?;
                }
                Ok(())
            }
            .await;

            if resp.is_ok() {
                self.complete().await?;
                Ok(())
            } else {
                resp
            }
        }
    }

    async fn upload(&self, data: &[u8]) -> grpcio::Result<()> {
        let mut req = WriteFileRequest::default();
        req.set_store_id(self.store_id.clone());
        req.set_filepath(self.filepath.clone());
        req.set_data(data.to_vec());

        self.client.write_file_async(&req)?.await?;

        Ok(())
    }

    async fn begin(&self) -> grpcio::Result<String> {
        let mut req = CreateWriterRequest::default();
        req.set_store_id(self.store_id.clone());
        req.set_filepath(self.filepath.clone());

        let resp = self.client.create_writer_async(&req)?.await?;

        Ok(String::from(resp.get_writer_id()))
    }

    async fn upload_part(&self, data: &[u8]) -> grpcio::Result<()> {
        let mut req = WriteWriterRequest::default();
        req.set_store_id(self.store_id.clone());
        req.set_writer_id(self.writer_id.clone());
        req.set_data(data.to_vec());

        self.client.write_writer_async(&req)?.await?;

        Ok(())
    }

    async fn complete(&self) -> grpcio::Result<()> {
        let mut req = CloseWriterRequest::default();
        req.set_store_id(self.store_id.clone());
        req.set_writer_id(self.writer_id.clone());

        self.client.close_writer_async(&req)?.await?;

        Ok(())
    }
}

impl ExternalStorage for GrpcStorage {
    fn write(
        &self,
        name: &str,
        mut reader: Box<dyn AsyncRead + Send + Unpin>,
        content_length: u64,
    ) -> io::Result<()> {
        let uploader = GrpcUploader::new(&self.client, self.store_id.clone(), String::from(name));
        block_on_external_io(uploader.run(&mut *reader, content_length)).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("failed to put object {}", e))
        })
    }

    fn read(&self, _: &str) -> Box<dyn AsyncRead + Unpin> {
        unimplemented!()
    }
}
