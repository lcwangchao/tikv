use std::sync::Arc;

use external_storage_server::{ExternalStorageClient};
use external_storage_server::service::DefaultExternalStorageService;
use external_storage_server::direct::DirectRawClient;
use grpcio::{ChannelBuilder, EnvBuilder};
use kvproto::extstorepb::{
    ExternalStorageClient as RpcRawClient, GetStoreRequest, ListStoreRequest, Store,
    WriteFileRequest,
};

fn build_rpc_client() -> ExternalStorageClient {
    let env = Arc::new(EnvBuilder::new().build());
    let ch = ChannelBuilder::new(env).connect("localhost:50051");

    ExternalStorageClient::new(RpcRawClient::new(ch))
}

fn create_s3_storage() -> external_storage::S3Storage {
    let mut config = kvproto::backup::S3::new();
    config.set_endpoint("http://127.0.0.1:9000".to_owned());
    config.set_access_key("minioadmin".to_owned());
    config.set_secret_access_key("minioadmin".to_owned());
    config.set_bucket("mytest".to_owned());
    config.set_prefix("backups".to_owned());

    external_storage::S3Storage::new(&config).unwrap()
}

fn build_direct_client() -> ExternalStorageClient {
    let mut service = DefaultExternalStorageService::new();
    service.register_store("s3".to_owned(), "s3".to_owned(), create_s3_storage());

    ExternalStorageClient::new(DirectRawClient::new(service))
}

fn fmt_store(store: &Store) -> String {
    format!(
        "Store: {}, driver: {}, provider: {}",
        store.get_id(),
        store.get_driver(),
        store.get_provider()
    )
}

pub async fn list_store(client: &ExternalStorageClient) {
    let req = ListStoreRequest::new();
    let resp = client.list_store(&req).await.unwrap();
    println!("ListStoreResponse [");
    for store in resp.get_items() {
        println!("    {}", fmt_store(store));
    }
    println!("]");
}

pub async fn get_store(client: &ExternalStorageClient, store_id: &str) {
    let mut req = GetStoreRequest::new();
    req.set_store_id(store_id.to_owned());
    let resp = client.get_store(&req).await.unwrap();
    println!("GetStoreResponse {}", fmt_store(resp.get_store()));
}

pub async fn write_file(client: &ExternalStorageClient, store_id: &str) {
    let mut req = WriteFileRequest::new();
    req.set_store_id(store_id.to_owned());
    req.set_filepath("path_to_file.txt".to_owned());

    let data = Vec::from("12345abcde");
    req.set_data(data);

    let _ = client.write_file(&req).await.unwrap();
    println!("WriteFileSuccess");
}

fn main() {
    let mut rt = ::tokio::runtime::Runtime::new().unwrap();

    let client = build_rpc_client();

    let task = write_file(&client, "s3");

    rt.block_on(task);
}
