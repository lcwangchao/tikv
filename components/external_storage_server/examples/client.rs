use std::sync::Arc;

use external_storage_server::{ExternalStorageClient};
use grpcio::{ChannelBuilder, EnvBuilder};
use kvproto::extstorepb::{
    ExternalStorageClient as RpcRawClient, GetStoreRequest, ListStoreRequest, Store,
    WriteFileRequest,
};

type Client = ExternalStorageClient<RpcRawClient>;

fn build_client() -> Client {
    let env = Arc::new(EnvBuilder::new().build());
    let ch = ChannelBuilder::new(env).connect("localhost:50051");

    ExternalStorageClient::new(RpcRawClient::new(ch))
}

fn fmt_store(store: &Store) -> String {
    format!(
        "Store: {}, driver: {}, provider: {}",
        store.get_id(),
        store.get_driver(),
        store.get_provider()
    )
}

pub async fn list_store(client: &Client) {
    let req = ListStoreRequest::new();
    let resp = client.list_store(&req).await.unwrap();
    println!("ListStoreResponse [");
    for store in resp.get_items() {
        println!("    {}", fmt_store(store));
    }
    println!("]");
}

pub async fn get_store(client: &Client, store_id: &str) {
    let mut req = GetStoreRequest::new();
    req.set_store_id(store_id.to_owned());
    let resp = client.get_store(&req).await.unwrap();
    println!("GetStoreResponse {}", fmt_store(resp.get_store()));
}

pub async fn write_file(client: &Client, store_id: &str) {
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

    let client = build_client();

    let task = write_file(&client, "s3");

    rt.block_on(task);
}
