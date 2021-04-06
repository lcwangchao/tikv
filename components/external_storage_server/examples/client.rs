use std::sync::Arc;

use external_storage_server::{def::{ExternalStorageApiClient}, native::ServerContext};
use external_storage_server::service::DefaultExternalStorageService;
use external_storage_server::direct::new_direct_client;
use external_storage_server::native::new_dylib_client;
use external_storage_server::grpc::new_rpc_client;

use grpcio::{ChannelBuilder, EnvBuilder};
use kvproto::extstorepb::{
    GetStoreRequest, ListStoreRequest, Store,
    WriteFileRequest,
};
use libc::c_void;

fn build_rpc_client() -> ExternalStorageApiClient {
    let env = Arc::new(EnvBuilder::new().build());
    let ch = ChannelBuilder::new(env).connect("localhost:50051");

    new_rpc_client(ch)
}

fn build_service() -> DefaultExternalStorageService {
    let mut service = DefaultExternalStorageService::new();

    service.register_store("s3", "s3", {
        let mut config = kvproto::backup::S3::new();
        config.set_endpoint("http://127.0.0.1:9000".to_owned());
        config.set_access_key("minioadmin".to_owned());
        config.set_secret_access_key("minioadmin".to_owned());
        config.set_bucket("mytest".to_owned());
        config.set_prefix("backups".to_owned());
        external_storage::S3Storage::new(&config).unwrap()
    });

    service
}

fn build_direct_client() -> ExternalStorageApiClient {
    new_direct_client(build_service())
}

fn build_dylib_client() -> ExternalStorageApiClient {
    new_dylib_client()
}

fn fmt_store(store: &Store) -> String {
    format!(
        "Store: {}, provider: {}",
        store.get_id(),
        store.get_provider()
    )
}

pub async fn list_store(client: &ExternalStorageApiClient) {
    let req = ListStoreRequest::new();
    let resp = client.list_store(&req).await.unwrap();
    println!("ListStoreResponse [");
    for store in resp.get_items() {
        println!("    {}", fmt_store(store));
    }
    println!("]");
}

pub async fn get_store(client: &ExternalStorageApiClient, store_id: &str) {
    let mut req = GetStoreRequest::new();
    req.set_store_id(store_id.to_owned());
    let resp = client.get_store(&req).await.unwrap();
    println!("GetStoreResponse {}", fmt_store(resp.get_store()));
}

pub async fn write_file(client: &ExternalStorageApiClient, store_id: &str) {
    let mut req = WriteFileRequest::new();
    req.set_store_id(store_id.to_owned());
    req.set_filepath("path_to_file.txt".to_owned());

    let data = Vec::from("12345abcde");
    req.set_data(data);

    let _ = client.write_file(&req).await.unwrap();
    println!("WriteFileSuccess");
}

pub fn create_server_context() -> std::io::Result<ServerContext> {
    let runtime = ::tokio::runtime::Runtime::new()?;
    let service = build_service();

    Ok(ServerContext::new(Arc::new(runtime), service))
}

#[no_mangle]
pub extern "C" fn server_external_storage_create_context() -> *mut c_void {
    Box::into_raw(Box::new(create_server_context())) as *mut c_void
}

fn main() {
    let mut rt = ::tokio::runtime::Runtime::new().unwrap();

    let client = build_dylib_client();

    let task = get_store(&client, "s3");

    rt.block_on(task);
}
