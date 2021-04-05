use std::io::Read;
use std::sync::Arc;

use external_storage_server::grpc::RpcExternalStorage;
use external_storage_server::service::DefaultExternalStorageService;
use kvproto::extstorepb_grpc::{create_external_storage, ExternalStorage};

const ADDR: &str = "127.0.0.1";
const PORT: u16 = 50051;

fn create_s3_storage() -> external_storage::S3Storage {
    let mut config = kvproto::backup::S3::new();
    config.set_endpoint("http://127.0.0.1:9000".to_owned());
    config.set_access_key("minioadmin".to_owned());
    config.set_secret_access_key("minioadmin".to_owned());
    config.set_bucket("mytest".to_owned());
    config.set_prefix("backups".to_owned());

    external_storage::S3Storage::new(&config).unwrap()
}

fn create_service() -> impl ExternalStorage + Send + Clone + 'static {
    let mut service = DefaultExternalStorageService::new();
    service.register_store("s3", "s3", create_s3_storage());

    RpcExternalStorage::new(service)
}

fn build_server(service: impl ExternalStorage + Send + Clone + 'static) -> grpcio::Server {
    let env = Arc::new(grpcio::Environment::new(1));
    let service = create_external_storage(service);
    let quota =
        grpcio::ResourceQuota::new(Some("ExternalStrageResourceQuota")).resize_memory(1024 * 1024);
    let ch_builder = grpcio::ChannelBuilder::new(env.clone()).set_resource_quota(quota);

    grpcio::ServerBuilder::new(env)
        .register_service(service)
        .bind(ADDR, PORT)
        .channel_args(ch_builder.build_args())
        .build()
        .unwrap()
}

fn wait_server(server: &mut grpcio::Server) {
    let (tx, rx) = futures::channel::oneshot::channel();
    std::thread::spawn(move || {
        println!("Press ENTER to exit...");
        let _ = std::io::stdin().read(&mut [0]).unwrap();
        tx.send(())
    });
    let _ = futures::executor::block_on(rx);
    let _ = futures::executor::block_on(server.shutdown());
}

fn main() {
    let mut server = build_server(create_service());
    server.start();

    for (host, port) in server.bind_addrs() {
        println!("listening on {}:{}", host, port);
    }

    wait_server(&mut server);
}
