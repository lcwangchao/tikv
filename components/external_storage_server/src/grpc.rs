use crate::{ExternalStorageService, ExternalStorageRawClient, call_external_storage_service};
use std::sync::Arc;
use async_trait::async_trait;

use kvproto::extstorepb::{CallRequest, CallResponse};
use kvproto::extstorepb_grpc::{ExternalStorage, ExternalStorageClient};

#[derive(Clone)]
pub struct RpcExternalStorage<T: ExternalStorageService> {
    runtime: Arc<::tokio::runtime::Runtime>,
    service: T,
}

impl<T: ExternalStorageService> RpcExternalStorage<T> {
    pub fn new(service: T) -> Self {
        let runtime = ::tokio::runtime::Runtime::new().unwrap();

        Self {
            runtime: Arc::new(runtime),
            service,
        }
    }
}

impl<T: ExternalStorageService + Send + Sync + Clone + 'static> ExternalStorage for RpcExternalStorage<T> {
    fn call(
        &mut self,
        _: grpcio::RpcContext,
        req: CallRequest,
        sink: grpcio::UnarySink<CallResponse>,
    ) {
        let service = self.service.clone();
        self.runtime.spawn(async move {
            // let res = call_external_storage_service(service.as_ref(), req).await;
            match call_external_storage_service(&service, req).await {
                Ok(res) => sink.success(res),
                Err(err) => sink.fail(err)
            }
        });
    }
}

pub type RpcRawClient = ExternalStorageClient;

#[async_trait]
impl ExternalStorageRawClient for RpcRawClient {
    async fn raw_call(&self, req: &CallRequest) -> crate::RpcErrResult<CallResponse> {
        Ok(self.call_async(req)?.await?)
    }
}
