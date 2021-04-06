use async_trait::async_trait;
use kvproto::extstorepb::{CallRequest, CallResponse};

use crate::def::*;
use crate::util::{pb_marshal, pb_unmarshal};

#[derive(Clone)]
struct DirectRawClient<T: ExternalStorageService + Send + Sync + 'static> {
    service: T,
}

impl<T: ExternalStorageService + Send + Sync + 'static> DirectRawClient<T> {
    pub fn new(service: T) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T: ExternalStorageService + Send + Sync + 'static> ExternalStorageRawClient for DirectRawClient<T>
{
    async fn call(&self, req: &CallRequest) -> RpcErrResult<CallResponse> {
        let bytes = pb_marshal(req)?;
        let req: CallRequest = pb_unmarshal(bytes.as_slice())?;

        match self.service.call(req).await {
            Ok(res) => Ok(res),
            Err(err) => Err(::grpcio::Error::RpcFailure(err)),
        }
    }
}

pub fn new_direct_client<T: ExternalStorageService + Send + Sync + 'static>(service: T) -> ExternalStorageApiClient {
    ExternalStorageApiClient::new(
        DirectRawClient::new(service)
    )
}
