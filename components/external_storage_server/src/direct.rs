use async_trait::async_trait;
use kvproto::extstorepb::{CallRequest, CallResponse};

use crate::{ExternalStorageClient, ExternalStorageRawClient, ExternalStorageService};

#[derive(Clone)]
struct DirectRawClient<T: ExternalStorageService> {
    service: T,
}

impl<T: ExternalStorageService> DirectRawClient<T> {
    pub fn new(service: T) -> Self {
        Self { service }
    }

    fn marshal<M: ::protobuf::Message>(message: &M) -> crate::RpcErrResult<Vec<u8>> {
        match message.write_to_bytes() {
            Ok(bytes) => Ok(bytes),
            Err(err) => Err(::grpcio::Error::Codec(Box::new(err))),
        }
    }

    fn unmarshal<M: ::protobuf::Message>(buf: &[u8]) -> crate::RpcErrResult<M> {
        match ::protobuf::parse_from_bytes::<M>(buf) {
            Ok(message) => Ok(message),
            Err(err) => Err(::grpcio::Error::Codec(Box::new(err))),
        }
    }
}

#[async_trait]
impl<T: ExternalStorageService + Send + Sync> ExternalStorageRawClient for DirectRawClient<T>
{
    async fn call(&self, req: &CallRequest) -> crate::RpcErrResult<CallResponse> {
        let bytes = Self::marshal(req)?;
        let req: CallRequest = Self::unmarshal(bytes.as_slice())?;

        match self.service.call(req).await {
            Ok(res) => Ok(res),
            Err(err) => Err(::grpcio::Error::RpcFailure(err)),
        }
    }
}

pub fn new_direct_client<T: ExternalStorageService + Send + Sync + 'static>(service: T) -> ExternalStorageClient {
    ExternalStorageClient::new(
        DirectRawClient::new(service)
    )
}
