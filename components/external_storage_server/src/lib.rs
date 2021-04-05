pub mod service;
pub mod grpc;
pub mod direct;

use async_trait::async_trait;
use kvproto::extstorepb::*;

pub type RpcStatusResult<T> = std::result::Result<T, ::grpcio::RpcStatus>;
pub type RpcErrResult<T> = std::result::Result<T, ::grpcio::Error>;

#[macro_export]
macro_rules! impl_external_storage {
    (
        $(#[$outer:meta])*
        pub trait $ServiceName:ident {
            $(
                async fn $fn_name:ident(&self, req: $RequestType:ident) -> RpcStatusResult<$ResponseType:ident>;
            )+
        }
    ) => {

        $(#[$outer])*
        pub trait $ServiceName {
            $(
                async fn $fn_name(&self, req: $RequestType) -> RpcStatusResult<$ResponseType>;
            )*

            async fn call(&self, req: CallRequest) -> RpcStatusResult<CallResponse> {
                if !req.has_request() {
                    return Err(::grpcio::RpcStatus::new(
                        ::grpcio::RpcStatusCode::INVALID_ARGUMENT,
                        Some("request message is empty".to_owned())
                    ));
                }
            
                let message = req.request.unwrap().message;
                if message.is_none() {
                    return Err(::grpcio::RpcStatus::new(
                        ::grpcio::RpcStatusCode::INVALID_ARGUMENT,
                        Some("request message is empty".to_owned())
                    ));
                }
            
                let mut inner_resp = CallResponseResponse::new();
                inner_resp.message = match message.unwrap() {
                    $(
                        CallRequest_Request_oneof_message::$RequestType(inner_req) => { 
                            Some(CallResponse_Response_oneof_message::$ResponseType(
                                self.$fn_name(inner_req).await?
                            ))
                        }
                    )*
                };
            
                let mut resp = CallResponse::new();
                resp.set_request_id(req.request_id);
                resp.set_response(inner_resp);
                Ok(resp)
            }
        }

        impl ExternalStorageClient {
            $(
                pub async fn $fn_name(&self, req: &$RequestType) -> RpcErrResult<$ResponseType> {
                    let mut inner_req = CallRequestRequest::new();
                    inner_req.message = Some(
                        CallRequest_Request_oneof_message::$RequestType(req.clone())
                    );
            
                    let mut call_req = CallRequest::new();
                    call_req.set_request(inner_req);
            
                    let call_resp = self.client.call(&call_req).await?;
                    if !call_resp.has_response() {
                        return Err(::grpcio::Error::RpcFailure(::grpcio::RpcStatus::new(
                            ::grpcio::RpcStatusCode::INTERNAL,
                            Some("faild to get response".to_owned())
                        )));
                    }
            
                    let message = call_resp.response.unwrap().message;
                    if message.is_none() {
                        return Err(::grpcio::Error::RpcFailure(::grpcio::RpcStatus::new(
                            ::grpcio::RpcStatusCode::INTERNAL,
                            Some("response message is empty".to_owned())
                        )));
                    }
            
                    match message.unwrap() {
                        CallResponse_Response_oneof_message::$ResponseType(inner_resp) => { 
                            Ok(inner_resp)
                        }
                        _ => {
                            return Err(::grpcio::Error::RpcFailure(::grpcio::RpcStatus::new(
                                ::grpcio::RpcStatusCode::INTERNAL,
                                Some("faild to get response".to_owned())
                            )))
                        }
                    }
                }
            )*
        }
    }
}

#[async_trait]
pub trait ExternalStorageRawClient {
    async fn call(&self, req: &CallRequest) -> RpcErrResult<CallResponse>;
}

pub struct ExternalStorageClient {
    client: Box<dyn ExternalStorageRawClient>
}

impl ExternalStorageClient {
    pub fn new(client: impl ExternalStorageRawClient + 'static) -> Self {
        Self {
            client: Box::new(client)
        }
    }
}

impl_external_storage!(
    #[async_trait]
    pub trait ExternalStorageService {
        async fn list_store(&self, req: ListStoreRequest) -> RpcStatusResult<ListStoreResponse>;
        async fn get_store(&self, req: GetStoreRequest) -> RpcStatusResult<GetStoreResponse>;
        async fn write_file(&self, req: WriteFileRequest) -> RpcStatusResult<WriteFileResponse>;
        async fn create_uploader(&self, req: CreateUploaderRequest) -> RpcStatusResult<CreateUploaderResponse>;
        async fn upload_part(&self, req: UploadPartRequest) -> RpcStatusResult<UploadPartResponse>;
        async fn complete_upload(&self, req: CompleteUploadRequest) -> RpcStatusResult<CompleteUploadResponse>;
        async fn abort_upload(&self, req: AbortUploadRequest) -> RpcStatusResult<AbortUploadResponse>;
    }
);
