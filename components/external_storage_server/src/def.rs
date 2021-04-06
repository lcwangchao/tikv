use async_trait::async_trait;
use kvproto::extstorepb::*;

pub type RpcStatusResult<T> = std::result::Result<T, ::grpcio::RpcStatus>;
pub type RpcErrResult<T> = std::result::Result<T, ::grpcio::Error>;

#[macro_export]
macro_rules! impl_external_storage {
    (
        $(#[$service_outer:meta])*
        pub trait $ServiceName:ident {
            $(
                async fn $service_fn_name:ident(&self, req: $ServiceRequestType:ident) -> RpcStatusResult<$ServiceResponseType:ident>;
            )+
        }

        #[async_trait]
        pub trait $ServiceExtName:ident {
            async fn call(&self, req: $ExtRequestType:ident) -> RpcStatusResult<$ExtResponseType:ident>;
        }

        $(#[$raw_client_outer:meta])*
        pub trait $RawClientName:ident {
            async fn $raw_client_fn_name:ident(&self, req: &$RawClientRequestType:ident) -> RpcErrResult<$RawClientResponseType:ident>;
        }

        $(#[$client_outer:meta])*
        pub struct $ClientName:ident {
            $ref_raw_client:ident: Box<dyn $RefRawClientName:ident + Send + Sync>
        }
    ) => {
        $(#[$service_outer])*
        pub trait $ServiceName {
            $(
                async fn $service_fn_name(&self, req: $ServiceRequestType) -> RpcStatusResult<$ServiceResponseType>;
            )*
        }

        #[async_trait]
        pub trait $ServiceExtName {
            async fn call(&self, req: $ExtRequestType) -> RpcStatusResult<$ExtResponseType>;
        }

        #[async_trait]
        impl $ServiceExtName for Box<&(dyn $ServiceName + Send + Sync)> {
            async fn call(&self, req: $ExtRequestType) -> RpcStatusResult<$ExtResponseType> {
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
                        CallRequest_Request_oneof_message::$ServiceRequestType(inner_req) => { 
                            Some(CallResponse_Response_oneof_message::$ServiceResponseType(
                                self.$service_fn_name(inner_req).await?
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

        #[async_trait]
        impl<T: $ServiceName + Send + Sync> $ServiceExtName for T {
            async fn call(&self, req: $ExtRequestType) -> RpcStatusResult<$ExtResponseType> {
                let bx: Box<&(dyn $ServiceName + Send + Sync)> = Box::new(self);
                bx.call(req).await
            }
        }

        #[async_trait]
        impl $ServiceExtName for Box<dyn $ServiceName + Send + Sync> {
            async fn call(&self, req: $ExtRequestType) -> RpcStatusResult<$ExtResponseType> {
                let service: Box<&(dyn $ServiceName + Send + Sync)> = Box::new(self.as_ref());
                service.call(req).await
            }
        }

        #[async_trait]
        impl $ServiceExtName for std::sync::Arc<dyn $ServiceName + Send + Sync> {
            async fn call(&self, req: $ExtRequestType) -> RpcStatusResult<$ExtResponseType> {
                let service: Box<&(dyn $ServiceName + Send + Sync)> = Box::new(self.as_ref());
                service.call(req).await
            }
        }

        $(#[$raw_client_outer])*
        pub trait $RawClientName {
            async fn $raw_client_fn_name(&self, req: &$RawClientRequestType) -> RpcErrResult<$RawClientResponseType>;
        }

        $(#[$client_outer])*
        pub struct $ClientName {
            $ref_raw_client: Box<dyn $RefRawClientName + Send + Sync>
        }

        impl $ClientName {
            pub fn new(client: impl $RefRawClientName + Send + Sync + 'static) -> Self {
                Self {
                    client: Box::new(client)
                }
            }

            $(
                pub async fn $service_fn_name(&self, req: &$ServiceRequestType) -> RpcErrResult<$ServiceResponseType> {
                    let mut inner_req = CallRequestRequest::new();
                    inner_req.message = Some(
                        CallRequest_Request_oneof_message::$ServiceRequestType(req.clone())
                    );
            
                    let mut call_req = CallRequest::new();
                    call_req.set_request_id(uuid::Uuid::new_v4().as_bytes().to_vec());
                    call_req.set_request(inner_req);
            
                    let call_resp = self.$ref_raw_client.$raw_client_fn_name(&call_req).await?;
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
                        CallResponse_Response_oneof_message::$ServiceResponseType(inner_resp) => { 
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

    #[async_trait]
    pub trait ExternalStorageServiceExt {
        async fn call(&self, req: CallRequest) -> RpcStatusResult<CallResponse>;
    }

    #[async_trait]
    pub trait ExternalStorageRawClient {
        async fn call(&self, req: &CallRequest) -> RpcErrResult<CallResponse>;
    }

    pub struct ExternalStorageApiClient {
        client: Box<dyn ExternalStorageRawClient + Send + Sync>
    }
);