use futures::channel::oneshot;
use std::{collections::HashMap};
use std::io;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use grpcio::RpcStatus;
use kvproto::extstorepb::{CallRequest, CallResponse};
use lazy_static::lazy_static;
use libc::{c_int, c_uchar, c_void};

use crate::util::{pb_marshal, pb_unmarshal};
use crate::{
    def::*,
    util::{check_status_of_call_response, put_status_error_to_call_response},
};

lazy_static! {
    static ref GLOBAL_SERVER_CONTEXT: Mutex<Option<ServerContext>> = Mutex::new(None);
    static ref GLOBAL_CLIENT_CONTEXT: Mutex<Option<ClientContext>> = Mutex::new(None);
}

#[derive(Clone)]
pub struct ServerContext {
    runtime: Arc<::tokio::runtime::Runtime>,
    service: Arc<dyn ExternalStorageService + Sync + Send>,
}

impl ServerContext {
    pub fn new(
        runtime: Arc<::tokio::runtime::Runtime>,
        service: impl ExternalStorageService + Sync + Send + 'static,
    ) -> Self {
        Self {
            runtime,
            service: Arc::new(service),
        }
    }

    fn shared_context() -> Option<Self> {
        let opt = GLOBAL_SERVER_CONTEXT.lock().unwrap();
        if opt.is_none() {
            None
        } else {
            Some(opt.clone().unwrap())
        }
    }

    fn init_shared_context(new_ctx_func: fn() -> io::Result<Self>) -> io::Result<()> {
        let mut opt = GLOBAL_SERVER_CONTEXT.lock().unwrap();
        match *opt {
            Some(_) => Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "already inited",
            )),
            None => {
                let ctx = new_ctx_func()?;
                opt.replace(ctx);
                Ok(())
            }
        }
    }
}

struct RequestClientContext {
    pub id: Vec<u8>,
    tx: oneshot::Sender<RpcStatusResult<CallResponse>>,
}

impl RequestClientContext {
    pub fn new(id: Vec<u8>, tx: oneshot::Sender<RpcStatusResult<CallResponse>>) -> Self {
        Self { id, tx }
    }
}

#[derive(Clone)]
struct ClientContext {
    requests: Arc<Mutex<HashMap<Vec<u8>, RequestClientContext>>>,
}

impl ClientContext {
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            requests: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn shared_context() -> Option<Self> {
        let opt = GLOBAL_CLIENT_CONTEXT.lock().unwrap();
        if opt.is_none() {
            None
        } else {
            Some(opt.clone().unwrap())
        }
    }

    pub fn init_shared_context(new_ctx_func: fn() -> io::Result<Self>) -> io::Result<()> {
        let mut opt = GLOBAL_CLIENT_CONTEXT.lock().unwrap();
        match *opt {
            Some(_) => Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "already inited",
            )),
            None => {
                let ctx = new_ctx_func()?;
                opt.replace(ctx);
                Ok(())
            }
        }
    }

    pub fn record_request(
        &self,
        request_id: &[u8],
    ) -> oneshot::Receiver<RpcStatusResult<CallResponse>> {
        let mut requests = self.requests.lock().unwrap();
        let (tx, rx) = oneshot::channel::<RpcStatusResult<CallResponse>>();
        let ctx = RequestClientContext::new(request_id.to_owned(), tx);

        requests.insert(ctx.id.clone(), ctx);

        rx
    }

    pub fn response_request(&self, request_id: &Vec<u8>, result: RpcStatusResult<CallResponse>) {
        let mut requests = self.requests.lock().unwrap();
        match requests.remove(request_id) {
            Some(request) => {
                let _ = request.tx.send(result);
            }
            None => (),
        };
    }
}

extern "C" {
    fn server_external_storage_create_context() -> *mut c_void;
}

#[no_mangle]
extern "C" fn server_external_storage_init() -> c_int {
    let result = ServerContext::init_shared_context(|| {
        let result = unsafe {
            let ptr = server_external_storage_create_context();
            Box::from_raw(ptr as *mut std::io::Result<ServerContext>)
        };

        *result
    });

    match result {
        Ok(_) => 0,
        Err(err) => match err.kind() {
            io::ErrorKind::AlreadyExists => 0,
            _ => -1,
        },
    }
}

#[no_mangle]
extern "C" fn server_external_storage_async_request(
    msg: *const c_uchar,
    msg_len: c_int,
    _: *const c_uchar,
    _: c_int,
    cb_func: extern "C" fn(*const c_uchar, c_int, *const c_uchar, c_int),
) -> c_int {
    let ctx_result = ServerContext::shared_context();
    if ctx_result.is_none() {
        return -1;
    }

    let ctx = ctx_result.unwrap();
    let bytes = unsafe { std::slice::from_raw_parts(msg, msg_len as usize) };
    let unmarshal_result = pb_unmarshal::<CallRequest>(bytes);
    if unmarshal_result.is_err() {
        return -1;
    }

    let req = unmarshal_result.unwrap();
    let service = ctx.service.clone();
    let request_id = req.get_request_id().to_owned();
    ctx.runtime.spawn(async move {
        let result = service.call(req).await;
        let resp = match result {
            Ok(res) => res,
            Err(err) => {
                let mut res = CallResponse::new();
                res.set_request_id(request_id);
                put_status_error_to_call_response(&mut res, &err);

                res
            }
        };

        let resp_bytes = pb_marshal(&resp).unwrap();
        cb_func(
            resp_bytes.as_ptr(),
            resp_bytes.len() as c_int,
            std::ptr::null(),
            0,
        );
    });

    0
}

extern "C" fn client_external_storage_callback(
    msg: *const c_uchar,
    msg_len: c_int,
    _: *const c_uchar,
    _: c_int,
) {
    let bytes = unsafe { std::slice::from_raw_parts(msg, msg_len as usize) };
    let unmarshal_result = pb_unmarshal::<CallResponse>(bytes);
    if unmarshal_result.is_err() {
        return;
    }

    match ClientContext::shared_context() {
        Some(ctx) => {
            let resp = unmarshal_result.unwrap();
            let request_id = resp.get_request_id().to_owned();
            ctx.response_request(&request_id, check_status_of_call_response(resp));
        }
        None => (),
    }
}

#[derive(Clone)]
struct NativeRawClient {}

impl NativeRawClient {
    pub fn new() -> Self {
        let _ = ClientContext::init_shared_context(|| {
            server_external_storage_init();
            ClientContext::new()
        });
        Self {}
    }

    fn call_native(
        &self,
        req: &CallRequest,
    ) -> RpcErrResult<oneshot::Receiver<RpcStatusResult<CallResponse>>> {
        let ctx = ClientContext::shared_context();
        if ctx.is_none() {
            return Err(::grpcio::Error::RpcFailure(::grpcio::RpcStatus::new(
                ::grpcio::RpcStatusCode::INTERNAL,
                Some("client not inited".to_owned()),
            )));
        }

        let bytes = pb_marshal(req)?;

        let code = server_external_storage_async_request(
            bytes.as_ptr(),
            bytes.len() as c_int,
            std::ptr::null(),
            0,
            client_external_storage_callback,
        );

        if code != 0 {
            return Err(::grpcio::Error::RpcFailure(::grpcio::RpcStatus::new(
                ::grpcio::RpcStatusCode::INTERNAL,
                Some(format!("async call refused, code: {}", code)),
            )));
        }

        let ctx = ctx.unwrap();
        Ok(ctx.record_request(req.get_request_id()))
    }
}

#[async_trait]
impl ExternalStorageRawClient for NativeRawClient {
    async fn call(&self, req: &CallRequest) -> RpcErrResult<CallResponse> {
        let rx = self.call_native(req)?;
        let wait_result = rx.await;
        if wait_result.is_err() {
            return Err(::grpcio::Error::RpcFailure(RpcStatus::new(
                ::grpcio::RpcStatusCode::ABORTED,
                Some(format!("{}", wait_result.unwrap_err())),
            )));
        }

        match wait_result.unwrap() {
            Ok(resp) => Ok(resp),
            Err(err) => Err(::grpcio::Error::RpcFailure(err)),
        }
    }
}

pub fn new_dylib_client() -> ExternalStorageApiClient {
    ExternalStorageApiClient::new(NativeRawClient::new())
}
