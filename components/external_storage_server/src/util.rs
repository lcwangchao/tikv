use grpcio::{RpcStatus, RpcStatusCode};
use kvproto::extstorepb::{CallResponse, CallResponseError};

use crate::def::*;

pub fn pb_marshal<M: ::protobuf::Message>(message: &M) -> RpcErrResult<Vec<u8>> {
    match message.write_to_bytes() {
        Ok(bytes) => Ok(bytes),
        Err(err) => Err(::grpcio::Error::Codec(Box::new(err))),
    }
}

pub fn pb_unmarshal<M: ::protobuf::Message>(buf: &[u8]) -> RpcErrResult<M> {
    match ::protobuf::parse_from_bytes::<M>(buf) {
        Ok(message) => Ok(message),
        Err(err) => Err(::grpcio::Error::Codec(Box::new(err))),
    }
}

pub fn put_status_error_to_call_response(resp: &mut CallResponse, err: &grpcio::RpcStatus) {
    let mut error = CallResponseError::new();
    error.set_status(i32::from(err.status));
    if err.details.is_some() {
        let details = err.details.clone().unwrap();
        error.set_details(details)
    }
    resp.set_error(error);
}

pub fn check_status_of_call_response(resp: CallResponse) -> RpcStatusResult<CallResponse> {
    if resp.has_error() {
        Err(RpcStatus::new(
            RpcStatusCode::from(resp.get_error().get_status()), 
            Option::Some(resp.get_error().get_details().to_owned())
        ))
    } else {
        Ok(resp)
    }
}