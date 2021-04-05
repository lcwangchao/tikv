use crate::{ExternalStorageService, RpcStatusResult};
use async_trait::async_trait;
use external_storage::{AsyncExternalStorage, AsyncUploader};
use grpcio::{RpcStatus, RpcStatusCode};
use kvproto::extstorepb::*;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use uuid::Uuid;

type Storages = Arc<Mutex<HashMap<String, StoreWrapper>>>;
type Uploaders = Arc<Mutex<HashMap<String, UploaderWrapper>>>;

#[derive(Clone)]
struct StoreWrapper {
    store: Arc<dyn AsyncExternalStorage>,
    store_id: String,
    provider: String,
}

#[derive(Clone)]
struct UploaderWrapper {
    uploader: Arc<dyn AsyncUploader>,
    uploader_id: String,
    store_id: String,
}

#[derive(Clone)]
pub struct DefaultExternalStorageService {
    storages: Storages,
    uploaders: Uploaders,
    rt: Arc<::tokio::runtime::Runtime>,
}

impl DefaultExternalStorageService {
    pub fn new() -> Self {
        let threaded_rt = ::tokio::runtime::Runtime::new().unwrap();

        Self {
            storages: Arc::new(Mutex::new(HashMap::new())),
            uploaders: Arc::new(Mutex::new(HashMap::new())),
            rt: Arc::new(threaded_rt),
        }
    }

    pub fn register_store<T: AsyncExternalStorage>(
        &mut self,
        store_id: &str,
        provider: &str,
        store: T,
    ) {
        let mut storages = self.storages.lock().unwrap();
        let wrapper = StoreWrapper {
            store: Arc::new(store),
            store_id: store_id.to_owned(),
            provider: provider.to_owned(),
        };

        storages.insert(wrapper.store_id.clone(), wrapper);
    }

    fn find_store(&self, id: &str) -> RpcStatusResult<StoreWrapper> {
        let storages = self.storages.lock().unwrap();
        if let Some(store) = storages.get(id) {
            Ok(store.clone())
        } else {
            Err(RpcStatus::new(
                RpcStatusCode::NOT_FOUND,
                Some(format!("cannot find store with id: {}", id)),
            ))
        }
    }

    fn find_uploader(&self, id: &str) -> RpcStatusResult<UploaderWrapper> {
        let uploaders = self.uploaders.lock().unwrap();
        if let Some(uploader) = uploaders.get(id) {
            Ok(uploader.clone())
        } else {
            Err(RpcStatus::new(
                RpcStatusCode::NOT_FOUND,
                Some(format!("cannot find uploader with id: {}", id)),
            ))
        }
    }

    fn find_store_uploader(&self, id: &str, store_id: &str) -> RpcStatusResult<UploaderWrapper> {
        let uploader = self.find_uploader(id)?;
        if uploader.store_id == store_id {
            Ok(uploader)
        } else {
            Err(RpcStatus::new(
                RpcStatusCode::NOT_FOUND,
                Some(format!(
                    "cannot find uploader with id: {}, store: {}",
                    id, store_id
                )),
            ))
        }
    }

    fn make_store_pb_obj(store_wrapper: &StoreWrapper) -> Store {
        let mut store = Store::new();
        store.set_id(store_wrapper.store_id.clone());
        store.set_provider(store_wrapper.provider.clone());

        store
    }
}

#[async_trait]
impl ExternalStorageService for DefaultExternalStorageService {
    async fn list_store(&self, _: ListStoreRequest) -> RpcStatusResult<ListStoreResponse> {
        let wrappers = self.storages.lock().unwrap();

        let mut stores: Vec<Store> = Vec::new();
        for wrapper in wrappers.values() {
            stores.push(Self::make_store_pb_obj(wrapper));
        }

        let mut resp = ListStoreResponse::new();
        resp.set_items(protobuf::RepeatedField::from_vec(stores));

        Ok(resp)
    }

    async fn get_store(&self, req: GetStoreRequest) -> RpcStatusResult<GetStoreResponse> {
        let wrapper = self.find_store(req.get_store_id())?;
        let mut resp = GetStoreResponse::new();
        resp.set_store(Self::make_store_pb_obj(&wrapper));

        Ok(resp)
    }

    async fn write_file(&self, req: WriteFileRequest) -> RpcStatusResult<WriteFileResponse> {
        let buf = req.get_data();
        let result = self
            .find_store(req.get_store_id())?
            .store
            .clone()
            .write_async(req.get_filepath(), Box::new(buf), buf.len() as u64)
            .await;

        match result {
            Ok(_) => Ok(WriteFileResponse::new()),
            Err(err) => Err(RpcStatus::new(
                RpcStatusCode::INTERNAL,
                Some(format!("failed to write file, {}", err)),
            )),
        }
    }

    async fn create_uploader(&self, req: CreateUploaderRequest) -> RpcStatusResult<CreateUploaderResponse> {
        let store = self.find_store(req.get_store_id())?.store.clone();

        let mut uploaders = self.uploaders.lock().unwrap();
        let uploader_id = Uuid::new_v4().to_string();
        uploaders.insert(
            uploader_id.clone(),
            UploaderWrapper {
                store_id: req.get_store_id().to_owned(),
                uploader_id: uploader_id.clone(),
                uploader: store.create_uploader(req.get_filepath()),
            },
        );

        let mut uploader_resp = Uploader::new();
        uploader_resp.set_filepath(req.get_filepath().to_owned());
        uploader_resp.set_id(uploader_id);

        let mut resp = CreateUploaderResponse::new();
        resp.set_uploader(uploader_resp);

        Ok(resp)
    }

    async fn upload_part(&self, req: UploadPartRequest) -> RpcStatusResult<UploadPartResponse> {
        let result = self
            .find_store_uploader(req.get_store_id(), req.get_uploader_id())?
            .uploader
            .clone()
            .upload_part_async(req.get_part_number(), req.get_data())
            .await;

        match result {
            Ok(_) => Ok(UploadPartResponse::new()),
            Err(err) => Err(RpcStatus::new(
                RpcStatusCode::INTERNAL,
                Some(format!("failed to upload part, {}", err)),
            )),
        }
    }

    async fn complete_upload(&self, req: CompleteUploadRequest) -> RpcStatusResult<CompleteUploadResponse> {
        let result = self
            .find_store_uploader(req.get_store_id(), req.get_uploader_id())?
            .uploader
            .clone()
            .complete_async()
            .await;

        match result {
            Ok(_) => Ok(CompleteUploadResponse::new()),
            Err(err) => Err(RpcStatus::new(
                RpcStatusCode::INTERNAL,
                Some(format!("failed to complete upload, {}", err)),
            )),
        }
    }

    async fn abort_upload(&self, req: AbortUploadRequest) -> RpcStatusResult<AbortUploadResponse> {
        let result = self
            .find_store_uploader(req.get_store_id(), req.get_uploader_id())?
            .uploader
            .clone()
            .abort_async()
            .await;

        match result {
            Ok(_) => Ok(AbortUploadResponse::new()),
            Err(err) => Err(RpcStatus::new(
                RpcStatusCode::INTERNAL,
                Some(format!("failed to abort upload, {}", err)),
            )),
        }
    }
}
