// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    convert::TryFrom,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc::SyncSender,
    },
    task::{Context, Poll},
    time::{Duration, Instant as StdInstant},
};

use cpu_time::ThreadTime;
use file_system::{IoType, set_io_type};
use futures::{
    channel::oneshot,
    future::{BoxFuture, FutureExt, TryFutureExt},
};
use kvproto::{errorpb, kvrpcpb::CommandPri};
use online_config::{ConfigChange, ConfigManager, ConfigValue, Result as CfgResult};
use pin_project::pin_project;
use prometheus::{Histogram, IntCounter, IntGauge, core::Metric};
use resource_control::{
    AdmissionDecision, ControlledFuture, ResourceController, ResourceGroupManager, ResourceLimiter,
    TaskPriority, with_resource_limiter,
};
use serde_derive::Serialize;
use thiserror::Error;
use tikv_util::{
    resource_control::{TaskMetadata, priority_from_task_meta},
    sys::{SysQuota, cpu_time::ProcessStat},
    thread_name_prefix::{UNIFIED_READ_POOL_THREAD, matches_thread_name_prefix},
    time::Instant,
    worker::{Runnable, RunnableWithTimer, Scheduler, Worker},
    yatp_pool::{self, CleanupMethod, DefaultTicker, FuturePool, PoolTicker, YatpPoolBuilder},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracker::TlsTrackedFuture;
use yatp::{
    metrics::MULTILEVEL_LEVEL_ELAPSED,
    pool::Remote,
    queue::{Extras, TaskCell as TaskCellTrait, priority::TaskPriorityProvider},
    task::future::TaskCell,
};

use self::metrics::*;
use crate::{
    config::{UNIFIED_READPOOL_MIN_CONCURRENCY, UnifiedReadPoolConfig},
    storage::kv::{Engine, FlowStatsReporter, destroy_tls_engine, set_tls_engine},
};

// the duration to check auto-scale unified-thread-pool's thread
const READ_POOL_THREAD_CHECK_DURATION: Duration = Duration::from_secs(10);
// consider scale out read pool size if the average thread cpu usage is higher
// than this threshold.
const READ_POOL_THREAD_HIGH_THRESHOLD: f64 = 0.8;
// consider scale in read pool size if the average thread cpu usage is lower
// than this threshold.
const READ_POOL_THREAD_LOW_THRESHOLD: f64 = 0.7;
// avg running tasks per-thread that indicates read-pool is busy
const RUNNING_TASKS_PER_THREAD_THRESHOLD: i64 = 3;
// Cap flow virtual-time lag so old CPU usage does not suppress a flow forever.
const MAX_READ_FLOW_VIRTUAL_TIME_LAG_US: u64 = 1_000_000;
const READ_FLOW_SCAN_INTERVAL: Duration = Duration::from_millis(200);
const READ_FLOW_MAP_SHARDS: usize = 1024;
const READ_FLOW_MAP_SHARD_MASK: u64 = READ_FLOW_MAP_SHARDS as u64 - 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ReadFlowId {
    pub read_ts: u64,
    pub task_id: u64,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct ReadFlowPrioritySnapshot {
    pub virtual_time_us: u64,
    pub min_virtual_time_us: u64,
    pub priority: u64,
}

impl ReadFlowId {
    pub fn new(read_ts: u64, task_id: u64) -> Option<Self> {
        if read_ts == 0 && task_id == 0 {
            None
        } else {
            Some(Self { read_ts, task_id })
        }
    }

    fn hash(&self) -> u64 {
        let mut x = self.read_ts ^ self.task_id.rotate_left(32);
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        x ^ (x >> 31)
    }

    fn shard_id(&self) -> usize {
        (self.hash() & READ_FLOW_MAP_SHARD_MASK) as usize
    }
}

struct ReadFlowState {
    semaphore: Arc<Semaphore>,
    refs: AtomicUsize,
    virtual_time_us: AtomicU64,
    idle_since_us: AtomicU64,
}

struct ReadFlowTaskState {
    flow: Arc<ReadFlowState>,
    priority_us: AtomicU64,
}

struct FlowMapShard {
    flows: HashMap<ReadFlowId, Arc<ReadFlowState>>,
    flow_task_ids: HashMap<u64, Arc<ReadFlowTaskState>>,
}

impl FlowMapShard {
    fn new() -> Self {
        Self {
            flows: HashMap::new(),
            flow_task_ids: HashMap::new(),
        }
    }
}

struct FlowsMap {
    shards: Vec<RwLock<FlowMapShard>>,
}

impl FlowsMap {
    fn new() -> Self {
        let shards = (0..READ_FLOW_MAP_SHARDS)
            .map(|_| RwLock::new(FlowMapShard::new()))
            .collect();
        Self { shards }
    }

    fn acquire(
        &self,
        flow_id: ReadFlowId,
        flow_task_id: u64,
        max_in_flight: usize,
        initial_virtual_time_us: u64,
    ) -> (Arc<ReadFlowState>, Arc<ReadFlowTaskState>) {
        let mut shard = self.shards[flow_id.shard_id()].write().unwrap();
        let state = shard
            .flows
            .entry(flow_id)
            .or_insert_with(|| {
                Arc::new(ReadFlowState {
                    semaphore: Arc::new(Semaphore::new(max_in_flight)),
                    refs: AtomicUsize::new(0),
                    virtual_time_us: AtomicU64::new(initial_virtual_time_us),
                    idle_since_us: AtomicU64::new(0),
                })
            })
            .clone();
        state.idle_since_us.store(0, Ordering::Release);
        state.refs.fetch_add(1, Ordering::Relaxed);
        let task_state = Arc::new(ReadFlowTaskState {
            flow: state.clone(),
            priority_us: AtomicU64::new(initial_virtual_time_us),
        });
        let old_task_state = shard.flow_task_ids.insert(flow_task_id, task_state.clone());
        debug_assert!(old_task_state.is_none());
        (state, task_state)
    }

    fn get_by_flow_id(&self, flow_id: ReadFlowId) -> Option<Arc<ReadFlowState>> {
        self.shards[flow_id.shard_id()]
            .read()
            .unwrap()
            .flows
            .get(&flow_id)
            .cloned()
    }

    fn get_by_task_id(&self, flow_task_id: u64) -> Option<Arc<ReadFlowTaskState>> {
        self.shards[(flow_task_id & READ_FLOW_MAP_SHARD_MASK) as usize]
            .read()
            .unwrap()
            .flow_task_ids
            .get(&flow_task_id)
            .cloned()
    }

    fn remove_task_id(&self, flow_task_id: u64) {
        self.shards[(flow_task_id & READ_FLOW_MAP_SHARD_MASK) as usize]
            .write()
            .unwrap()
            .flow_task_ids
            .remove(&flow_task_id);
    }

    fn scan_min_virtual_time_us(
        &self,
        now_us: u64,
        is_live: impl Fn(&ReadFlowState, u64) -> bool,
    ) -> u64 {
        let mut min_vt = u64::MAX;
        for shard in &self.shards {
            let shard = shard.read().unwrap();
            for state in shard.flows.values() {
                if is_live(state, now_us) {
                    min_vt = min_vt.min(state.virtual_time_us.load(Ordering::Relaxed));
                }
            }
        }
        if min_vt == u64::MAX { 0 } else { min_vt }
    }

    fn gc_expired_idle_flows(&self, now_us: u64, is_live: impl Fn(&ReadFlowState, u64) -> bool) {
        for shard in &self.shards {
            let mut shard = shard.write().unwrap();
            let expired_flow_ids = shard
                .flows
                .iter()
                .filter_map(|(flow_id, state)| (!is_live(state, now_us)).then_some(*flow_id))
                .collect::<Vec<_>>();
            for flow_id in expired_flow_ids {
                if let Some(state) = shard.flows.remove(&flow_id) {
                    shard
                        .flow_task_ids
                        .retain(|_, task_state| !Arc::ptr_eq(&state, &task_state.flow));
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct ReadFlowController {
    max_in_flight: usize,
    flows: Arc<FlowsMap>,
    next_flow_task_seq: Arc<AtomicU64>,
    min_virtual_time_us: Arc<AtomicU64>,
    min_virtual_time_updated_at_us: Arc<AtomicU64>,
    last_idle_gc_at_us: Arc<AtomicU64>,
    started_at: Arc<StdInstant>,
}

impl Default for ReadFlowController {
    fn default() -> Self {
        Self::new(0)
    }
}

impl ReadFlowController {
    fn new(max_in_flight: usize) -> Self {
        Self {
            max_in_flight,
            flows: Arc::new(FlowsMap::new()),
            next_flow_task_seq: Arc::new(AtomicU64::new(1)),
            min_virtual_time_us: Arc::new(AtomicU64::new(0)),
            min_virtual_time_updated_at_us: Arc::new(AtomicU64::new(0)),
            last_idle_gc_at_us: Arc::new(AtomicU64::new(0)),
            started_at: Arc::new(StdInstant::now()),
        }
    }

    async fn acquire(&self, flow_id: Option<ReadFlowId>) -> ReadFlowPermit {
        let Some(flow_id) = flow_id else {
            return ReadFlowPermit::Noop;
        };
        if self.max_in_flight == 0 {
            return ReadFlowPermit::Noop;
        }

        let initial_virtual_time_us = self.cached_min_virtual_time_us();
        let flow_task_id = self.next_flow_task_id(flow_id.shard_id());
        let (state, task_state) = self.flows.acquire(
            flow_id,
            flow_task_id,
            self.max_in_flight,
            initial_virtual_time_us,
        );
        let semaphore = state.semaphore.clone();
        let flow_ref = ReadFlowRef {
            controller: self.clone(),
            flow_id,
            flow_task_id,
        };

        match semaphore.acquire_owned().await {
            Ok(permit) => ReadFlowPermit::Limited {
                permit: Some(permit),
                flow_ref: Some(flow_ref),
                state,
                task_state,
                flow_task_id,
            },
            // The semaphore is never closed. If that ever changes, fail open so
            // reads are not permanently blocked.
            Err(_) => ReadFlowPermit::Noop,
        }
    }

    fn release_ref(&self, flow_id: ReadFlowId) {
        let Some(state) = self.flows.get_by_flow_id(flow_id) else {
            return;
        };
        let old_refs = state.refs.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(old_refs > 0);

        if old_refs == 1 {
            state
                .idle_since_us
                .store(self.elapsed_us(), Ordering::Release);
        }
    }

    fn release_task_id(&self, flow_task_id: u64) {
        self.flows.remove_task_id(flow_task_id);
    }

    fn next_flow_task_id(&self, shard_id: usize) -> u64 {
        const FLOW_TASK_ID_MARKER: u64 = 1 << 63;
        let seq = self.next_flow_task_seq.fetch_add(1, Ordering::Relaxed);
        FLOW_TASK_ID_MARKER | (seq << READ_FLOW_MAP_SHARDS.trailing_zeros()) | shard_id as u64
    }

    fn flow_priority_snapshot(&self, state: &ReadFlowState) -> ReadFlowPrioritySnapshot {
        let min_vt = self.cached_min_virtual_time_us();
        let max_vt = min_vt.saturating_add(MAX_READ_FLOW_VIRTUAL_TIME_LAG_US);
        let vt = state
            .virtual_time_us
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |vt| {
                (vt > max_vt).then_some(max_vt)
            })
            .unwrap_or_else(|vt| vt);
        let virtual_time_us = vt.min(max_vt);
        ReadFlowPrioritySnapshot {
            virtual_time_us,
            min_virtual_time_us: min_vt,
            priority: virtual_time_us,
        }
    }

    fn task_priority_snapshot(&self, task_state: &ReadFlowTaskState) -> ReadFlowPrioritySnapshot {
        let min_vt = self.cached_min_virtual_time_us();
        let max_vt = min_vt.saturating_add(MAX_READ_FLOW_VIRTUAL_TIME_LAG_US);
        let vt = task_state.priority_us.load(Ordering::Acquire).min(max_vt);
        ReadFlowPrioritySnapshot {
            virtual_time_us: vt,
            min_virtual_time_us: min_vt,
            priority: vt,
        }
    }

    fn cached_min_virtual_time_us(&self) -> u64 {
        self.min_virtual_time_us.load(Ordering::Acquire)
    }

    fn refresh_min_virtual_time_and_gc(&self) {
        let now_us = self.elapsed_us();
        self.maybe_gc_expired_idle_flows(now_us);
        let min_vt = self.scan_min_virtual_time_us_at(now_us);
        self.min_virtual_time_us.store(min_vt, Ordering::Release);
        self.min_virtual_time_updated_at_us
            .store(now_us, Ordering::Release);
    }

    fn elapsed_us(&self) -> u64 {
        self.started_at
            .elapsed()
            .as_micros()
            .min(u128::from(u64::MAX)) as u64
    }

    fn scan_min_virtual_time_us_at(&self, now_us: u64) -> u64 {
        self.flows
            .scan_min_virtual_time_us(now_us, |state, now_us| {
                Self::is_active_or_recent_idle(state, now_us)
            })
    }

    fn is_active_or_recent_idle(state: &ReadFlowState, now_us: u64) -> bool {
        if state.refs.load(Ordering::Acquire) > 0 {
            return true;
        }
        let idle_since_us = state.idle_since_us.load(Ordering::Acquire);
        idle_since_us != 0
            && now_us.saturating_sub(idle_since_us) <= MAX_READ_FLOW_VIRTUAL_TIME_LAG_US
    }

    fn maybe_gc_expired_idle_flows(&self, now_us: u64) {
        let last_gc_us = self.last_idle_gc_at_us.load(Ordering::Acquire);
        if now_us.saturating_sub(last_gc_us) < MAX_READ_FLOW_VIRTUAL_TIME_LAG_US {
            return;
        }
        if self
            .last_idle_gc_at_us
            .compare_exchange(last_gc_us, now_us, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        self.flows.gc_expired_idle_flows(now_us, |state, now_us| {
            Self::is_active_or_recent_idle(state, now_us)
        });
    }

    fn flow_priority_tag_by_task_id(&self, flow_task_id: u64) -> Option<u64> {
        self.flows
            .get_by_task_id(flow_task_id)
            .map(|task_state| self.task_priority_snapshot(&task_state).priority)
    }

    fn flow_priority_snapshot_by_flow_id(
        &self,
        flow_id: ReadFlowId,
    ) -> Option<ReadFlowPrioritySnapshot> {
        self.flows
            .get_by_flow_id(flow_id)
            .map(|state| self.flow_priority_snapshot(&state))
    }
}

fn spawn_read_flow_scanner(pool: &yatp::ThreadPool<TaskCell>, flow_controller: ReadFlowController) {
    let scanner = async move {
        loop {
            futures_timer::Delay::new(READ_FLOW_SCAN_INTERVAL).await;
            flow_controller.refresh_min_virtual_time_and_gc();
        }
    };
    let extras = Extras::new_multilevel(u64::MAX, Some(0));
    pool.spawn(TaskCell::new(TlsTrackedFuture::new(scanner), extras));
}

struct FlowPriorityProvider {
    flow_controller: ReadFlowController,
}

impl FlowPriorityProvider {
    fn new(flow_controller: ReadFlowController) -> Self {
        Self { flow_controller }
    }
}

impl TaskPriorityProvider for FlowPriorityProvider {
    fn priority_of(&self, extras: &Extras) -> u64 {
        self.flow_controller
            .flow_priority_tag_by_task_id(extras.task_id())
            .unwrap_or(u64::from(extras.current_level()))
    }
}

struct ReadFlowRef {
    controller: ReadFlowController,
    flow_id: ReadFlowId,
    flow_task_id: u64,
}

impl Drop for ReadFlowRef {
    fn drop(&mut self) {
        self.controller.release_task_id(self.flow_task_id);
        self.controller.release_ref(self.flow_id);
    }
}

enum ReadFlowPermit {
    Noop,
    Limited {
        permit: Option<OwnedSemaphorePermit>,
        flow_ref: Option<ReadFlowRef>,
        state: Arc<ReadFlowState>,
        task_state: Arc<ReadFlowTaskState>,
        flow_task_id: u64,
    },
}

impl ReadFlowPermit {
    fn yatp_task_id(&self, fallback: u64) -> u64 {
        match self {
            ReadFlowPermit::Noop => fallback,
            ReadFlowPermit::Limited { flow_task_id, .. } => *flow_task_id,
        }
    }

    fn charge_us(&self, delta: u64) {
        let ReadFlowPermit::Limited {
            state, task_state, ..
        } = self
        else {
            return;
        };
        let charged_to_vt = state
            .virtual_time_us
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |vt| {
                Some(vt.saturating_add(delta))
            })
            .map(|old_vt| old_vt.saturating_add(delta))
            .unwrap_or_else(|old_vt| old_vt);
        task_state
            .priority_us
            .store(charged_to_vt, Ordering::Release);
    }

    fn precharge(&self, duration: Duration) -> u64 {
        let delta = duration.as_micros().min(u128::from(u64::MAX)) as u64;
        self.charge_us(delta);
        delta
    }

    fn record_elapsed(
        &self,
        duration: Duration,
        actual_cpu_us: &mut u64,
        charged_cpu_us: &mut u64,
    ) {
        let delta = duration.as_micros().min(u128::from(u64::MAX)) as u64;
        *actual_cpu_us = actual_cpu_us.saturating_add(delta);
        if *actual_cpu_us > *charged_cpu_us {
            let charge = actual_cpu_us.saturating_sub(*charged_cpu_us);
            self.charge_us(charge);
            *charged_cpu_us = *actual_cpu_us;
        }
    }
}

impl Drop for ReadFlowPermit {
    fn drop(&mut self) {
        if let ReadFlowPermit::Limited {
            permit, flow_ref, ..
        } = self
        {
            permit.take();
            flow_ref.take();
        }
    }
}

#[pin_project]
struct FlowTrackedFuture<F> {
    #[pin]
    future: F,
    flow_permit: Option<ReadFlowPermit>,
    estimated_poll_cpu: Duration,
    actual_cpu_us: u64,
    charged_cpu_us: u64,
    running_task_gauge: IntGauge,
}

impl<F> FlowTrackedFuture<F> {
    fn new(
        future: F,
        flow_permit: ReadFlowPermit,
        estimated_cpu: Duration,
        running_task_gauge: IntGauge,
    ) -> Self {
        let charged_cpu_us = flow_permit.precharge(estimated_cpu);
        Self {
            future,
            flow_permit: Some(flow_permit),
            estimated_poll_cpu: estimated_cpu,
            actual_cpu_us: 0,
            charged_cpu_us,
            running_task_gauge,
        }
    }
}

impl<F: Future> Future for FlowTrackedFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let start_time = ThreadTime::now();
        let poll = this.future.poll(cx);

        if let Some(flow_permit) = this.flow_permit.as_ref() {
            flow_permit.record_elapsed(
                start_time.elapsed(),
                this.actual_cpu_us,
                this.charged_cpu_us,
            );
            if poll.is_pending() {
                *this.charged_cpu_us = (*this.charged_cpu_us)
                    .saturating_add(flow_permit.precharge(*this.estimated_poll_cpu));
            }
        }

        if poll.is_ready() {
            this.flow_permit.take();
            this.running_task_gauge.dec();
        }

        poll
    }
}

pub enum ReadPool {
    FuturePools {
        read_pool_high: FuturePool,
        read_pool_normal: FuturePool,
        read_pool_low: FuturePool,
    },
    Yatp {
        pool: yatp::ThreadPool<TaskCell>,
        running_tasks: [IntGauge; TaskPriority::PRIORITY_COUNT],
        running_threads: IntGauge,
        max_tasks: usize,
        pool_size: usize,
        resource_ctl: Option<Arc<ResourceController>>,
        resource_manager: Option<Arc<ResourceGroupManager>>,
        time_slice_inspector: Arc<TimeSliceInspector>,
    },
    YatpFlowControl {
        pool: yatp::ThreadPool<TaskCell>,
        running_tasks: [IntGauge; TaskPriority::PRIORITY_COUNT],
        running_threads: IntGauge,
        max_tasks: usize,
        pool_size: usize,
        time_slice_inspector: Arc<TimeSliceInspector>,
        flow_controller: ReadFlowController,
    },
}

impl ReadPool {
    pub fn handle(&self) -> ReadPoolHandle {
        match self {
            ReadPool::FuturePools {
                read_pool_high,
                read_pool_normal,
                read_pool_low,
            } => ReadPoolHandle::FuturePools {
                read_pool_high: read_pool_high.clone(),
                read_pool_normal: read_pool_normal.clone(),
                read_pool_low: read_pool_low.clone(),
            },
            ReadPool::Yatp {
                pool,
                running_tasks,
                running_threads,
                max_tasks,
                pool_size,
                resource_ctl,
                resource_manager,
                time_slice_inspector,
            } => ReadPoolHandle::Yatp {
                remote: pool.remote().clone(),
                running_tasks: running_tasks.clone(),
                running_threads: running_threads.clone(),
                max_tasks: *max_tasks,
                pool_size: *pool_size,
                resource_ctl: resource_ctl.clone(),
                resource_manager: resource_manager.clone(),
                time_slice_inspector: time_slice_inspector.clone(),
            },
            ReadPool::YatpFlowControl {
                pool,
                running_tasks,
                running_threads,
                max_tasks,
                pool_size,
                time_slice_inspector,
                flow_controller,
            } => ReadPoolHandle::YatpFlowControl {
                remote: pool.remote().clone(),
                running_tasks: running_tasks.clone(),
                running_threads: running_threads.clone(),
                max_tasks: *max_tasks,
                pool_size: *pool_size,
                time_slice_inspector: time_slice_inspector.clone(),
                flow_controller: flow_controller.clone(),
            },
        }
    }
}

#[derive(Clone)]
pub enum ReadPoolHandle {
    FuturePools {
        read_pool_high: FuturePool,
        read_pool_normal: FuturePool,
        read_pool_low: FuturePool,
    },
    Yatp {
        remote: Remote<TaskCell>,
        running_tasks: [IntGauge; TaskPriority::PRIORITY_COUNT],
        running_threads: IntGauge,
        max_tasks: usize,
        pool_size: usize,
        resource_ctl: Option<Arc<ResourceController>>,
        resource_manager: Option<Arc<ResourceGroupManager>>,
        time_slice_inspector: Arc<TimeSliceInspector>,
    },
    YatpFlowControl {
        remote: Remote<TaskCell>,
        running_tasks: [IntGauge; TaskPriority::PRIORITY_COUNT],
        running_threads: IntGauge,
        max_tasks: usize,
        pool_size: usize,
        time_slice_inspector: Arc<TimeSliceInspector>,
        flow_controller: ReadFlowController,
    },
}

/// Runs admission control then, if the task is allowed through, enqueues it.
/// Admission is checked before the capacity/eviction check so that a rejected
/// or timed-out delayed task never causes an already-queued task to be dropped.
async fn admission_and_enqueue(
    resource_manager: Option<Arc<ResourceGroupManager>>,
    resource_limiter: Option<Arc<ResourceLimiter>>,
    task_priority: TaskPriority,
    gauge: IntGauge,
    max_tasks: usize,
    remote: Remote<TaskCell>,
    task_cell: TaskCell,
    running_tasks: Vec<IntGauge>,
    resource_ctl: Option<Arc<ResourceController>>,
    estimated_priority: u64,
) -> Result<(), ReadPoolError> {
    // Admission control runs before any eviction so that a rejected or
    // timed-out delayed task never causes an already-queued task to be dropped.
    let delay = match (resource_manager.as_deref(), resource_limiter.as_deref()) {
        (Some(rm), Some(limiter)) => match rm.admission_decision(true, limiter) {
            AdmissionDecision::Reject => return Err(ReadPoolError::Rejected),
            AdmissionDecision::Delay(d) => {
                if task_priority == TaskPriority::High {
                    warn!("admission delay on high-priority read task";
                          "group" => limiter.name(),
                          "delay" => ?d);
                }
                Some((d, resource_manager))
            }
            AdmissionDecision::Allow => None,
        },
        _ => None,
    };
    if let Some((d, slot)) = delay {
        let mut _guard = slot.as_ref().map(|rm| rm.delay_slot_guard());
        futures_timer::Delay::new(d).await;
        if let Some(guard) = _guard.as_mut() {
            guard.release();
        }
    }
    // After admission (and any sleep), check pool capacity and evict if needed.
    if gauge.get() as usize >= max_tasks {
        if let Some(ref _resource_ctl) = resource_ctl {
            if let Some(mut evicted) = remote.try_evict_lowest(estimated_priority) {
                let evicted_prio = priority_from_task_meta(evicted.mut_extras().metadata());
                running_tasks[evicted_prio as usize].dec();
                UNIFIED_READ_POOL_EVICTED_TASKS.inc();
                drop(evicted);
            } else {
                return Err(ReadPoolError::UnifiedReadPoolFull);
            }
        } else {
            return Err(ReadPoolError::UnifiedReadPoolFull);
        }
    }
    gauge.inc();
    remote.spawn(task_cell);
    Ok(())
}

impl ReadPoolHandle {
    pub fn read_flow_priority_snapshot(
        &self,
        flow_id: Option<ReadFlowId>,
    ) -> Option<ReadFlowPrioritySnapshot> {
        let flow_id = flow_id?;
        match self {
            ReadPoolHandle::YatpFlowControl {
                flow_controller, ..
            } => flow_controller.flow_priority_snapshot_by_flow_id(flow_id),
            _ => None,
        }
    }

    pub fn spawn<F>(
        &self,
        f: F,
        priority: CommandPri,
        task_id: u64,
        metadata: TaskMetadata<'_>,
        resource_limiter: Option<Arc<ResourceLimiter>>,
    ) -> BoxFuture<'static, Result<(), ReadPoolError>>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_with_flow_and_estimated_cpu(
            f,
            priority,
            task_id,
            metadata,
            resource_limiter,
            None,
            Duration::ZERO,
        )
    }

    pub fn spawn_with_flow<F>(
        &self,
        f: F,
        priority: CommandPri,
        task_id: u64,
        metadata: TaskMetadata<'_>,
        resource_limiter: Option<Arc<ResourceLimiter>>,
        flow_id: Option<ReadFlowId>,
    ) -> BoxFuture<'static, Result<(), ReadPoolError>>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_with_flow_and_estimated_cpu(
            f,
            priority,
            task_id,
            metadata,
            resource_limiter,
            flow_id,
            Duration::ZERO,
        )
    }

    pub fn spawn_with_flow_and_estimated_cpu<F>(
        &self,
        f: F,
        priority: CommandPri,
        task_id: u64,
        metadata: TaskMetadata<'_>,
        resource_limiter: Option<Arc<ResourceLimiter>>,
        flow_id: Option<ReadFlowId>,
        estimated_cpu: Duration,
    ) -> BoxFuture<'static, Result<(), ReadPoolError>>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        match self {
            ReadPoolHandle::FuturePools {
                read_pool_high,
                read_pool_normal,
                read_pool_low,
            } => {
                let pool = match priority {
                    CommandPri::High => read_pool_high,
                    CommandPri::Normal => read_pool_normal,
                    CommandPri::Low => read_pool_low,
                };
                let res = pool.spawn(f).map_err(ReadPoolError::from);
                futures::future::ready(res).boxed()
            }
            ReadPoolHandle::Yatp {
                remote,
                running_tasks,
                max_tasks,
                resource_ctl,
                resource_manager,
                ..
            } => {
                let task_priority = TaskPriority::from(metadata.override_priority());
                let running_task_gauge = running_tasks[task_priority as usize].clone();

                let is_background = resource_limiter.as_ref().is_some_and(|l| l.is_background());
                let fixed_level = if is_background {
                    // Background tasks always run at low priority in the pool.
                    Some(2)
                } else {
                    match priority {
                        CommandPri::High => Some(0),
                        CommandPri::Normal => None,
                        CommandPri::Low => Some(2),
                    }
                };
                let group_name = metadata.group_name().to_owned();
                let estimated_priority = resource_ctl
                    .as_ref()
                    .map_or(u64::MAX, |ctl| ctl.peek_priority_of(&metadata, priority));
                let mut extras = Extras::new_multilevel(task_id, fixed_level);
                extras.set_metadata(metadata.to_vec());
                // Clone gauge: one for inc (after admission), one inside the
                // future for dec (when the task completes).
                let gauge_for_spawn = running_task_gauge.clone();
                let task_cell = if let Some(resource_ctl) = resource_ctl {
                    let inner = ControlledFuture::new(
                        f.map(move |_| {
                            running_task_gauge.dec();
                        }),
                        resource_ctl.clone(),
                        group_name.clone(),
                    );
                    TaskCell::new(
                        TlsTrackedFuture::new(with_resource_limiter(
                            inner,
                            resource_limiter.clone(),
                            true, // skip compaction pressure for foreground jobs
                            true, // measure-only: build debt, never sleep inside pool
                            resource_manager.clone(),
                            0, // read path: no write bytes
                        )),
                        extras,
                    )
                } else {
                    TaskCell::new(
                        TlsTrackedFuture::new(f.map(move |_| {
                            running_task_gauge.dec();
                        })),
                        extras,
                    )
                };
                admission_and_enqueue(
                    resource_manager.clone(),
                    resource_limiter,
                    task_priority,
                    gauge_for_spawn,
                    *max_tasks,
                    remote.clone(),
                    task_cell,
                    running_tasks.to_vec(),
                    resource_ctl.clone(),
                    estimated_priority,
                )
                .boxed()
            }
            ReadPoolHandle::YatpFlowControl {
                remote,
                running_tasks,
                max_tasks,
                flow_controller,
                ..
            } => {
                let task_priority = TaskPriority::from(metadata.override_priority());
                let running_task_gauge = running_tasks[task_priority as usize].clone();

                let fixed_level = match priority {
                    CommandPri::High => Some(0),
                    CommandPri::Normal => None,
                    CommandPri::Low => Some(2),
                };
                let metadata = metadata.deep_clone();
                let remote = remote.clone();
                let running_tasks = running_tasks.to_vec();
                let flow_controller = flow_controller.clone();
                let max_tasks = *max_tasks;
                async move {
                    let flow_permit = flow_controller.acquire(flow_id).await;
                    let yatp_task_id = flow_permit.yatp_task_id(task_id);
                    let mut extras = Extras::new_multilevel(yatp_task_id, fixed_level);
                    extras.set_metadata(metadata.to_vec());
                    // Clone gauge: one for inc (after enqueue), one inside the
                    // future for dec (when the task completes).
                    let gauge_for_spawn = running_task_gauge.clone();
                    if gauge_for_spawn.get() as usize >= max_tasks {
                        let estimated_priority = flow_controller
                            .flow_priority_tag_by_task_id(yatp_task_id)
                            .or_else(|| fixed_level.map(u64::from))
                            .unwrap_or(u64::MAX);
                        if let Some(mut evicted) = remote.try_evict_lowest(estimated_priority) {
                            let evicted_prio =
                                priority_from_task_meta(evicted.mut_extras().metadata());
                            running_tasks[evicted_prio as usize].dec();
                            UNIFIED_READ_POOL_EVICTED_TASKS.inc();
                            drop(evicted);
                        } else {
                            return Err(ReadPoolError::UnifiedReadPoolFull);
                        }
                    }
                    let tracked_future =
                        FlowTrackedFuture::new(f, flow_permit, estimated_cpu, running_task_gauge);
                    let task_cell = TaskCell::new(TlsTrackedFuture::new(tracked_future), extras);
                    gauge_for_spawn.inc();
                    remote.spawn(task_cell);
                    Ok(())
                }
                .boxed()
            }
        }
    }

    pub fn spawn_handle<F, T>(
        &self,
        f: F,
        priority: CommandPri,
        task_id: u64,
        metadata: TaskMetadata<'_>,
        resource_limiter: Option<Arc<ResourceLimiter>>,
    ) -> impl Future<Output = Result<T, ReadPoolError>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.spawn_handle_with_flow(f, priority, task_id, metadata, resource_limiter, None)
    }

    pub fn spawn_handle_with_flow<F, T>(
        &self,
        f: F,
        priority: CommandPri,
        task_id: u64,
        metadata: TaskMetadata<'_>,
        resource_limiter: Option<Arc<ResourceLimiter>>,
        flow_id: Option<ReadFlowId>,
    ) -> impl Future<Output = Result<T, ReadPoolError>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.spawn_handle_with_flow_and_estimated_cpu(
            f,
            priority,
            task_id,
            metadata,
            resource_limiter,
            flow_id,
            Duration::ZERO,
        )
    }

    pub fn spawn_handle_with_flow_and_estimated_cpu<F, T>(
        &self,
        f: F,
        priority: CommandPri,
        task_id: u64,
        metadata: TaskMetadata<'_>,
        resource_limiter: Option<Arc<ResourceLimiter>>,
        flow_id: Option<ReadFlowId>,
        estimated_cpu: Duration,
    ) -> impl Future<Output = Result<T, ReadPoolError>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel::<T>();
        let spawn_fut = self.spawn_with_flow_and_estimated_cpu(
            f.map(move |res| {
                let _ = tx.send(res);
            }),
            priority,
            task_id,
            metadata,
            resource_limiter,
            flow_id,
            estimated_cpu,
        );
        async move {
            spawn_fut.await?;
            rx.map_err(ReadPoolError::from).await
        }
    }

    pub fn get_normal_pool_size(&self) -> usize {
        match self {
            ReadPoolHandle::FuturePools {
                read_pool_normal, ..
            } => read_pool_normal.get_pool_size(),
            ReadPoolHandle::Yatp { pool_size, .. }
            | ReadPoolHandle::YatpFlowControl { pool_size, .. } => *pool_size,
        }
    }

    pub fn get_queue_size_per_worker(&self) -> usize {
        match self {
            ReadPoolHandle::FuturePools {
                read_pool_normal, ..
            } => read_pool_normal.get_running_task_count() / read_pool_normal.get_pool_size(),
            ReadPoolHandle::Yatp {
                running_tasks,
                pool_size,
                ..
            }
            | ReadPoolHandle::YatpFlowControl {
                running_tasks,
                pool_size,
                ..
            } => running_tasks.iter().map(|r| r.get()).sum::<i64>() as usize / *pool_size,
        }
    }

    pub fn scale_pool_size(&mut self, max_thread_count: usize) {
        match self {
            ReadPoolHandle::FuturePools { .. } => {
                unreachable!()
            }
            ReadPoolHandle::Yatp {
                remote,
                running_threads,
                max_tasks,
                pool_size,
                ..
            }
            | ReadPoolHandle::YatpFlowControl {
                remote,
                running_threads,
                max_tasks,
                pool_size,
                ..
            } => {
                remote.scale_workers(max_thread_count);
                *max_tasks = max_tasks
                    .saturating_div(*pool_size)
                    .saturating_mul(max_thread_count);
                running_threads.set(max_thread_count as i64);
                *pool_size = max_thread_count;
            }
        }
    }

    pub fn set_max_tasks_per_worker(&mut self, tasks_per_thread: usize) {
        match self {
            ReadPoolHandle::FuturePools { .. } => {
                unreachable!()
            }
            ReadPoolHandle::Yatp {
                max_tasks,
                pool_size,
                ..
            }
            | ReadPoolHandle::YatpFlowControl {
                max_tasks,
                pool_size,
                ..
            } => {
                *max_tasks = tasks_per_thread.saturating_mul(*pool_size);
            }
        }
    }

    pub fn get_ewma_time_slice(&self) -> Option<Duration> {
        match self {
            ReadPoolHandle::FuturePools { .. } => None,
            ReadPoolHandle::Yatp {
                time_slice_inspector,
                ..
            }
            | ReadPoolHandle::YatpFlowControl {
                time_slice_inspector,
                ..
            } => Some(time_slice_inspector.get_ewma_time_slice()),
        }
    }

    pub fn update_ewma_time_slice(&self) {
        if let ReadPoolHandle::Yatp {
            time_slice_inspector,
            ..
        }
        | ReadPoolHandle::YatpFlowControl {
            time_slice_inspector,
            ..
        } = self
        {
            time_slice_inspector.update();
        }
    }

    pub fn get_estimated_wait_duration(&self) -> Option<Duration> {
        self.get_ewma_time_slice()
            .map(|s| s * (self.get_queue_size_per_worker() as u32))
    }

    pub fn check_busy_threshold(
        &self,
        busy_threshold: Duration,
    ) -> Result<(), errorpb::ServerIsBusy> {
        if busy_threshold.is_zero() {
            return Ok(());
        }
        let estimated_wait = match self.get_estimated_wait_duration() {
            Some(estimated_wait) if estimated_wait > busy_threshold => estimated_wait,
            _ => return Ok(()),
        };
        // TODO: Get applied_index from the raftstore and check memory locks. Then, we
        // can skip read index in replica read. But now the difficulty is that we don't
        // have access to the the local reader in gRPC threads.
        let mut busy_err = errorpb::ServerIsBusy::default();
        busy_err.set_reason("estimated wait time exceeds threshold".to_owned());
        busy_err.estimated_wait_ms = u32::try_from(estimated_wait.as_millis()).unwrap_or(u32::MAX);
        warn!("Already many pending tasks in the read queue, task is rejected";
            "busy_threshold" => ?&busy_threshold,
            "busy_err" => ?&busy_err,
        );
        Err(busy_err)
    }
}

pub const UPDATE_EWMA_TIME_SLICE_INTERVAL: Duration = Duration::from_millis(200);

pub struct TimeSliceInspector {
    // `atomic_ewma_nanos` is a mirror of `inner.ewma` provided for fast access. It is updated in
    // the `update` method.
    atomic_ewma_nanos: AtomicU64,
    inner: Mutex<TimeSliceInspectorInner>,
}

struct TimeSliceInspectorInner {
    time_slice_hist: [Histogram; 3],
    ewma: Duration,

    last_sum: Duration,
    last_count: u64,
}

impl TimeSliceInspector {
    pub fn new(name: &str) -> Self {
        let time_slice_hist = [
            yatp::metrics::TASK_POLL_DURATION.with_label_values(&[name, "0"]),
            yatp::metrics::TASK_POLL_DURATION.with_label_values(&[name, "1"]),
            yatp::metrics::TASK_POLL_DURATION.with_label_values(&[name, "2"]),
        ];
        let inner = TimeSliceInspectorInner {
            time_slice_hist,
            ewma: Duration::default(),
            last_sum: Duration::default(),
            last_count: 0,
        };
        Self {
            atomic_ewma_nanos: AtomicU64::default(),
            inner: Mutex::new(inner),
        }
    }

    pub fn update(&self) {
        // new_ewma = WEIGHT * new_val + (1 - WEIGHT) * old_ewma
        const WEIGHT: f64 = 0.3;
        // If the accumulated time slice is less than 100ms, the EWMA is not updated.
        const MIN_TIME_DIFF: Duration = Duration::from_millis(100);

        let mut inner = self.inner.lock().unwrap();
        let mut new_sum = Duration::default();
        let mut new_count = 0;
        // Now, we simplify the problem by merging samples from all levels. If we want
        // more accurate answer in the future, calculate for each level separately.
        for hist in &inner.time_slice_hist {
            // Call `metric` to get a consistent snapshot of sum and count.
            let metric_proto = hist.metric();
            let hist_proto = metric_proto.get_histogram();
            new_sum += Duration::from_secs_f64(hist_proto.get_sample_sum());
            new_count += hist_proto.get_sample_count();
        }
        let time_diff = new_sum.saturating_sub(inner.last_sum);
        let count_diff = new_count.saturating_sub(inner.last_count);
        if time_diff < MIN_TIME_DIFF || count_diff == 0 {
            return;
        }
        let new_val = time_diff / ((new_count - inner.last_count) as u32);
        let new_ewma = new_val.mul_f64(WEIGHT) + inner.ewma.mul_f64(1.0 - WEIGHT);
        inner.ewma = new_ewma;
        inner.last_sum = new_sum;
        inner.last_count = new_count;

        self.atomic_ewma_nanos
            .store(new_ewma.as_nanos() as u64, Ordering::Release);
    }

    pub fn get_ewma_time_slice(&self) -> Duration {
        Duration::from_nanos(self.atomic_ewma_nanos.load(Ordering::Acquire))
    }
}

#[derive(Clone)]
pub struct ReporterTicker<R: FlowStatsReporter> {
    reporter: R,
}

impl<R: FlowStatsReporter> PoolTicker for ReporterTicker<R> {
    fn on_tick(&mut self) {
        self.flush_metrics_on_tick();
    }
}

impl<R: FlowStatsReporter> ReporterTicker<R> {
    fn flush_metrics_on_tick(&mut self) {
        crate::storage::metrics::tls_flush(&self.reporter);
        crate::coprocessor::metrics::tls_flush(&self.reporter);
    }
}

#[cfg(test)]
fn get_unified_read_pool_name() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!(
        "unified-read-pool-test-{}",
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(not(test))]
fn get_unified_read_pool_name() -> String {
    UNIFIED_READ_POOL_THREAD.to_string()
}

#[inline]
pub fn build_yatp_read_pool<E: Engine, R: FlowStatsReporter>(
    config: &UnifiedReadPoolConfig,
    reporter: R,
    engine: E,
    resource_ctl: Option<Arc<ResourceController>>,
    resource_manager: Option<Arc<ResourceGroupManager>>,
    cleanup_method: CleanupMethod,
    enable_task_wait_metrics: bool,
) -> ReadPool {
    let unified_read_pool_name = get_unified_read_pool_name();
    build_yatp_read_pool_with_name(
        config,
        reporter,
        engine,
        resource_ctl,
        resource_manager,
        cleanup_method,
        unified_read_pool_name,
        enable_task_wait_metrics,
    )
}

pub fn build_yatp_read_pool_with_name<E: Engine, R: FlowStatsReporter>(
    config: &UnifiedReadPoolConfig,
    reporter: R,
    engine: E,
    resource_ctl: Option<Arc<ResourceController>>,
    resource_manager: Option<Arc<ResourceGroupManager>>,
    cleanup_method: CleanupMethod,
    unified_read_pool_name: String,
    enable_task_wait_metrics: bool,
) -> ReadPool {
    let raftkv = Arc::new(Mutex::new(engine));
    let builder = YatpPoolBuilder::new(ReporterTicker { reporter })
        .name_prefix(&unified_read_pool_name)
        .cleanup_method(cleanup_method)
        .stack_size(config.stack_size.0 as usize)
        .thread_count(
            1, // min_thread_count is controlled by readPoolConfigRunner
            config.max_thread_count,
            std::cmp::max(
                std::cmp::max(
                    UNIFIED_READPOOL_MIN_CONCURRENCY,
                    SysQuota::cpu_cores_quota() as usize,
                ),
                config.max_thread_count,
            ),
        )
        .after_start(move || {
            let engine = raftkv.lock().unwrap().clone();
            set_tls_engine(engine);
            set_io_type(IoType::ForegroundRead);
        })
        .before_stop(|| unsafe {
            destroy_tls_engine::<E>();
        })
        .enable_task_wait_metrics(enable_task_wait_metrics);

    let enable_flow_control = config.enable_flow_fairness;
    let flow_controller =
        enable_flow_control.then(|| ReadFlowController::new(config.max_flow_concurrency));
    let pool = if enable_flow_control {
        builder.build_priority_pool(Arc::new(FlowPriorityProvider::new(
            flow_controller.as_ref().unwrap().clone(),
        )))
    } else if let Some(ref r) = resource_ctl {
        builder.build_priority_pool(r.clone())
    } else {
        builder.build_multi_level_pool()
    };
    let time_slice_inspector = Arc::new(TimeSliceInspector::new(&unified_read_pool_name));
    let running_tasks = TaskPriority::priorities().map(|p| {
        UNIFIED_READ_POOL_RUNNING_TASKS.with_label_values(&[&unified_read_pool_name, p.as_str()])
    });
    let running_threads = {
        let running_threads =
            UNIFIED_READ_POOL_RUNNING_THREADS.with_label_values(&[&unified_read_pool_name]);
        running_threads.set(config.max_thread_count as _);
        running_threads
    };
    let max_tasks = config
        .max_tasks_per_worker
        .saturating_mul(config.max_thread_count);
    let pool_size = config.max_thread_count;

    if let Some(flow_controller) = flow_controller {
        spawn_read_flow_scanner(&pool, flow_controller.clone());
        ReadPool::YatpFlowControl {
            pool,
            running_tasks,
            running_threads,
            max_tasks,
            pool_size,
            time_slice_inspector,
            flow_controller,
        }
    } else {
        ReadPool::Yatp {
            pool,
            running_tasks,
            running_threads,
            max_tasks,
            pool_size,
            resource_ctl,
            resource_manager,
            time_slice_inspector,
        }
    }
}

pub fn build_yatp_flow_control_read_pool(
    config: &UnifiedReadPoolConfig,
    unified_read_pool_name: String,
    cleanup_method: CleanupMethod,
    enable_task_wait_metrics: bool,
) -> ReadPool {
    let flow_controller = ReadFlowController::new(config.max_flow_concurrency);
    let pool = YatpPoolBuilder::new(DefaultTicker::default())
        .name_prefix(&unified_read_pool_name)
        .cleanup_method(cleanup_method)
        .stack_size(config.stack_size.0 as usize)
        .thread_count(
            config.max_thread_count,
            config.max_thread_count,
            config.max_thread_count,
        )
        .enable_task_wait_metrics(enable_task_wait_metrics)
        .build_priority_pool(Arc::new(FlowPriorityProvider::new(flow_controller.clone())));
    spawn_read_flow_scanner(&pool, flow_controller.clone());
    let time_slice_inspector = Arc::new(TimeSliceInspector::new(&unified_read_pool_name));
    let running_tasks = TaskPriority::priorities().map(|p| {
        UNIFIED_READ_POOL_RUNNING_TASKS.with_label_values(&[&unified_read_pool_name, p.as_str()])
    });
    let running_threads = {
        let running_threads =
            UNIFIED_READ_POOL_RUNNING_THREADS.with_label_values(&[&unified_read_pool_name]);
        running_threads.set(config.max_thread_count as _);
        running_threads
    };
    let max_tasks = config
        .max_tasks_per_worker
        .saturating_mul(config.max_thread_count);
    let pool_size = config.max_thread_count;

    ReadPool::YatpFlowControl {
        pool,
        running_tasks,
        running_threads,
        max_tasks,
        pool_size,
        time_slice_inspector,
        flow_controller,
    }
}

impl From<Vec<FuturePool>> for ReadPool {
    fn from(mut v: Vec<FuturePool>) -> ReadPool {
        assert_eq!(v.len(), 3);
        let read_pool_high = v.remove(2);
        let read_pool_normal = v.remove(1);
        let read_pool_low = v.remove(0);
        ReadPool::FuturePools {
            read_pool_high,
            read_pool_normal,
            read_pool_low,
        }
    }
}

struct ReadPoolCpuTimeTracker {
    yatp_total_time_elapsed: IntCounter,
    // the total time duration of each thread busy with handling tasks. This time also includes
    // the time when the threads are off-cpu, so it might be much higher than the actual cpu time.
    prev_total_task_handling_time_us: u64,
    prev_thread_check_time: Instant,
    prev_thread_usage_per_sec: f64,
    prev_total_cpu_time: u64,
    prev_cpu_check_time: Instant,
    prev_cpu_usage_per_second: f64,
}

impl ReadPoolCpuTimeTracker {
    fn new(pool_name: &str) -> Self {
        let now = Instant::now_coarse();
        let yatp_total_time_elapsed = MULTILEVEL_LEVEL_ELAPSED
            .get_metric_with_label_values(&[pool_name, "total"])
            .unwrap();
        let prev_total_task_handling_time_us = yatp_total_time_elapsed.get();

        Self {
            yatp_total_time_elapsed,
            prev_total_task_handling_time_us,
            prev_thread_check_time: now,
            prev_thread_usage_per_sec: 0.0,
            prev_total_cpu_time: 0,
            prev_cpu_check_time: now,
            prev_cpu_usage_per_second: 0.0,
        }
    }

    /// Get actual CPU usage of unified read pool threads using kernel thread
    /// stats.
    fn get_unified_read_pool_cpu(&mut self) -> f64 {
        #[cfg(test)]
        {
            // In test mode, return the manually set value if available
            if self.prev_cpu_usage_per_second > 0.0 {
                return self.prev_cpu_usage_per_second;
            }
        }
        use tikv_util::sys::thread::{full_thread_stat, ticks_per_second};

        let check_time = Instant::now_coarse();
        let duration = check_time.saturating_duration_since(self.prev_cpu_check_time);

        // Minimum duration check to avoid noise - if too soon, return prev value
        if duration < Duration::from_millis(500) {
            return self.prev_cpu_usage_per_second;
        }

        let mut current_total_cpu_time = 0i64;
        let pid = tikv_util::sys::thread::process_id();
        let tids: Vec<_> = tikv_util::sys::thread::thread_ids(pid).unwrap();
        // Collect CPU stats for each cached read pool worker thread
        for &tid in &tids {
            if let Ok(stat) = full_thread_stat(pid, tid) {
                // Look for unified read pool thread name pattern
                if matches_thread_name_prefix(&stat.command, UNIFIED_READ_POOL_THREAD) {
                    // Sum utime + stime (user + system time)
                    current_total_cpu_time += stat.utime + stat.stime;
                }
            }
        }

        // Calculate CPU time difference since last check
        let cpu_time_diff = current_total_cpu_time.saturating_sub(self.prev_total_cpu_time as i64);

        let cpu_utilization = if duration.as_secs_f64() > 0.0 && cpu_time_diff > 0 {
            // Convert CPU time to seconds using ticks_per_second
            let cpu_seconds = (cpu_time_diff as f64) / (ticks_per_second() as f64);
            let wall_seconds = duration.as_secs_f64();
            cpu_seconds / wall_seconds
        } else {
            0.0
        };

        self.prev_total_cpu_time = current_total_cpu_time as u64;
        self.prev_cpu_check_time = check_time;
        self.prev_cpu_usage_per_second = cpu_utilization;

        cpu_utilization
    }

    #[cfg(test)]
    fn set_test_cpu_utilization(&mut self, cpu: f64) {
        self.prev_cpu_usage_per_second = cpu;
    }

    /// Baseline thread usage measurement using yatp metrics (includes off-CPU
    /// time)
    fn prev_avg_thread_usage(&mut self) -> f64 {
        let check_time = Instant::now_coarse();
        let duration = check_time.saturating_duration_since(self.prev_thread_check_time);
        // if the check duration is too small, just return the latest cached value.
        if duration < Duration::from_millis(100) {
            return self.prev_thread_usage_per_sec;
        }
        let total_thread_time = self.yatp_total_time_elapsed.get();
        let total_thread_usage_per_sec = (total_thread_time - self.prev_total_task_handling_time_us)
            as f64
            / duration.as_micros() as f64;
        self.prev_total_task_handling_time_us = total_thread_time;
        self.prev_thread_check_time = check_time;
        self.prev_thread_usage_per_sec = total_thread_usage_per_sec;
        total_thread_usage_per_sec
    }
}
struct ReadPoolConfigRunner {
    interval: Duration,
    sender: SyncSender<usize>,
    handle: ReadPoolHandle,
    cpu_time_tracker: ReadPoolCpuTimeTracker,
    process_stats: ProcessStat,
    min_thread_count: usize,
    // configed thread pool size, it's the min thread count to be scale. It is set to
    // max_thread_count
    core_thread_count: usize,
    // the max thread count can be scaled
    max_thread_count: usize,
    // the current active thread count
    cur_thread_count: usize,
    auto_adjust: bool,
    // CPU threshold from configuration (0 means disabled, RFC 0114)
    cpu_threshold: f64,
}

impl Runnable for ReadPoolConfigRunner {
    type Task = Task;
    fn run(&mut self, task: Self::Task) {
        match task {
            Task::PoolSize(s) => {
                if s != self.core_thread_count {
                    self.handle.scale_pool_size(s);
                    self.core_thread_count = s;
                    self.cur_thread_count = s;
                    self.notify_pool_size_change(s);
                }
            }
            Task::AutoAdjust(s) => {
                self.auto_adjust = s;
                // when auto adjust is disabled, reset to the config pool size.
                if !s && self.cur_thread_count != self.core_thread_count {
                    self.handle.scale_pool_size(self.core_thread_count);
                    self.cur_thread_count = self.core_thread_count;
                }
            }
            Task::MaxTasksPerWorker(s) => {
                self.handle.set_max_tasks_per_worker(s);
            }
            Task::CpuThreshold(s) => {
                self.cpu_threshold = s;
            }
        }
    }
}

impl RunnableWithTimer for ReadPoolConfigRunner {
    fn get_interval(&self) -> Duration {
        self.interval
    }

    fn on_timeout(&mut self) {
        self.adjust_pool_size();
    }
}

impl ReadPoolConfigRunner {
    fn running_tasks(&self) -> i64 {
        match &self.handle {
            ReadPoolHandle::Yatp { running_tasks, .. } => {
                running_tasks.iter().map(|r| r.get()).sum()
            }
            _ => unreachable!(),
        }
    }

    // Adjust pool size using based on thread utilization or cpu utilization.
    fn adjust_pool_size(&mut self) {
        if !self.auto_adjust {
            return;
        }

        let read_pool_cpu = self.cpu_time_tracker.get_unified_read_pool_cpu();
        let thread_usage = self.cpu_time_tracker.prev_avg_thread_usage();
        let running_tasks = self.running_tasks();
        let process_cpu = match self.process_stats.cpu_usage() {
            Ok(p) => p,
            Err(e) => {
                warn!("fetch process cpu usage failed"; "err" => ?e);
                return;
            }
        };
        let target_cpu_cores = if self.cpu_threshold > 0.0 {
            self.cpu_threshold * SysQuota::cpu_cores_quota()
        } else {
            SysQuota::cpu_cores_quota()
        };

        // Base scaling conditions (process CPU, thread usage, task queue depth)
        let busy_thread_scale_out = self.cur_thread_count < self.max_thread_count
            && process_cpu * (self.cur_thread_count as f64 + 1.0) / (self.cur_thread_count as f64)
                < target_cpu_cores
            && thread_usage > self.cur_thread_count as f64 * READ_POOL_THREAD_HIGH_THRESHOLD
            && running_tasks > self.cur_thread_count as i64 * RUNNING_TASKS_PER_THREAD_THRESHOLD;

        let busy_thread_scale_in = self.cur_thread_count > self.min_thread_count
            && thread_usage < (self.cur_thread_count - 1) as f64 * READ_POOL_THREAD_LOW_THRESHOLD
            && running_tasks < self.cur_thread_count as i64 * RUNNING_TASKS_PER_THREAD_THRESHOLD;

        let leeway = 0.1;
        let busy_cpu_scale_in =
            self.cpu_threshold > 0.0 && read_pool_cpu > (leeway + 1.0) * target_cpu_cores;
        let busy_cpu_scale_out = read_pool_cpu < (1.0 - leeway) * target_cpu_cores
            && self.cur_thread_count < self.core_thread_count;

        let new_thread_count = if busy_cpu_scale_in {
            // CPU threshold takes precedence over busy thread scaling conditions
            std::cmp::max(
                std::cmp::max(
                    (target_cpu_cores).floor() as usize,
                    ((self.cur_thread_count as f64) * target_cpu_cores / read_pool_cpu).floor()
                        as usize,
                ),
                1, // minimum 1 running thread
            )
        } else if busy_cpu_scale_out {
            self.cur_thread_count + 1
        } else if busy_thread_scale_in {
            self.cur_thread_count - 1
        } else if busy_thread_scale_out {
            self.cur_thread_count + 1
        } else {
            self.cur_thread_count
        };

        if new_thread_count != self.cur_thread_count {
            self.handle.scale_pool_size(new_thread_count);
            self.notify_pool_size_change(new_thread_count);
            self.cur_thread_count = new_thread_count;
        }
    }

    fn notify_pool_size_change(&self, new_thread_count: usize) {
        // it's unlikely to send failed.
        if let Err(e) = self.sender.try_send(new_thread_count) {
            warn!("notify read pool thread count change failed"; "err" => ?e);
        }
    }
}

enum Task {
    PoolSize(usize),
    AutoAdjust(bool),
    MaxTasksPerWorker(usize),
    CpuThreshold(f64),
}

impl std::fmt::Display for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Task::PoolSize(s) => write!(f, "PoolSize({})", *s),
            Task::AutoAdjust(s) => write!(f, "AutoAdjust({})", *s),
            Task::MaxTasksPerWorker(s) => write!(f, "MaxTasksPerWorker({})", *s),
            Task::CpuThreshold(s) => write!(f, "CpuThreshold({})", *s),
        }
    }
}

pub struct ReadPoolConfigManager {
    scheduler: Scheduler<Task>,
}

impl ReadPoolConfigManager {
    pub fn new(
        handle: ReadPoolHandle,
        sender: SyncSender<usize>,
        worker: &Worker,
        min_thread_count: usize,
        max_thread_count: usize,
        auto_adjust: bool,
        cpu_threshold: f64,
    ) -> Self {
        let runner = ReadPoolConfigRunner {
            interval: READ_POOL_THREAD_CHECK_DURATION,
            sender,
            handle,
            cpu_time_tracker: ReadPoolCpuTimeTracker::new(&get_unified_read_pool_name()),
            process_stats: ProcessStat::cur_proc_stat().unwrap(),
            min_thread_count,
            core_thread_count: max_thread_count,
            cur_thread_count: max_thread_count,
            max_thread_count,
            auto_adjust,
            cpu_threshold,
        };
        let scheduler = worker.start_with_timer("read-pool-config-worker", runner);

        Self { scheduler }
    }
}

impl Drop for ReadPoolConfigManager {
    fn drop(&mut self) {
        self.scheduler.stop();
    }
}

impl ConfigManager for ReadPoolConfigManager {
    fn dispatch(&mut self, change: ConfigChange) -> CfgResult<()> {
        if let Some(ConfigValue::Module(unified)) = change.get("unified") {
            if let Some(ConfigValue::Usize(max_thread_count)) = unified.get("max_thread_count") {
                self.scheduler.schedule(Task::PoolSize(*max_thread_count))?;
            }
            if let Some(ConfigValue::Bool(b)) = unified.get("auto_adjust_pool_size") {
                self.scheduler.schedule(Task::AutoAdjust(*b))?;
            }
            if let Some(ConfigValue::Usize(max_tasks)) = unified.get("max_tasks_per_worker") {
                self.scheduler
                    .schedule(Task::MaxTasksPerWorker(*max_tasks))?;
            }
            if let Some(ConfigValue::F64(cpu_threshold)) = unified.get("cpu_threshold") {
                self.scheduler
                    .schedule(Task::CpuThreshold(*cpu_threshold))?;
            }
        }
        info!(
            "readpool config changed";
            "change" => ?change,
        );
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ReadPoolError {
    #[error("{0}")]
    FuturePoolFull(#[from] yatp_pool::Full),

    #[error("Unified read pool is full")]
    UnifiedReadPoolFull,

    #[error("Request rejected by admission control")]
    Rejected,

    #[error("{0}")]
    Canceled(#[from] oneshot::Canceled),
}

mod metrics {
    use lazy_static::lazy_static;
    use prometheus::*;

    lazy_static! {
        pub static ref UNIFIED_READ_POOL_RUNNING_TASKS: IntGaugeVec = register_int_gauge_vec!(
            "tikv_unified_read_pool_running_tasks",
            "The number of running tasks in the unified read pool",
            &["name", "priority"]
        )
        .unwrap();
        pub static ref UNIFIED_READ_POOL_RUNNING_THREADS: IntGaugeVec = register_int_gauge_vec!(
            "tikv_unified_read_pool_thread_count",
            "The number of running threads in the unified read pool",
            &["name"]
        )
        .unwrap();
        pub static ref UNIFIED_READ_POOL_EVICTED_TASKS: IntCounter = register_int_counter!(
            "tikv_unified_read_pool_evicted_tasks",
            "Number of tasks evicted from the unified read pool by higher-priority tasks"
        )
        .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use futures::channel::oneshot;
    use futures_executor::block_on;
    use kvproto::kvrpcpb::ResourceControlContext;
    use raftstore::store::{ReadStats, WriteStats};
    use resource_control::ResourceGroupManager;

    use super::*;
    use crate::storage::TestEngineBuilder;

    fn record_flow_elapsed(permit: &ReadFlowPermit, duration: Duration) {
        let mut actual_cpu_us = 0;
        let mut charged_cpu_us = 0;
        permit.record_elapsed(duration, &mut actual_cpu_us, &mut charged_cpu_us);
    }

    #[derive(Clone)]
    struct DummyReporter;

    impl FlowStatsReporter for DummyReporter {
        fn report_read_stats(&self, _read_stats: ReadStats) {}
        fn report_write_stats(&self, _write_stats: WriteStats) {}
    }

    #[test]
    fn test_yatp_full() {
        let config = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: 2,
            max_tasks_per_worker: 1,
            ..Default::default()
        };
        // max running tasks number should be 2*1 = 2

        let engine = TestEngineBuilder::new().build().unwrap();
        let name = "test-yatp-full";
        let pool = build_yatp_read_pool_with_name(
            &config,
            DummyReporter,
            engine,
            None,
            None,
            CleanupMethod::InPlace,
            name.to_owned(),
            false,
        );

        let gen_task = || {
            let (tx, rx) = oneshot::channel::<()>();
            let task = async move {
                let _ = rx.await;
            };
            (task, tx)
        };

        let handle = pool.handle();
        let (task1, tx1) = gen_task();
        let (task2, _tx2) = gen_task();
        let (task3, _tx3) = gen_task();
        let (task4, _tx4) = gen_task();

        block_on(handle.spawn(task1, CommandPri::Normal, 1, TaskMetadata::default(), None))
            .unwrap();
        block_on(handle.spawn(task2, CommandPri::Normal, 2, TaskMetadata::default(), None))
            .unwrap();

        thread::sleep(Duration::from_millis(300));
        match block_on(handle.spawn(task3, CommandPri::Normal, 3, TaskMetadata::default(), None)) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }
        tx1.send(()).unwrap();

        thread::sleep(Duration::from_millis(300));
        block_on(handle.spawn(task4, CommandPri::Normal, 4, TaskMetadata::default(), None))
            .unwrap();
        assert_eq!(
            UNIFIED_READ_POOL_RUNNING_TASKS
                .with_label_values(&[name, "medium"])
                .get(),
            2
        );
    }

    #[test]
    fn test_yatp_scale_up() {
        let config = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: 2,
            max_tasks_per_worker: 1,
            ..Default::default()
        };
        // max running tasks number should be 2*1 = 2

        let engine = TestEngineBuilder::new().build().unwrap();
        let pool = build_yatp_read_pool(
            &config,
            DummyReporter,
            engine,
            None,
            None,
            CleanupMethod::InPlace,
            false,
        );

        let gen_task = || {
            let (tx, rx) = oneshot::channel::<()>();
            let task = async move {
                let _ = rx.await;
            };
            (task, tx)
        };

        let mut handle = pool.handle();
        let (task1, _tx1) = gen_task();
        let (task2, _tx2) = gen_task();
        let (task3, _tx3) = gen_task();
        let (task4, _tx4) = gen_task();
        let (task5, _tx5) = gen_task();

        block_on(handle.spawn(task1, CommandPri::Normal, 1, TaskMetadata::default(), None))
            .unwrap();
        block_on(handle.spawn(task2, CommandPri::Normal, 2, TaskMetadata::default(), None))
            .unwrap();

        thread::sleep(Duration::from_millis(300));
        match block_on(handle.spawn(task3, CommandPri::Normal, 3, TaskMetadata::default(), None)) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }

        handle.scale_pool_size(3);
        assert_eq!(handle.get_normal_pool_size(), 3);

        block_on(handle.spawn(task4, CommandPri::Normal, 4, TaskMetadata::default(), None))
            .unwrap();

        thread::sleep(Duration::from_millis(300));
        match block_on(handle.spawn(task5, CommandPri::Normal, 5, TaskMetadata::default(), None)) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }
    }

    #[test]
    fn test_yatp_scale_down() {
        let config = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: 2,
            max_tasks_per_worker: 1,
            ..Default::default()
        };
        // max running tasks number for each priority should be 2*1 = 2

        let engine = TestEngineBuilder::new().build().unwrap();
        let pool = build_yatp_read_pool(
            &config,
            DummyReporter,
            engine,
            None,
            None,
            CleanupMethod::InPlace,
            false,
        );

        let gen_task = || {
            let (tx, rx) = oneshot::channel::<()>();
            let task = async move {
                let _ = rx.await;
            };
            (task, tx)
        };

        let mut handle = pool.handle();
        let (task1, tx1) = gen_task();
        let (task2, tx2) = gen_task();
        let (task3, _tx3) = gen_task();
        let (task4, _tx4) = gen_task();
        let (task5, _tx5) = gen_task();

        block_on(handle.spawn(task1, CommandPri::Normal, 1, TaskMetadata::default(), None))
            .unwrap();
        block_on(handle.spawn(task2, CommandPri::Normal, 2, TaskMetadata::default(), None))
            .unwrap();

        thread::sleep(Duration::from_millis(300));
        match block_on(handle.spawn(task3, CommandPri::Normal, 3, TaskMetadata::default(), None)) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }

        // spawn a high-priority task, should not return Full error.
        let (task_high, tx_h) = gen_task();
        let mut ctx = ResourceControlContext::default();
        ctx.override_priority = 16; // high priority
        let metadata = TaskMetadata::from_ctx(&ctx);
        let f = handle.spawn_handle(task_high, CommandPri::Normal, 6, metadata, None);
        tx_h.send(()).unwrap();
        block_on(f).unwrap();

        tx1.send(()).unwrap();
        tx2.send(()).unwrap();
        thread::sleep(Duration::from_millis(300));

        handle.scale_pool_size(1);
        assert_eq!(handle.get_normal_pool_size(), 1);

        block_on(handle.spawn(task4, CommandPri::Normal, 4, TaskMetadata::default(), None))
            .unwrap();

        thread::sleep(Duration::from_millis(300));
        match block_on(handle.spawn(task5, CommandPri::Normal, 5, TaskMetadata::default(), None)) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }
    }

    #[test]
    fn test_time_slice_inspector_ewma() {
        const MARGIN: f64 = 1e-5; // 10us

        let name = "test_time_slice_inspector_ewma";
        let inspector = TimeSliceInspector::new(name);
        let hist = yatp::metrics::TASK_POLL_DURATION.with_label_values(&[name, "0"]);

        // avg: 0.055, prev_ewma: 0 => new_ewma = 0.0165
        for i in 1..=10 {
            hist.observe(i as f64 * 0.01);
        }
        inspector.update();
        let ewma = inspector.get_ewma_time_slice().as_secs_f64();
        assert!((ewma - 0.0165).abs() < MARGIN);

        // avg: 0.0125, prev_ewma: 0.0165 => new_ewma = 0.0153
        for i in 5..=20 {
            hist.observe(i as f64 * 0.001);
        }
        inspector.update();
        let ewma = inspector.get_ewma_time_slice().as_secs_f64();
        assert!((ewma - 0.0153).abs() < MARGIN);

        // sum: 55ms, don't update ewma
        for i in 1..=10 {
            hist.observe(i as f64 * 0.001);
        }
        inspector.update();
        let ewma = inspector.get_ewma_time_slice().as_secs_f64();
        assert!((ewma - 0.0153).abs() < MARGIN);

        // avg: 0.00786, prev_ewma: 0.0153 => new_ewma = 0.01307
        for i in 5..=15 {
            hist.observe(i as f64 * 0.001);
        }
        inspector.update();
        let ewma = inspector.get_ewma_time_slice().as_secs_f64();
        assert!((ewma - 0.01307).abs() < MARGIN);
    }

    #[test]
    fn test_config_validation_cpu_threshold() {
        use tikv_util::config::ReadableSize;
        // Valid cpu_threshold
        let valid_config = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: 2,
            stack_size: ReadableSize::mb(2),
            max_tasks_per_worker: 2,
            auto_adjust_pool_size: true,
            cpu_threshold: 0.7,
            enable_flow_fairness: false,
            max_flow_concurrency: 0,
        };
        // Just verify config can be created
        assert_eq!(valid_config.cpu_threshold, 0.7);

        // Test disabled threshold (0.0)
        let disabled_config = UnifiedReadPoolConfig {
            cpu_threshold: 0.0,
            ..valid_config
        };
        assert_eq!(disabled_config.cpu_threshold, 0.0);
    }

    #[test]
    fn test_cpu_threshold_scale_down_and_up() {
        use tikv_util::worker::Worker;

        let min_thread_count = (0.8 * SysQuota::cpu_cores_quota()) as usize;
        let max_thread_count = SysQuota::cpu_cores_quota() as usize;
        let config = UnifiedReadPoolConfig {
            min_thread_count,
            max_thread_count,
            max_tasks_per_worker: 4,
            cpu_threshold: 0.6, // 60% threshold
            auto_adjust_pool_size: true,
            ..Default::default()
        };

        let engine = TestEngineBuilder::new().build().unwrap();
        let pool = build_yatp_read_pool(
            &config,
            DummyReporter,
            engine,
            None,
            None,
            CleanupMethod::InPlace,
            false,
        );

        let handle = pool.handle();
        let worker = Worker::new("test-worker");

        // Create ReadPoolConfigRunner with a real CPU tracker first
        let mut runner = ReadPoolConfigRunner {
            interval: Duration::from_secs(10),
            sender: std::sync::mpsc::sync_channel(10).0,
            handle: handle.clone(),
            cpu_time_tracker: ReadPoolCpuTimeTracker::new("test-pool"),
            process_stats: ProcessStat::cur_proc_stat().unwrap(),
            min_thread_count: config.min_thread_count,
            core_thread_count: config.min_thread_count,
            cur_thread_count: config.min_thread_count,
            max_thread_count: config.max_thread_count,
            auto_adjust: true,
            cpu_threshold: config.cpu_threshold,
        };

        assert_eq!(runner.cur_thread_count, min_thread_count);

        // Test 1: Set high CPU utilization using test helper
        runner
            .cpu_time_tracker
            .set_test_cpu_utilization(0.8 * SysQuota::cpu_cores_quota());

        let initial_threads = runner.cur_thread_count;
        runner.adjust_pool_size(); // Call the REAL adjust_pool_size method

        assert!(
            runner.cur_thread_count < initial_threads,
            "Thread count should decrease when CPU usage is high. Before: {}, After: {}",
            initial_threads,
            runner.cur_thread_count
        );

        // Test 2: Set low CPU utilization to test scale up
        runner
            .cpu_time_tracker
            .set_test_cpu_utilization(0.3 * num_cpus::get() as f64);
        let before_scale_up = runner.cur_thread_count;

        runner.adjust_pool_size();

        // Should not scale down further when CPU is low
        if before_scale_up < runner.core_thread_count {
            assert!(
                runner.cur_thread_count >= before_scale_up,
                "Thread count should not decrease further when CPU is low"
            );
        }

        worker.stop();
    }

    #[test]
    fn test_yatp_task_poll_duration_metric() {
        let count_metric = |name: &str| -> u64 {
            let mut sum = 0;
            for i in 0..=2 {
                let hist =
                    yatp::metrics::TASK_POLL_DURATION.with_label_values(&[name, &format!("{}", i)]);
                sum += hist.get_sample_count();
            }
            sum
        };

        for control in [false, true] {
            let name = format!("test_yatp_task_poll_duration_metric_{}", control);
            let (resource_ctl, resource_manager) = if control {
                let rm = Arc::new(ResourceGroupManager::default());
                let ctl = rm.derive_controller(name.clone(), true);
                (Some(ctl), Some(rm))
            } else {
                (None, None)
            };
            let config = UnifiedReadPoolConfig {
                min_thread_count: 1,
                max_thread_count: 2,
                max_tasks_per_worker: 1,
                ..Default::default()
            };

            let engine = TestEngineBuilder::new().build().unwrap();

            let pool = build_yatp_read_pool_with_name(
                &config,
                DummyReporter,
                engine,
                resource_ctl,
                resource_manager,
                CleanupMethod::InPlace,
                name.clone(),
                false,
            );

            let gen_task = || {
                let (tx, rx) = oneshot::channel::<()>();
                let task = async move {
                    // sleep the thread 100ms to trigger flushing the metrics.
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    let _ = rx.await;
                };
                (task, tx)
            };

            let handle = pool.handle();
            let (task1, tx1) = gen_task();
            let (task2, tx2) = gen_task();

            block_on(handle.spawn(task1, CommandPri::Normal, 1, TaskMetadata::default(), None))
                .unwrap();
            block_on(handle.spawn(task2, CommandPri::Normal, 2, TaskMetadata::default(), None))
                .unwrap();

            tx1.send(()).unwrap();
            tx2.send(()).unwrap();

            thread::sleep(Duration::from_millis(300));
            assert_eq!(count_metric(&name), 2);
            drop(pool);
        }
    }

    // Duplicated from resource_control::resource_group::tests which is
    // #[cfg(test)] pub(crate) and not accessible from this crate.
    fn new_resource_group_ru(
        name: String,
        ru: u64,
        group_priority: u32,
    ) -> kvproto::resource_manager::ResourceGroup {
        use kvproto::resource_manager::{GroupMode, GroupRequestUnitSettings, ResourceGroup};
        let mut group = ResourceGroup::new();
        group.set_name(name);
        group.set_mode(GroupMode::RuMode);
        group.set_priority(group_priority);
        let mut ru_setting = GroupRequestUnitSettings::new();
        ru_setting.mut_r_u().mut_settings().set_fill_rate(ru);
        group.set_r_u_settings(ru_setting);
        group
    }

    #[test]
    fn test_yatp_eviction() {
        // Test that when the read pool is full, a higher-priority incoming task
        // can evict the lowest-priority queued task.
        //
        // Strategy: Use 1 worker thread and max_tasks_per_worker=4 (total=4).
        // Spawn 1 blocking task to occupy the only worker thread, then spawn
        // 3 low-priority tasks that will sit in the queue. The pool is now
        // "full" (4 running_tasks). A high-priority task should evict one of
        // the queued low-priority tasks.
        //
        // Note on the two priority systems:
        // - `override_priority` (in ResourceControlContext) determines the TaskPriority
        //   bucket (High/Medium/Low) used for running_tasks counters. Both groups use 0
        //   here, so all tasks are "medium".
        // - Resource group priority (1 vs 16) is what peek_priority_of uses for the
        //   eviction comparison. "high_group" (priority=16) produces a numerically
        //   smaller value than "low_group" (priority=1), meaning it is scheduled first
        //   and can evict low_group tasks.
        let resource_manager = Arc::new(ResourceGroupManager::default());
        let low_group = new_resource_group_ru("low_group".into(), 5000, 1);
        resource_manager.add_resource_group(low_group);
        let high_group = new_resource_group_ru("high_group".into(), 5000, 16);
        resource_manager.add_resource_group(high_group);

        let name = "test-yatp-eviction";
        let resource_ctl = resource_manager.derive_controller(name.into(), true);

        let config = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: 1,
            max_tasks_per_worker: 4,
            ..Default::default()
        };

        let engine = TestEngineBuilder::new().build().unwrap();
        let pool = build_yatp_read_pool_with_name(
            &config,
            DummyReporter,
            engine,
            Some(resource_ctl),
            Some(resource_manager),
            CleanupMethod::InPlace,
            name.to_owned(),
            false,
        );

        let gen_task = || {
            let (tx, rx) = oneshot::channel::<()>();
            let task = async move {
                let _ = rx.await;
            };
            (task, tx)
        };

        let handle = pool.handle();

        let low_ctx = ResourceControlContext {
            resource_group_name: "low_group".to_string(),
            override_priority: 0,
            ..Default::default()
        };

        // Task 1: synchronously blocks the only worker thread so that it
        // cannot pop any further tasks from the global priority queue. An
        // async-only future (oneshot::channel::await) would return Pending
        // immediately, letting the worker loop back and drain tasks 2-4 from
        // the queue before the eviction attempt — causing a race.
        let (block_tx, block_rx) = std::sync::mpsc::channel::<()>();
        let task1 = async move {
            let _ = block_rx.recv();
        };
        block_on(handle.spawn(
            task1,
            CommandPri::Normal,
            1,
            TaskMetadata::from_ctx(&low_ctx),
            None,
        ))
        .unwrap();

        // Wait for task1 to be picked up and block the worker.
        thread::sleep(Duration::from_millis(300));

        // Tasks 2-4: these will sit in the global queue since the worker is
        // blocked by task1.
        let (task2, _tx2) = gen_task();
        let (task3, _tx3) = gen_task();
        let (task4, _tx4) = gen_task();

        block_on(handle.spawn(
            task2,
            CommandPri::Normal,
            2,
            TaskMetadata::from_ctx(&low_ctx),
            None,
        ))
        .unwrap();
        block_on(handle.spawn(
            task3,
            CommandPri::Normal,
            3,
            TaskMetadata::from_ctx(&low_ctx),
            None,
        ))
        .unwrap();
        block_on(handle.spawn(
            task4,
            CommandPri::Normal,
            4,
            TaskMetadata::from_ctx(&low_ctx),
            None,
        ))
        .unwrap();

        // Verify pool is full: spawning another low-priority task should fail.
        let (task_low5, _tx_low5) = gen_task();
        match block_on(handle.spawn(
            task_low5,
            CommandPri::Normal,
            5,
            TaskMetadata::from_ctx(&low_ctx),
            None,
        )) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            other => panic!(
                "expected UnifiedReadPoolFull for low-priority task, got {:?}",
                other.err()
            ),
        }

        // Now spawn a high-priority task — should succeed via eviction of a
        // queued low-priority task.
        let (task_high, _tx_high) = gen_task();
        let high_ctx = ResourceControlContext {
            resource_group_name: "high_group".to_string(),
            override_priority: 0,
            ..Default::default()
        };

        block_on(handle.spawn(
            task_high,
            CommandPri::High,
            6,
            TaskMetadata::from_ctx(&high_ctx),
            None,
        ))
        .expect("high-priority task should succeed via eviction");

        // The eviction metric should have been incremented.
        assert!(
            UNIFIED_READ_POOL_EVICTED_TASKS.get() >= 1,
            "eviction counter should be incremented"
        );

        // Unblock task1 so the worker thread can resume and the pool can
        // shut down cleanly.
        let _ = block_tx.send(());
        thread::sleep(Duration::from_millis(300));
    }

    #[test]
    fn test_yatp_read_flow_concurrency_limit() {
        let config = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: 2,
            max_tasks_per_worker: 4,
            enable_flow_fairness: true,
            max_flow_concurrency: 1,
            ..Default::default()
        };

        let engine = TestEngineBuilder::new().build().unwrap();
        let name = "test-yatp-read-flow-concurrency-limit";
        let pool = build_yatp_read_pool_with_name(
            &config,
            DummyReporter,
            engine,
            None,
            None,
            CleanupMethod::InPlace,
            name.to_owned(),
            false,
        );
        let handle = pool.handle();
        let flow_id = ReadFlowId::new(42, 7);

        let (block_tx, block_rx) = std::sync::mpsc::channel::<()>();
        let task1 = async move {
            let _ = block_rx.recv();
        };
        block_on(handle.spawn_with_flow(
            task1,
            CommandPri::Normal,
            1,
            TaskMetadata::default(),
            None,
            flow_id,
        ))
        .unwrap();

        let (spawned_tx, spawned_rx) = std::sync::mpsc::channel::<()>();
        let handle2 = handle.clone();
        let join = thread::spawn(move || {
            block_on(handle2.spawn_with_flow(
                async move {
                    let _ = spawned_tx.send(());
                },
                CommandPri::Normal,
                2,
                TaskMetadata::default(),
                None,
                flow_id,
            ))
            .unwrap();
        });

        thread::sleep(Duration::from_millis(300));
        assert!(spawned_rx.try_recv().is_err());
        assert_eq!(
            UNIFIED_READ_POOL_RUNNING_TASKS
                .with_label_values(&[name, "medium"])
                .get(),
            1
        );

        block_tx.send(()).unwrap();
        spawned_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("second task should enter the pool after the first flow slot is released");
        join.join().unwrap();
    }

    #[test]
    fn test_read_flow_virtual_time_affects_priority() {
        let controller = ReadFlowController::new(8);
        let flow1 = ReadFlowId::new(42, 7).unwrap();
        let flow2 = ReadFlowId::new(43, 7).unwrap();
        let flow3 = ReadFlowId::new(44, 7).unwrap();

        let flow1_first = block_on(controller.acquire(Some(flow1)));
        let flow2_first = block_on(controller.acquire(Some(flow2)));
        let flow1_task_id = flow1_first.yatp_task_id(1);
        let flow2_task_id = flow2_first.yatp_task_id(2);
        assert_ne!(flow1_task_id, flow1.yatp_task_id());
        assert_eq!(
            controller.flow_priority_tag_by_task_id(flow1_task_id),
            Some(0)
        );

        record_flow_elapsed(&flow1_first, Duration::from_millis(1500));

        assert_eq!(
            controller.flow_priority_tag_by_task_id(flow2_task_id),
            Some(0)
        );
        assert_eq!(
            controller.flow_priority_tag_by_task_id(flow1_task_id),
            Some(MAX_READ_FLOW_VIRTUAL_TIME_LAG_US)
        );

        drop(flow2_first);
        controller.refresh_min_virtual_time_and_gc();
        let flow3_first = block_on(controller.acquire(Some(flow3)));
        let flow3_task_id = flow3_first.yatp_task_id(3);
        assert_eq!(
            controller.flow_priority_tag_by_task_id(flow3_task_id),
            Some(0)
        );
        assert_eq!(
            controller.flow_priority_tag_by_task_id(flow1_task_id),
            Some(MAX_READ_FLOW_VIRTUAL_TIME_LAG_US)
        );
    }

    #[test]
    fn test_read_flow_starts_from_active_min_virtual_time() {
        let controller = ReadFlowController::new(8);
        let flow1 = ReadFlowId::new(42, 7).unwrap();
        let flow2 = ReadFlowId::new(43, 7).unwrap();

        let flow1_first = block_on(controller.acquire(Some(flow1)));
        record_flow_elapsed(&flow1_first, Duration::from_millis(150));

        let flow1_task_id = flow1_first.yatp_task_id(1);
        controller.refresh_min_virtual_time_and_gc();
        let flow2_first = block_on(controller.acquire(Some(flow2)));
        let flow2_task_id = flow2_first.yatp_task_id(2);
        let flow1_priority = controller
            .flow_priority_tag_by_task_id(flow1_task_id)
            .unwrap();
        let flow2_priority = controller
            .flow_priority_tag_by_task_id(flow2_task_id)
            .unwrap();
        assert_eq!(flow1_priority, flow2_priority);
        assert_eq!(
            flow1_priority,
            Duration::from_millis(150).as_micros() as u64
        );
    }

    #[test]
    fn test_read_flow_keeps_recent_idle_virtual_time() {
        let controller = ReadFlowController::new(8);
        let flow1 = ReadFlowId::new(42, 7).unwrap();
        let flow2 = ReadFlowId::new(43, 7).unwrap();

        let flow1_first = block_on(controller.acquire(Some(flow1)));
        record_flow_elapsed(&flow1_first, Duration::from_millis(150));
        drop(flow1_first);

        controller.refresh_min_virtual_time_and_gc();

        let flow1_again = block_on(controller.acquire(Some(flow1)));
        let flow1_again_task_id = flow1_again.yatp_task_id(1);
        assert_eq!(
            controller.flow_priority_tag_by_task_id(flow1_again_task_id),
            Some(Duration::from_millis(150).as_micros() as u64)
        );
        drop(flow1_again);

        let flow2_first = block_on(controller.acquire(Some(flow2)));
        let flow2_task_id = flow2_first.yatp_task_id(2);
        assert_eq!(
            controller.flow_priority_tag_by_task_id(flow2_task_id),
            Some(Duration::from_millis(150).as_micros() as u64)
        );
    }

    #[test]
    fn test_flow_priority_provider_recomputes_priority() {
        let controller = ReadFlowController::new(8);
        let flow1 = ReadFlowId::new(42, 7).unwrap();
        let flow2 = ReadFlowId::new(43, 7).unwrap();
        let flow1_permit = block_on(controller.acquire(Some(flow1)));
        let _flow2_permit = block_on(controller.acquire(Some(flow2)));

        let provider = FlowPriorityProvider::new(controller.clone());

        let flow1_task_id = flow1_permit.yatp_task_id(1);
        let mut extras = Extras::new_multilevel(flow1_task_id, None);
        extras.set_metadata(TaskMetadata::default().to_vec());
        let priority_before = provider.priority_of(&extras);

        record_flow_elapsed(&flow1_permit, Duration::from_millis(150));
        let priority_after = provider.priority_of(&extras);

        assert!(
            priority_after > priority_before,
            "flow virtual time should make the same task id recompute to a larger YATP priority"
        );
    }

    #[test]
    fn test_flow_tracked_future_updates_virtual_time_per_poll() {
        struct TwoPollFuture {
            first_poll: bool,
        }

        impl Future for TwoPollFuture {
            type Output = ();

            fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
                thread::sleep(Duration::from_millis(5));
                if self.first_poll {
                    self.first_poll = false;
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            }
        }

        let controller = ReadFlowController::new(1);
        let flow = ReadFlowId::new(42, 7).unwrap();
        let peer_flow = ReadFlowId::new(43, 7).unwrap();
        let flow_permit = block_on(controller.acquire(Some(flow)));
        let _peer_flow_permit = block_on(controller.acquire(Some(peer_flow)));
        let gauge = IntGauge::new(
            "test_flow_tracked_future_updates_virtual_time_per_poll",
            "test gauge",
        )
        .unwrap();
        gauge.inc();

        let flow_task_id = flow_permit.yatp_task_id(1);
        let tracked_future = FlowTrackedFuture::new(
            TwoPollFuture { first_poll: true },
            flow_permit,
            Duration::ZERO,
            gauge.clone(),
        );
        futures::pin_mut!(tracked_future);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(tracked_future.as_mut().poll(&mut cx).is_pending());
        assert!(
            controller
                .flow_priority_tag_by_task_id(flow_task_id)
                .unwrap()
                > 0
        );
        assert_eq!(gauge.get(), 1);

        assert!(tracked_future.as_mut().poll(&mut cx).is_ready());
        assert_eq!(gauge.get(), 0);
    }

    #[test]
    fn test_flow_tracked_future_precharges_estimated_cpu() {
        let controller = ReadFlowController::new(1);
        let flow = ReadFlowId::new(42, 7).unwrap();
        let peer_flow = ReadFlowId::new(43, 7).unwrap();
        let flow_permit = block_on(controller.acquire(Some(flow)));
        let _peer_flow_permit = block_on(controller.acquire(Some(peer_flow)));
        let gauge = IntGauge::new(
            "test_flow_tracked_future_precharges_estimated_cpu",
            "test gauge",
        )
        .unwrap();
        gauge.inc();

        let flow_task_id = flow_permit.yatp_task_id(1);
        let tracked_future = FlowTrackedFuture::new(
            futures::future::ready(()),
            flow_permit,
            Duration::from_millis(50),
            gauge.clone(),
        );

        assert_eq!(
            controller.flow_priority_tag_by_task_id(flow_task_id),
            Some(Duration::from_millis(50).as_micros() as u64)
        );

        futures::pin_mut!(tracked_future);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(tracked_future.as_mut().poll(&mut cx).is_ready());
        assert_eq!(gauge.get(), 0);
        assert_eq!(
            controller.flow_priority_tag_by_task_id(flow_task_id),
            Some(Duration::from_millis(50).as_micros() as u64)
        );
    }

    #[test]
    fn test_flow_tracked_future_precharges_next_poll() {
        struct TwoPollFuture {
            first_poll: bool,
        }

        impl Future for TwoPollFuture {
            type Output = ();

            fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
                if self.first_poll {
                    self.first_poll = false;
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            }
        }

        let controller = ReadFlowController::new(1);
        let flow = ReadFlowId::new(42, 7).unwrap();
        let flow_permit = block_on(controller.acquire(Some(flow)));
        let gauge = IntGauge::new(
            "test_flow_tracked_future_precharges_next_poll",
            "test gauge",
        )
        .unwrap();
        gauge.inc();

        let flow_task_id = flow_permit.yatp_task_id(1);
        let tracked_future = FlowTrackedFuture::new(
            TwoPollFuture { first_poll: true },
            flow_permit,
            Duration::from_millis(50),
            gauge.clone(),
        );
        futures::pin_mut!(tracked_future);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert_eq!(
            controller.flow_priority_tag_by_task_id(flow_task_id),
            Some(Duration::from_millis(50).as_micros() as u64)
        );

        assert!(tracked_future.as_mut().poll(&mut cx).is_pending());
        assert_eq!(
            controller.flow_priority_tag_by_task_id(flow_task_id),
            Some(Duration::from_millis(100).as_micros() as u64)
        );

        assert!(tracked_future.as_mut().poll(&mut cx).is_ready());
        assert_eq!(gauge.get(), 0);
        assert_eq!(
            controller.flow_priority_tag_by_task_id(flow_task_id),
            Some(Duration::from_millis(100).as_micros() as u64)
        );
    }

    #[test]
    fn test_read_flow_concurrent_tasks_keep_own_precharged_priority() {
        let controller = ReadFlowController::new(8);
        let flow = ReadFlowId::new(42, 7).unwrap();
        let first = block_on(controller.acquire(Some(flow)));
        let second = block_on(controller.acquire(Some(flow)));
        let first_task_id = first.yatp_task_id(1);
        let second_task_id = second.yatp_task_id(2);

        first.precharge(Duration::from_millis(10));
        second.precharge(Duration::from_millis(10));

        assert_eq!(
            controller.flow_priority_tag_by_task_id(first_task_id),
            Some(Duration::from_millis(10).as_micros() as u64)
        );
        assert_eq!(
            controller.flow_priority_tag_by_task_id(second_task_id),
            Some(Duration::from_millis(20).as_micros() as u64)
        );
    }
}
