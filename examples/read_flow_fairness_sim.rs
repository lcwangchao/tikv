// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{BTreeMap, VecDeque},
    env, fs,
    hint::black_box,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use chrono::Local;
use cpu_time::ThreadTime;
use futures::{
    Future,
    stream::{FuturesUnordered, StreamExt},
};
use kvproto::kvrpcpb::CommandPri;
use serde_derive::{Deserialize, Serialize};
use tikv::{
    config::UnifiedReadPoolConfig,
    read_pool::{ReadFlowId, ReadFlowPrioritySnapshot, build_yatp_flow_control_read_pool},
};
use tikv_util::{
    config::ReadableDuration, resource_control::TaskMetadata, yatp_pool::CleanupMethod,
};
use tokio::{runtime::Builder, sync::oneshot, task::LocalSet};

type Interval = ReadableDuration;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, rename_all = "kebab-case")]
struct SimConfig {
    simulate: SimulateConfig,
    payloads: Vec<PayloadConfig>,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            simulate: SimulateConfig::default(),
            payloads: vec![PayloadConfig::default()],
        }
    }
}

impl SimConfig {
    fn normalize(&mut self) {
        if self.simulate.duration.0.is_zero() {
            self.simulate.duration = ReadableDuration::secs(1);
        }
        if self.simulate.stats_print_interval.0.is_zero() {
            self.simulate.stats_print_interval = ReadableDuration::secs(1);
        }
        if self.simulate.stats_window.0.is_zero() {
            self.simulate.stats_window = self.simulate.stats_print_interval;
        }
        if self.simulate.report_path.is_empty() {
            self.simulate.report_path = "read-flow-fairness-sim.html".to_string();
        }
        self.simulate.yatp_threads = self.simulate.yatp_threads.max(1);
        self.simulate.max_flow_concurrency = self.simulate.max_flow_concurrency.max(1);
        assert!(
            !self.payloads.is_empty(),
            "at least one payload is required"
        );
        let mut seen = BTreeMap::new();
        for (index, payload) in self.payloads.iter_mut().enumerate() {
            if payload.name.is_empty() {
                payload.name = index.to_string();
            }
            assert!(
                seen.insert(payload.name.clone(), ()).is_none(),
                "duplicate payload name {}",
                payload.name
            );
            payload.concurrency = payload.concurrency.max(1);
            payload.query_concurrency = payload.query_concurrency.max(1);
            assert!(
                payload.rate > 0.0,
                "payload {} rate is required",
                payload.name
            );
            assert!(
                !payload.query_cpu.0.is_zero(),
                "payload {} query-cpu is required",
                payload.name
            );
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, rename_all = "kebab-case")]
struct SimulateConfig {
    duration: Interval,
    #[serde(alias = "stats_print_interval")]
    stats_print_interval: Interval,
    #[serde(alias = "stats_window")]
    stats_window: Interval,
    #[serde(alias = "report_path")]
    report_path: String,
    #[serde(alias = "yatp_threads")]
    yatp_threads: usize,
    #[serde(alias = "max_flow_concurrency")]
    max_flow_concurrency: usize,
}

impl Default for SimulateConfig {
    fn default() -> Self {
        Self {
            duration: ReadableDuration::minutes(5),
            stats_print_interval: ReadableDuration::secs(1),
            stats_window: ReadableDuration::secs(5),
            report_path: "read-flow-fairness-sim.html".to_string(),
            yatp_threads: 4,
            max_flow_concurrency: 128,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, rename_all = "kebab-case")]
struct PayloadConfig {
    name: String,
    concurrency: usize,
    rate: f64,
    #[serde(alias = "query_concurrency")]
    query_concurrency: usize,
    #[serde(alias = "query_cpu")]
    query_cpu: Interval,
    #[serde(alias = "query_task_cpu")]
    query_task_cpu: Interval,
    #[serde(alias = "query_task_slice_cpu")]
    query_task_slice_cpu: Interval,
    #[serde(alias = "start_time")]
    start_time: Interval,
    #[serde(alias = "end_time")]
    end_time: Interval,
    #[serde(alias = "slow_query_latency")]
    slow_query_latency: Interval,
}

impl Default for PayloadConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            concurrency: 1,
            rate: 1.0,
            query_concurrency: 1,
            query_cpu: ReadableDuration::millis(1),
            query_task_cpu: ReadableDuration::ZERO,
            query_task_slice_cpu: ReadableDuration::ZERO,
            start_time: ReadableDuration::ZERO,
            end_time: ReadableDuration::ZERO,
            slow_query_latency: ReadableDuration::ZERO,
        }
    }
}

impl PayloadConfig {
    fn query_task_count(&self) -> usize {
        if self.query_task_cpu.0.is_zero() {
            return 1;
        }

        let cpu_ns = self.query_cpu.0.as_nanos();
        let task_ns = self.query_task_cpu.0.as_nanos();
        cpu_ns
            .saturating_add(task_ns - 1)
            .saturating_div(task_ns)
            .min(usize::MAX as u128) as usize
    }

    fn fill_query_task_cpus(&self, task_cpus: &mut Vec<Duration>) {
        task_cpus.clear();
        if self.query_task_cpu.0.is_zero() {
            task_cpus.push(self.query_cpu.0);
            return;
        }

        let mut remaining = self.query_cpu.0;
        while !remaining.is_zero() {
            let task_cpu = remaining.min(self.query_task_cpu.0);
            task_cpus.push(task_cpu);
            remaining = remaining.saturating_sub(task_cpu);
        }
    }
}

#[derive(Debug)]
struct QueryEvent {
    at: Instant,
    payload: String,
    latency: Duration,
    total_schedule_wait: Duration,
    first_schedule_wait: Duration,
    acquire_wait: Duration,
    rx_wake_delay: Duration,
    subtasks: usize,
    errors: usize,
    slow_query_latency: Duration,
    tasks: Option<Vec<QueryTaskEvent>>,
}

#[derive(Debug)]
struct QueryTaskEvent {
    task_index: usize,
    lane_index: usize,
    cancelled: bool,
    created_after: Duration,
    elapsed: Duration,
    acquire_wait: Duration,
    total_schedule_wait_us: u64,
    first_schedule_wait_us: u64,
    schedule_waits_us: Vec<u64>,
    cpu_us: u64,
    cpu_slices_us: Vec<u64>,
    cpu_wall_us: u64,
    cpu_wall_slices_us: Vec<u64>,
    rx_wake_delay_us: u64,
    priority_snapshots: Vec<Option<ReadFlowPrioritySnapshot>>,
}

#[derive(Debug)]
enum SimEvent {
    Cpu {
        at: Instant,
        payload: String,
        cpu_us: u64,
    },
    Query(QueryEvent),
}

#[derive(Debug)]
struct SimRequest {
    query_seq: u64,
    created_at: Instant,
}

struct QueryScratch {
    task_cpus: Vec<Duration>,
    tasks: Vec<QueryTaskEvent>,
}

impl QueryScratch {
    fn new(task_count: usize, collect_timeline: bool) -> Self {
        Self {
            task_cpus: Vec::with_capacity(task_count),
            tasks: if collect_timeline {
                Vec::with_capacity(task_count)
            } else {
                Vec::new()
            },
        }
    }

    fn prepare(&mut self, payload: &PayloadConfig) {
        payload.fill_query_task_cpus(&mut self.task_cpus);
        self.tasks.clear();
        if !payload.slow_query_latency.0.is_zero() {
            let task_count = self.task_cpus.len();
            if self.tasks.capacity() < task_count {
                self.tasks.reserve(task_count - self.tasks.capacity());
            }
        }
    }
}

struct QueryDone {
    scratch: QueryScratch,
}

struct QueryJob {
    payload: Arc<PayloadConfig>,
    request: SimRequest,
    handle: tikv::read_pool::ReadPoolHandle,
    events_tx: tokio::sync::mpsc::UnboundedSender<SimEvent>,
    done_tx: tokio::sync::mpsc::UnboundedSender<QueryDone>,
    stop: Arc<AtomicBool>,
    scratch: QueryScratch,
}

struct SubTaskResult {
    cancelled: bool,
    total_schedule_wait_us: u64,
    first_schedule_wait_us: u64,
    schedule_waits_us: Vec<u64>,
    cpu_us: u64,
    cpu_slices_us: Vec<u64>,
    cpu_wall_us: u64,
    cpu_wall_slices_us: Vec<u64>,
    completed_at: Instant,
    priority_snapshots: Vec<Option<ReadFlowPrioritySnapshot>>,
}

struct SimSubtask {
    handle: tikv::read_pool::ReadPoolHandle,
    flow_id: Option<ReadFlowId>,
    remaining_cpu: Duration,
    slice_cpu: Duration,
    wait_started_at: Instant,
    stop: Arc<AtomicBool>,
    collect_timeline: bool,
    total_schedule_wait_us: u64,
    first_schedule_wait_us: Option<u64>,
    schedule_waits_us: Vec<u64>,
    cpu_us: u64,
    cpu_slices_us: Vec<u64>,
    cpu_wall_us: u64,
    cpu_wall_slices_us: Vec<u64>,
    priority_snapshots: Vec<Option<ReadFlowPrioritySnapshot>>,
}

impl SimSubtask {
    fn new(
        handle: tikv::read_pool::ReadPoolHandle,
        flow_id: Option<ReadFlowId>,
        created_at: Instant,
        cpu: Duration,
        slice_cpu: Duration,
        stop: Arc<AtomicBool>,
        collect_timeline: bool,
    ) -> Self {
        let capacity = collect_timeline
            .then(|| {
                let slice_count = Self::expected_slice_count(cpu, slice_cpu);
                slice_count
                    .saturating_mul(3)
                    .saturating_add(1)
                    .saturating_div(2)
                    .max(1)
            })
            .unwrap_or(0);
        Self {
            handle,
            flow_id,
            remaining_cpu: cpu,
            slice_cpu,
            wait_started_at: created_at,
            stop,
            collect_timeline,
            total_schedule_wait_us: 0,
            first_schedule_wait_us: None,
            schedule_waits_us: Vec::with_capacity(capacity),
            cpu_us: 0,
            cpu_slices_us: Vec::with_capacity(capacity),
            cpu_wall_us: 0,
            cpu_wall_slices_us: Vec::with_capacity(capacity),
            priority_snapshots: Vec::with_capacity(capacity),
        }
    }

    fn should_yield(&self) -> bool {
        !self.slice_cpu.is_zero() && self.slice_cpu < self.remaining_cpu
    }

    fn expected_slice_count(cpu: Duration, slice_cpu: Duration) -> usize {
        if cpu.is_zero() || slice_cpu.is_zero() || slice_cpu >= cpu {
            return 1;
        }
        let cpu_ns = cpu.as_nanos();
        let slice_ns = slice_cpu.as_nanos();
        cpu_ns
            .saturating_add(slice_ns - 1)
            .saturating_div(slice_ns)
            .min(usize::MAX as u128) as usize
    }
}

impl Future for SimSubtask {
    type Output = SubTaskResult;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let waited = self.wait_started_at.elapsed();
        let waited_us = waited.as_micros() as u64;
        if self.first_schedule_wait_us.is_none() {
            self.first_schedule_wait_us = Some(waited_us);
        }
        self.total_schedule_wait_us = self.total_schedule_wait_us.saturating_add(waited_us);
        if self.collect_timeline {
            let priority_snapshot = self.handle.read_flow_priority_snapshot(self.flow_id);
            self.schedule_waits_us.push(waited_us);
            self.priority_snapshots.push(priority_snapshot);
        }
        if self.stop.load(Ordering::Relaxed) {
            return Poll::Ready(self.finish(true));
        }
        let current = if self.should_yield() {
            self.remaining_cpu.min(self.slice_cpu)
        } else {
            self.remaining_cpu
        };
        let stop = self.stop.clone();
        let cpu_wall_started_at = Instant::now();
        let cpu_us = burn_cpu(current, Some(&stop));
        let cpu_wall_us = cpu_wall_started_at.elapsed().as_micros() as u64;
        self.cpu_us = self.cpu_us.saturating_add(cpu_us);
        self.cpu_wall_us = self.cpu_wall_us.saturating_add(cpu_wall_us);
        if self.collect_timeline {
            self.cpu_slices_us.push(cpu_us);
            self.cpu_wall_slices_us.push(cpu_wall_us);
        }
        if stop.load(Ordering::Relaxed) {
            return Poll::Ready(self.finish(true));
        }
        self.remaining_cpu = self.remaining_cpu.saturating_sub(current);
        if self.remaining_cpu.is_zero() {
            Poll::Ready(self.finish(false))
        } else {
            self.wait_started_at = Instant::now();
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

impl SimSubtask {
    fn finish(&mut self, cancelled: bool) -> SubTaskResult {
        SubTaskResult {
            cancelled,
            total_schedule_wait_us: self.total_schedule_wait_us,
            first_schedule_wait_us: self.first_schedule_wait_us.unwrap_or_default(),
            schedule_waits_us: std::mem::take(&mut self.schedule_waits_us),
            cpu_us: self.cpu_us,
            cpu_slices_us: std::mem::take(&mut self.cpu_slices_us),
            cpu_wall_us: self.cpu_wall_us,
            cpu_wall_slices_us: std::mem::take(&mut self.cpu_wall_slices_us),
            completed_at: Instant::now(),
            priority_snapshots: std::mem::take(&mut self.priority_snapshots),
        }
    }
}

async fn payload_driver(
    payload: Arc<PayloadConfig>,
    sim_start: Instant,
    sim_end: Instant,
    query_id: Arc<AtomicU64>,
    handle: tikv::read_pool::ReadPoolHandle,
    query_tx: tokio::sync::mpsc::UnboundedSender<QueryJob>,
    events_tx: tokio::sync::mpsc::UnboundedSender<SimEvent>,
) {
    let payload_start = sim_start + payload.start_time.0;
    sleep_until(payload_start).await;
    let payload_end = if payload.end_time.0.is_zero() {
        sim_end
    } else {
        (sim_start + payload.end_time.0).min(sim_end)
    };
    if payload_end <= Instant::now() {
        return;
    }

    let stop = Arc::new(AtomicBool::new(false));
    let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<QueryDone>();
    let interval = Duration::from_secs_f64((1.0 / payload.rate).max(0.0));
    let mut next_query_at = Instant::now();
    let mut in_flight = 0usize;
    let collect_timeline = !payload.slow_query_latency.0.is_zero();
    let mut scratch_pool = Vec::with_capacity(payload.concurrency);
    let query_task_count = payload.query_task_count();
    for _ in 0..payload.concurrency {
        scratch_pool.push(QueryScratch::new(query_task_count, collect_timeline));
    }
    loop {
        let now = Instant::now();
        if now >= payload_end {
            stop.store(true, Ordering::Relaxed);
        }
        while let Ok(done) = done_rx.try_recv() {
            scratch_pool.push(done.scratch);
            in_flight = in_flight.saturating_sub(1);
        }
        while now >= next_query_at && now < payload_end && in_flight < payload.concurrency {
            let mut scratch = scratch_pool
                .pop()
                .unwrap_or_else(|| QueryScratch::new(query_task_count, collect_timeline));
            scratch.prepare(&payload);
            let request = SimRequest {
                query_seq: query_id.fetch_add(1, Ordering::Relaxed),
                created_at: Instant::now(),
            };
            if query_tx
                .send(QueryJob {
                    payload: payload.clone(),
                    request,
                    handle: handle.clone(),
                    events_tx: events_tx.clone(),
                    done_tx: done_tx.clone(),
                    stop: stop.clone(),
                    scratch,
                })
                .is_err()
            {
                return;
            }
            in_flight += 1;
            next_query_at = next_query_at
                .checked_add(interval)
                .unwrap_or_else(Instant::now);
            if next_query_at < Instant::now() {
                next_query_at = Instant::now();
            }
        }

        if Instant::now() >= payload_end && in_flight == 0 {
            break;
        }

        if in_flight == 0 {
            sleep_until(next_query_at.min(payload_end)).await;
            continue;
        }

        if in_flight >= payload.concurrency || next_query_at >= payload_end {
            if Instant::now() < payload_end {
                tokio::select! {
                    _ = sleep_until(payload_end) => {
                        stop.store(true, Ordering::Relaxed);
                    }
                    maybe_done = done_rx.recv() => {
                        if let Some(done) = maybe_done {
                            scratch_pool.push(done.scratch);
                            in_flight = in_flight.saturating_sub(1);
                        } else {
                            break;
                        }
                    }
                }
            } else if let Some(done) = done_rx.recv().await {
                scratch_pool.push(done.scratch);
                in_flight = in_flight.saturating_sub(1);
            } else {
                break;
            }
        } else {
            tokio::select! {
                _ = sleep_until(next_query_at) => {}
                maybe_done = done_rx.recv() => {
                    if let Some(done) = maybe_done {
                        scratch_pool.push(done.scratch);
                        in_flight = in_flight.saturating_sub(1);
                    } else {
                        break;
                    }
                }
            }
        }
    }
}

async fn query_worker(mut query_rx: tokio::sync::mpsc::UnboundedReceiver<QueryJob>) {
    let mut active = FuturesUnordered::new();
    let mut input_closed = false;
    loop {
        if input_closed && active.is_empty() {
            break;
        }
        tokio::select! {
            maybe_job = query_rx.recv(), if !input_closed => {
                match maybe_job {
                    Some(job) => {
                        active.push(tokio::task::spawn_local(async move {
                            let scratch = run_query(
                                &job.payload,
                                job.request,
                                &job.handle,
                                &job.events_tx,
                                job.stop,
                                job.scratch,
                            ).await;
                            let _ = job.done_tx.send(QueryDone { scratch });
                        }));
                    }
                    None => {
                        input_closed = true;
                    }
                }
            }
            maybe_done = active.next(), if !active.is_empty() => {
                if let Some(Err(err)) = maybe_done {
                    eprintln!("query future failed: {err}");
                }
            }
        }
    }
}

fn spawn_query_runtime(
    query_rx: tokio::sync::mpsc::UnboundedReceiver<QueryJob>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let runtime = Builder::new_current_thread().enable_time().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&runtime, query_worker(query_rx));
    })
}

async fn run_query(
    payload: &PayloadConfig,
    request: SimRequest,
    handle: &tikv::read_pool::ReadPoolHandle,
    events_tx: &tokio::sync::mpsc::UnboundedSender<SimEvent>,
    stop: Arc<AtomicBool>,
    mut scratch: QueryScratch,
) -> QueryScratch {
    if stop.load(Ordering::Relaxed) {
        return scratch;
    }
    let flow_id = ReadFlowId::new(request.query_seq, payload_id(&payload.name));
    let query_start = request.created_at;
    let collect_timeline = !payload.slow_query_latency.0.is_zero();
    let task_count = scratch.task_cpus.len();
    let max_in_flight = if payload.query_task_cpu.0.is_zero() {
        1
    } else {
        payload.query_concurrency.min(task_count)
    };
    let mut next_task_index = 0;
    let mut in_flight = FuturesUnordered::new();
    while next_task_index < task_count && in_flight.len() < max_in_flight {
        let lane_index = next_task_index;
        in_flight.push(spawn_subtask(
            payload.name.clone(),
            handle.clone(),
            flow_id,
            next_task_index,
            lane_index,
            query_start,
            scratch.task_cpus[next_task_index],
            payload.query_task_slice_cpu.0,
            stop.clone(),
            collect_timeline,
            events_tx.clone(),
        ));
        next_task_index += 1;
    }

    let mut total_schedule_wait = Duration::ZERO;
    let mut first_schedule_wait = Duration::ZERO;
    let mut acquire_wait = Duration::ZERO;
    let mut rx_wake_delay = Duration::ZERO;
    let mut errors = 0usize;
    while let Some(result) = in_flight.next().await {
        let mut freed_lane = None;
        match result {
            Ok(task) => {
                if task.cancelled {
                    scratch.tasks.clear();
                    return scratch;
                }
                freed_lane = Some(task.lane_index);
                acquire_wait += task.acquire_wait;
                rx_wake_delay += Duration::from_micros(task.rx_wake_delay_us);
                total_schedule_wait += Duration::from_micros(task.total_schedule_wait_us);
                first_schedule_wait += Duration::from_micros(task.first_schedule_wait_us);
                if collect_timeline {
                    scratch.tasks.push(task);
                }
            }
            Err(_) => errors += 1,
        }
        if stop.load(Ordering::Relaxed) {
            scratch.tasks.clear();
            return scratch;
        }
        if next_task_index < task_count {
            in_flight.push(spawn_subtask(
                payload.name.clone(),
                handle.clone(),
                flow_id,
                next_task_index,
                freed_lane.unwrap_or(next_task_index % max_in_flight.max(1)),
                query_start,
                scratch.task_cpus[next_task_index],
                payload.query_task_slice_cpu.0,
                stop.clone(),
                collect_timeline,
                events_tx.clone(),
            ));
            next_task_index += 1;
        }
    }
    if stop.load(Ordering::Relaxed) {
        scratch.tasks.clear();
        return scratch;
    }
    if collect_timeline {
        scratch.tasks.sort_by_key(|task| task.task_index);
    }
    let query_finished_at = Instant::now();
    let latency = query_finished_at.saturating_duration_since(query_start);
    let tasks = if collect_timeline && latency >= payload.slow_query_latency.0 {
        Some(std::mem::take(&mut scratch.tasks))
    } else {
        scratch.tasks.clear();
        None
    };
    let _ = events_tx.send(SimEvent::Query(QueryEvent {
        at: query_finished_at,
        payload: payload.name.clone(),
        latency,
        total_schedule_wait,
        first_schedule_wait,
        acquire_wait,
        rx_wake_delay,
        subtasks: task_count,
        errors,
        slow_query_latency: payload.slow_query_latency.0,
        tasks,
    }));
    scratch
}

async fn spawn_subtask(
    payload: String,
    handle: tikv::read_pool::ReadPoolHandle,
    flow_id: Option<ReadFlowId>,
    task_index: usize,
    lane_index: usize,
    query_start: Instant,
    task_cpu: Duration,
    slice_cpu: Duration,
    stop: Arc<AtomicBool>,
    collect_timeline: bool,
    events_tx: tokio::sync::mpsc::UnboundedSender<SimEvent>,
) -> Result<QueryTaskEvent, ()> {
    let created_at = Instant::now();
    let created_after = created_at.saturating_duration_since(query_start);
    let task = SimSubtask::new(
        handle.clone(),
        flow_id,
        created_at,
        task_cpu,
        slice_cpu,
        stop.clone(),
        collect_timeline,
    );
    let (tx, rx) = oneshot::channel();
    let task = async move {
        let result = task.await;
        let _ = tx.send(result);
    };
    let estimated_poll_cpu = if !slice_cpu.is_zero() && slice_cpu < task_cpu {
        slice_cpu
    } else {
        task_cpu
    };
    let acquire_started_at = Instant::now();
    handle
        .spawn_with_flow_and_estimated_cpu(
            task,
            CommandPri::Normal,
            task_index as u64,
            TaskMetadata::default(),
            None,
            flow_id,
            estimated_poll_cpu,
        )
        .await
        .map_err(|_| ())?;
    let acquire_wait = acquire_started_at.elapsed();
    let result = rx.await.map_err(|_| ())?;
    let received_at = Instant::now();
    let elapsed = received_at.saturating_duration_since(created_at);
    let rx_wake_delay_us = received_at
        .saturating_duration_since(result.completed_at)
        .as_micros() as u64;
    if !result.cancelled && result.cpu_us > 0 {
        let _ = events_tx.send(SimEvent::Cpu {
            at: received_at,
            payload,
            cpu_us: result.cpu_us,
        });
    }
    Ok(QueryTaskEvent {
        task_index,
        lane_index,
        cancelled: result.cancelled,
        created_after,
        elapsed,
        acquire_wait,
        total_schedule_wait_us: result.total_schedule_wait_us,
        first_schedule_wait_us: result.first_schedule_wait_us,
        schedule_waits_us: result.schedule_waits_us,
        cpu_us: result.cpu_us,
        cpu_slices_us: result.cpu_slices_us,
        cpu_wall_us: result.cpu_wall_us,
        cpu_wall_slices_us: result.cpu_wall_slices_us,
        rx_wake_delay_us,
        priority_snapshots: result.priority_snapshots,
    })
}

async fn run_simulation(cfg: SimConfig) {
    let read_pool_cfg = UnifiedReadPoolConfig {
        min_thread_count: 1,
        max_thread_count: cfg.simulate.yatp_threads,
        enable_flow_fairness: true,
        max_flow_concurrency: cfg.simulate.max_flow_concurrency,
        ..Default::default()
    };
    let read_pool = build_yatp_flow_control_read_pool(
        &read_pool_cfg,
        "read-flow-fairness-sim".to_string(),
        CleanupMethod::InPlace,
        false,
    );
    let handle = read_pool.handle();
    let sim_start = Instant::now();
    let sim_end = sim_start + cfg.simulate.duration.0;
    let query_id = Arc::new(AtomicU64::new(1));
    let (events_tx, mut events_rx) = tokio::sync::mpsc::unbounded_channel();
    let (query_tx, query_rx) = tokio::sync::mpsc::unbounded_channel();
    let query_thread = spawn_query_runtime(query_rx);

    let mut task_handles = Vec::new();
    for payload in &cfg.payloads {
        let payload = Arc::new(payload.clone());
        task_handles.push(tokio::task::spawn_local(payload_driver(
            payload.clone(),
            sim_start,
            sim_end,
            query_id.clone(),
            handle.clone(),
            query_tx.clone(),
            events_tx.clone(),
        )));
    }
    drop(events_tx);
    drop(query_tx);

    let mut tasks = task_handles.into_iter().collect::<FuturesUnordered<_>>();
    let mut stats = stats::Stats::new(
        cfg.simulate.stats_print_interval.0,
        cfg.simulate.stats_window.0,
        cfg.simulate.yatp_threads,
        cfg.payloads
            .iter()
            .map(|payload| payload.name.clone())
            .collect(),
    );
    let mut stats_tick = tokio::time::interval(cfg.simulate.stats_print_interval.0);
    let mut tasks_done = false;
    while !tasks_done || !events_rx.is_closed() {
        tokio::select! {
            maybe_task = tasks.next(), if !tasks_done => {
                if maybe_task.is_none() {
                    tasks_done = true;
                }
            }
            maybe_event = events_rx.recv() => {
                match maybe_event {
                    Some(SimEvent::Cpu { at, payload, cpu_us }) => {
                        stats.record_subtask_completed(at, payload, cpu_us);
                    }
                    Some(SimEvent::Query(event)) => {
                        stats.record_query_completed(event.at, event);
                    }
                    None => {
                        if tasks_done {
                            break;
                        }
                    }
                }
            }
            _ = stats_tick.tick() => {
                stats.maybe_print(Instant::now());
            }
        }
    }
    while let Ok(event) = events_rx.try_recv() {
        match event {
            SimEvent::Cpu {
                at,
                payload,
                cpu_us,
            } => stats.record_subtask_completed(at, payload, cpu_us),
            SimEvent::Query(event) => stats.record_query_completed(event.at, event),
        }
    }
    stats.flush(Instant::now());
    stats.write_html_report(&cfg.simulate.report_path, &cfg);
    query_thread.join().expect("query runtime thread failed");
    drop(read_pool);
}

fn payload_id(payload_name: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in payload_name.as_bytes() {
        hash = hash.wrapping_mul(1099511628211).wrapping_add(*byte as u64);
    }
    hash.max(1)
}

fn burn_cpu(duration: Duration, stop: Option<&AtomicBool>) -> u64 {
    if duration.is_zero() {
        return 0;
    }
    let start = ThreadTime::now();
    let mut value = duration.as_nanos() as u64 ^ 0x9e37_79b9_7f4a_7c15;
    while start.elapsed() < duration {
        if stop.is_some_and(|stop| stop.load(Ordering::Relaxed)) {
            break;
        }
        for _ in 0..256 {
            value = value
                .wrapping_mul(6_364_136_223_846_793_005)
                .rotate_left(17)
                ^ 0xa076_1d64_78bd_642f;
            black_box(value);
        }
    }
    black_box(value);
    start.elapsed().as_micros() as u64
}

fn div_duration(duration: Duration, divisor: usize) -> Duration {
    if divisor == 0 {
        return duration;
    }
    Duration::from_nanos((duration.as_nanos() / divisor as u128).min(u64::MAX as u128) as u64)
}

async fn sleep_until(instant: Instant) {
    let now = Instant::now();
    if instant > now {
        tokio::time::sleep(instant - now).await;
    }
}

fn load_config() -> SimConfig {
    let path = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(default_config_path);
    let content = fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("failed to read config file {}: {err}", path.display()));
    let mut cfg: SimConfig = toml::from_str(&content)
        .unwrap_or_else(|err| panic!("failed to parse config file {}: {err}", path.display()));
    cfg.normalize();
    cfg
}

fn default_config_path() -> PathBuf {
    let example_config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join("read_flow_fairness_config.toml");
    if example_config_path.exists() {
        return example_config_path;
    }
    PathBuf::from("read_flow_fairness_config.toml")
}

mod stats {
    use super::*;

    const TABLE_SEPARATOR_LEN: usize = 148;

    pub struct Stats {
        start_at: Instant,
        window: Duration,
        print_interval: Duration,
        next_print_at: Instant,
        worker_threads: usize,
        payloads: Vec<String>,
        cpu_samples: VecDeque<CpuSample>,
        query_samples: VecDeque<QuerySample>,
        series: BTreeMap<String, Vec<StatsSample>>,
        printed_any: bool,
        slow_queries: Vec<SlowQuerySample>,
    }

    struct CpuSample {
        at: Instant,
        payload: String,
        cpu_us: u64,
    }

    struct QuerySample {
        at: Instant,
        payload: String,
        latency_us: u64,
        total_schedule_wait_us: u64,
        first_schedule_wait_us: u64,
        acquire_wait_us: u64,
        errors: usize,
    }

    #[derive(Serialize)]
    struct SlowQuerySample {
        at_secs: f64,
        payload: String,
        latency_us: u64,
        total_schedule_wait_us: u64,
        first_schedule_wait_us: u64,
        acquire_wait_us: u64,
        rx_wake_delay_us: u64,
        tasks: Vec<SlowQueryTaskSample>,
    }

    #[derive(Serialize)]
    struct SlowQueryTaskSample {
        task_index: usize,
        lane_index: usize,
        created_after_us: u64,
        elapsed_us: u64,
        acquire_wait_us: u64,
        cpu_us: u64,
        cpu_slices_us: Vec<u64>,
        cpu_wall_us: u64,
        cpu_wall_slices_us: Vec<u64>,
        rx_wake_delay_us: u64,
        schedule_waits_us: Vec<u64>,
        priority_snapshots: Vec<Option<ReadFlowPrioritySnapshot>>,
    }

    #[derive(Serialize)]
    struct StatsSample {
        time_secs: f64,
        cpu: Option<f64>,
        avg_task_ms: Option<f64>,
        qps: Option<f64>,
        avg_ms: Option<f64>,
        total_schedule_ms: Option<f64>,
        first_schedule_ms: Option<f64>,
        acquire_ms: Option<f64>,
        p50_ms: Option<f64>,
        p80_ms: Option<f64>,
        p99_ms: Option<f64>,
        p999_ms: Option<f64>,
        errors: Option<f64>,
    }

    #[derive(Serialize)]
    struct ReportJson<'a> {
        config: &'a SimConfig,
        series: &'a BTreeMap<String, Vec<StatsSample>>,
        slow_queries: &'a [SlowQuerySample],
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Metric {
        Cpu,
        AvgTaskMs,
        Qps,
        AvgMs,
        TotalScheduleMs,
        FirstScheduleMs,
        AcquireMs,
        P50Ms,
        P80Ms,
        P99Ms,
        P999Ms,
        Errors,
    }

    impl Stats {
        pub fn new(
            print_interval: Duration,
            window: Duration,
            worker_threads: usize,
            payloads: Vec<String>,
        ) -> Self {
            let now = Instant::now();
            Self {
                start_at: now,
                window,
                print_interval,
                next_print_at: now + print_interval,
                worker_threads,
                payloads,
                cpu_samples: VecDeque::new(),
                query_samples: VecDeque::new(),
                series: BTreeMap::new(),
                printed_any: false,
                slow_queries: Vec::new(),
            }
        }

        pub fn record_subtask_completed(&mut self, at: Instant, payload: String, cpu_us: u64) {
            self.cpu_samples.push_back(CpuSample {
                at,
                payload,
                cpu_us,
            });
            self.prune(at);
        }

        pub fn record_query_completed(&mut self, at: Instant, event: QueryEvent) {
            if !event.slow_query_latency.is_zero() && event.latency >= event.slow_query_latency {
                self.record_slow_query(at, &event);
            }
            let avg_total_schedule_wait = if event.subtasks == 0 {
                Duration::ZERO
            } else {
                div_duration(event.total_schedule_wait, event.subtasks)
            };
            let avg_first_schedule_wait = if event.subtasks == 0 {
                Duration::ZERO
            } else {
                div_duration(event.first_schedule_wait, event.subtasks)
            };
            let avg_acquire_wait = if event.subtasks == 0 {
                Duration::ZERO
            } else {
                div_duration(event.acquire_wait, event.subtasks)
            };
            self.query_samples.push_back(QuerySample {
                at,
                payload: event.payload,
                latency_us: event.latency.as_micros() as u64,
                total_schedule_wait_us: avg_total_schedule_wait.as_micros() as u64,
                first_schedule_wait_us: avg_first_schedule_wait.as_micros() as u64,
                acquire_wait_us: avg_acquire_wait.as_micros() as u64,
                errors: event.errors,
            });
            self.prune(at);
        }

        pub fn maybe_print(&mut self, now: Instant) {
            while now >= self.next_print_at {
                self.print_window(self.next_print_at);
                self.next_print_at += self.print_interval;
            }
        }

        pub fn flush(&mut self, now: Instant) {
            if !self.printed_any {
                self.print_window(now);
            }
        }

        pub fn write_html_report(&self, report_path: &str, cfg: &SimConfig) {
            write_html_report(report_path, cfg, &self.series, &self.slow_queries);
        }

        fn record_slow_query(&mut self, at: Instant, event: &QueryEvent) {
            let total_schedule_wait_us = event.total_schedule_wait.as_micros() as u64;
            let first_schedule_wait_us = event.first_schedule_wait.as_micros() as u64;
            let acquire_wait_us = event.acquire_wait.as_micros() as u64;
            let rx_wake_delay_us = event.rx_wake_delay.as_micros() as u64;
            let latency_us = event.latency.as_micros() as u64;
            println!(
                "slow query: payload={}, latency={:.2}ms, sched={:.2}ms, first_sched={:.2}ms, acquire={:.2}ms, rx_wake={:.2}ms, tasks={}",
                event.payload,
                latency_us as f64 / 1000.0,
                total_schedule_wait_us as f64 / 1000.0,
                first_schedule_wait_us as f64 / 1000.0,
                acquire_wait_us as f64 / 1000.0,
                rx_wake_delay_us as f64 / 1000.0,
                event.tasks.as_ref().map_or(0, Vec::len)
            );
            let tasks = event.tasks.as_deref().unwrap_or(&[]);
            self.slow_queries.push(SlowQuerySample {
                at_secs: at.saturating_duration_since(self.start_at).as_secs_f64(),
                payload: event.payload.clone(),
                latency_us,
                total_schedule_wait_us,
                first_schedule_wait_us,
                acquire_wait_us,
                rx_wake_delay_us,
                tasks: tasks
                    .iter()
                    .map(|task| SlowQueryTaskSample {
                        task_index: task.task_index,
                        lane_index: task.lane_index,
                        created_after_us: task.created_after.as_micros() as u64,
                        elapsed_us: task.elapsed.as_micros() as u64,
                        acquire_wait_us: task.acquire_wait.as_micros() as u64,
                        cpu_us: task.cpu_us,
                        cpu_slices_us: task.cpu_slices_us.clone(),
                        cpu_wall_us: task.cpu_wall_us,
                        cpu_wall_slices_us: task.cpu_wall_slices_us.clone(),
                        rx_wake_delay_us: task.rx_wake_delay_us,
                        schedule_waits_us: task.schedule_waits_us.clone(),
                        priority_snapshots: task.priority_snapshots.clone(),
                    })
                    .collect(),
            });
        }

        fn print_window(&mut self, now: Instant) {
            self.printed_any = true;
            self.prune(now);
            let window_start_at = now.checked_sub(self.window).unwrap_or(self.start_at);
            let window_start_at = window_start_at.max(self.start_at);
            let window_start = window_start_at
                .saturating_duration_since(self.start_at)
                .as_secs_f64();
            let window_end = now.saturating_duration_since(self.start_at).as_secs_f64();
            let window_secs = now
                .saturating_duration_since(window_start_at)
                .as_secs_f64()
                .max(f64::EPSILON);
            let total_cpu_us = self
                .cpu_samples
                .iter()
                .filter(|sample| sample.at >= window_start_at && sample.at <= now)
                .map(|sample| sample.cpu_us)
                .sum::<u64>();
            let cpu_cores = total_cpu_us as f64 / 1_000_000.0 / window_secs;
            let cpu_ratio = cpu_cores / self.worker_threads as f64 * 100.0;
            println!();
            println!(
                "[{}] stats window [{:.1}s, {:.1}s), interval {:.1}s, worker_cpu {:.2} cores ({:.1}% of {} workers)",
                format_elapsed(now.saturating_duration_since(self.start_at)),
                window_start,
                window_end,
                window_secs,
                cpu_cores,
                cpu_ratio,
                self.worker_threads
            );
            println!(
                "{:<12} {:<8} {:<10} {:<12} {:<12} {:<10} {:<10} {:<10} {:<10} {:<10} {:<16} {:<12} {:<10}",
                "payload",
                "cpu%",
                "qps",
                "avg_ms",
                "task_avg_ms",
                "sched_ms",
                "p50_ms",
                "p80_ms",
                "p99_ms",
                "p999_ms",
                "first_sched_ms",
                "acquire_ms",
                "err/s"
            );
            println!("{}", "-".repeat(TABLE_SEPARATOR_LEN));

            for payload in self.payloads.clone() {
                let payload_cpu_samples = self
                    .cpu_samples
                    .iter()
                    .filter(|sample| {
                        sample.at >= window_start_at
                            && sample.at <= now
                            && sample.payload == payload
                    })
                    .collect::<Vec<_>>();
                let payload_cpu_us = payload_cpu_samples
                    .iter()
                    .map(|sample| sample.cpu_us)
                    .sum::<u64>();
                let avg_task_ms = (!payload_cpu_samples.is_empty())
                    .then_some(payload_cpu_us as f64 / payload_cpu_samples.len() as f64 / 1000.0);
                let mut latencies = Vec::new();
                let mut total_schedule_waits = Vec::new();
                let mut first_schedule_waits = Vec::new();
                let mut acquire_waits = Vec::new();
                let mut errors_count = 0usize;
                for sample in self.query_samples.iter().filter(|sample| {
                    sample.at >= window_start_at && sample.at <= now && sample.payload == payload
                }) {
                    latencies.push(sample.latency_us);
                    total_schedule_waits.push(sample.total_schedule_wait_us);
                    first_schedule_waits.push(sample.first_schedule_wait_us);
                    acquire_waits.push(sample.acquire_wait_us);
                    errors_count += sample.errors;
                }
                latencies.sort_unstable();
                let count = latencies.len() as f64;
                let qps = (!latencies.is_empty()).then_some(count / window_secs);
                let avg_ms = (!latencies.is_empty())
                    .then_some(latencies.iter().sum::<u64>() as f64 / count / 1000.0);
                let total_schedule_ms = avg_ms_for_values(&total_schedule_waits);
                let first_schedule_ms = avg_ms_for_values(&first_schedule_waits);
                let acquire_ms = avg_ms_for_values(&acquire_waits);
                let p50_ms =
                    (!latencies.is_empty()).then_some(percentile(&latencies, 0.50) as f64 / 1000.0);
                let p80_ms =
                    (!latencies.is_empty()).then_some(percentile(&latencies, 0.80) as f64 / 1000.0);
                let p99_ms =
                    (!latencies.is_empty()).then_some(percentile(&latencies, 0.99) as f64 / 1000.0);
                let p999_ms = (!latencies.is_empty())
                    .then_some(percentile(&latencies, 0.999) as f64 / 1000.0);
                let errors = (!latencies.is_empty() || errors_count > 0)
                    .then_some(errors_count as f64 / window_secs);
                let cpu = (!payload_cpu_samples.is_empty())
                    .then_some(payload_cpu_us as f64 / 1_000_000.0 / window_secs * 100.0);
                self.series
                    .entry(payload.clone())
                    .or_default()
                    .push(StatsSample {
                        time_secs: window_end,
                        cpu,
                        avg_task_ms,
                        qps,
                        avg_ms,
                        total_schedule_ms,
                        first_schedule_ms,
                        acquire_ms,
                        p50_ms,
                        p80_ms,
                        p99_ms,
                        p999_ms,
                        errors,
                    });
                println!(
                    "{:<12} {:<8} {:<10} {:<12} {:<12} {:<10} {:<10} {:<10} {:<10} {:<10} {:<16} {:<12} {:<10}",
                    payload,
                    fmt_opt(cpu),
                    fmt_opt(qps),
                    fmt_opt(avg_ms),
                    fmt_opt(avg_task_ms),
                    fmt_opt(total_schedule_ms),
                    fmt_opt(p50_ms),
                    fmt_opt(p80_ms),
                    fmt_opt(p99_ms),
                    fmt_opt(p999_ms),
                    fmt_opt(first_schedule_ms),
                    fmt_opt(acquire_ms),
                    fmt_opt(errors)
                );
            }
        }

        fn prune(&mut self, now: Instant) {
            let Some(cutoff) = now.checked_sub(self.window + self.print_interval) else {
                return;
            };
            while self
                .cpu_samples
                .front()
                .is_some_and(|sample| sample.at < cutoff)
            {
                self.cpu_samples.pop_front();
            }
            while self
                .query_samples
                .front()
                .is_some_and(|sample| sample.at < cutoff)
            {
                self.query_samples.pop_front();
            }
        }
    }

    fn write_html_report(
        report_path: &str,
        cfg: &SimConfig,
        series: &BTreeMap<String, Vec<StatsSample>>,
        slow_queries: &[SlowQuerySample],
    ) {
        let metrics = [
            Metric::Cpu,
            Metric::Qps,
            Metric::AvgMs,
            Metric::AvgTaskMs,
            Metric::TotalScheduleMs,
            Metric::P50Ms,
            Metric::P80Ms,
            Metric::P99Ms,
            Metric::P999Ms,
            Metric::FirstScheduleMs,
            Metric::AcquireMs,
            Metric::Errors,
        ];
        let chart_sections = metrics
            .iter()
            .map(|metric| render_chart_section(*metric, series))
            .collect::<String>();
        let config_section = render_config_section(cfg);
        let report_json = render_report_json(cfg, series, slow_queries);
        let slow_queries = render_slow_queries(slow_queries);
        let chart_script = render_chart_script(series, &metrics);
        let interaction_script = render_interaction_script();
        let html = format!(
            "<!doctype html><html><head><meta charset=\"utf-8\"><title>Read Flow Fairness Simulation Report</title><script src=\"https://cdn.jsdelivr.net/npm/chart.js\"></script><style>{}</style></head><body><h1>Read Flow Fairness Simulation Report</h1>{}{}{}{}{}{}</body></html>",
            report_style(),
            report_json,
            config_section,
            chart_sections,
            slow_queries,
            chart_script,
            interaction_script
        );
        let report_path = resolve_report_path(report_path);
        if let Some(parent) = report_path.parent() {
            fs::create_dir_all(parent).unwrap_or_else(|err| {
                panic!(
                    "failed to create report directory {}: {err}",
                    parent.display()
                );
            });
        }
        fs::write(&report_path, html).unwrap_or_else(|err| {
            panic!("failed to write report {}: {err}", report_path.display());
        });
        let report_path =
            fs::canonicalize(&report_path).unwrap_or_else(|_| report_path.to_path_buf());
        println!("wrote report to {}", report_path.display());
    }

    fn render_report_json(
        cfg: &SimConfig,
        series: &BTreeMap<String, Vec<StatsSample>>,
        slow_queries: &[SlowQuerySample],
    ) -> String {
        let json = serde_json::to_string_pretty(&ReportJson {
            config: cfg,
            series,
            slow_queries,
        })
        .expect("failed to serialize report json")
        .replace("</", "<\\/");
        format!(
            "<script id=\"read-flow-fairness-report-json\" type=\"application/json\">{}</script>",
            json
        )
    }

    fn report_style() -> &'static str {
        r#"body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;margin:32px;color:#1f2933}h1{margin-bottom:24px}section{margin:0 0 36px}h2{font-size:18px;margin:0 0 10px}pre{padding:12px;border:1px solid #d8dee4;background:#f6f8fa;overflow:auto}details{margin-top:10px}summary{cursor:pointer}.chart-wrap{height:320px;border:1px solid #d8dee4;padding:16px;background:#fff}.metric-table{margin-top:10px}.table-wrap{overflow:auto;border:1px solid #d8dee4;background:#fff}table{border-collapse:collapse;width:100%;font-size:13px}th,td{border:1px solid #d8dee4;padding:6px 8px;text-align:left;white-space:nowrap}th{background:#f6f8fa}.kv{display:inline-flex;align-items:baseline;margin:2px 6px 2px 0;border:1px solid #d8dee4;background:#f8fafc;border-radius:4px;overflow:hidden;font-size:12px;line-height:1.5}.kv-key{padding:1px 5px;color:#475569;background:#eef2f7;font-weight:600}.kv-value{padding:1px 6px;color:#0f172a;font-weight:500}.slow-query{border:1px solid #d8dee4;margin:0 0 16px;padding:12px;background:#fff}.slow-query summary{font-weight:600}.slow-detail{margin-top:12px}.slow-legend{margin:0 0 10px}.slow-viz{overflow:auto;padding:8px 0}.slow-overview{min-width:760px;border:1px solid #d8dee4;background:#fff}.time-axis-h{position:relative;margin-left:72px;height:28px;border-bottom:1px solid #cbd5e1}.time-tick-h{position:absolute;top:0;bottom:0;border-left:1px solid #cbd5e1;color:#64748b;font-size:11px}.time-tick-h span{position:relative;left:4px;top:2px;background:#fff;padding:0 2px}.lane-section{border-top:1px solid #e2e8f0}.lane-main{position:relative;height:42px}.lane-label{position:absolute;left:0;top:0;width:64px;height:100%;display:flex;align-items:center;justify-content:flex-end;padding-right:8px;color:#475569;font-size:12px}.lane-track{position:absolute;left:72px;right:0;top:7px;height:28px;background:#f8fafc;border-left:1px solid #cbd5e1}.lane-task-details{position:relative;margin-left:72px;margin-right:10px;margin-bottom:10px;padding-top:8px}.task-block{position:absolute;top:2px;height:24px;min-width:8px;border:2px solid #334155;background:#dbeafe;box-sizing:border-box;overflow:hidden;cursor:pointer}.task-block:hover:not(.is-selected){outline:2px dashed #64748b;z-index:3}.task-block.is-selected{border-color:#2563eb;box-shadow:0 0 0 4px rgba(37,99,235,.48);z-index:4}.task-overview-segment{position:absolute;top:0;bottom:0}.task-label{position:relative;z-index:1;display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:11px;font-weight:600;line-height:20px;padding:0 4px;color:#064e3b;text-shadow:0 1px 1px rgba(255,255,255,.8)}.task-detail{position:relative;display:none;border:1px solid #d8dee4;background:#f8fafc;padding:10px}.task-detail.is-active{display:block;border-color:#93c5fd;background:#eff6ff;box-shadow:0 2px 8px rgba(15,23,42,.08)}.detail-title{margin-bottom:8px}.detail-timeline{position:relative;height:32px;border:1px solid #cbd5e1;background:#fff}.slice-block{position:absolute;top:4px;height:22px;min-width:6px;border:1px solid #334155;background:#fff;box-sizing:border-box;cursor:pointer;overflow:hidden}.slice-block:hover:not(.is-selected){outline:2px dashed #64748b;z-index:2}.slice-block.is-selected{border-color:#2563eb;box-shadow:0 0 0 4px rgba(37,99,235,.48);z-index:3}.slice-segment{position:absolute;top:0;bottom:0}.slice-details{position:relative;margin-top:8px;padding-top:8px}.slice-detail{position:relative;display:none;border:1px solid #d8dee4;background:#fff;padding:8px}.slice-detail.is-active{display:block;border-color:#93c5fd;background:#eff6ff;box-shadow:0 2px 8px rgba(15,23,42,.08)}.slice-detail-title{margin-bottom:6px}.slice-timeline{position:relative;height:22px;border:1px solid #cbd5e1;background:#fff}.slice-detail-segment{position:absolute;top:0;bottom:0}.state-wait{background:#f97316}.state-cpu{background:#86efac}.state-cpu-gap{background:#cbd5e1}.state-rx{background:#a78bfa}.state-other{background:#94a3b8}.legend-chip{display:inline-block;width:10px;height:10px;margin-right:4px;vertical-align:-1px}.muted{color:#64748b}"#
    }

    fn resolve_report_path(report_path: &str) -> PathBuf {
        let report_path = Path::new(report_path);
        let report_path = if report_path.is_absolute() {
            report_path.to_path_buf()
        } else {
            cargo_target_dir()
                .unwrap_or_else(|| PathBuf::from("target"))
                .join(report_path)
        };
        timestamped_report_path(&report_path)
    }

    fn timestamped_report_path(report_path: &Path) -> PathBuf {
        let timestamp = Local::now().format("%Y%m%d-%H%M%S").to_string();
        let stem = report_path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("report");
        let extension = report_path
            .extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("html");
        let file_name = if extension.is_empty() {
            format!("{stem}-{timestamp}")
        } else {
            format!("{stem}-{timestamp}.{extension}")
        };
        let mut timestamped_path = report_path.to_path_buf();
        timestamped_path.set_file_name(file_name);
        timestamped_path
    }

    fn cargo_target_dir() -> Option<PathBuf> {
        let current_exe = env::current_exe().ok()?;
        current_exe
            .ancestors()
            .find(|path| path.file_name().is_some_and(|name| name == "target"))
            .map(Path::to_path_buf)
    }

    fn render_chart_section(metric: Metric, series: &BTreeMap<String, Vec<StatsSample>>) -> String {
        format!(
            "<section><h2>{}</h2><div class=\"chart-wrap\"><canvas id=\"{}\"></canvas></div>{}</section>",
            escape_html(metric_title(metric)),
            metric_id(metric),
            render_metric_table(metric, series)
        )
    }

    fn render_config_section(cfg: &SimConfig) -> String {
        let config = toml::to_string_pretty(cfg)
            .unwrap_or_else(|err| format!("failed to serialize config: {err}"));
        format!(
            "<section><h2>Config</h2><pre>{}</pre></section>",
            escape_html(&config)
        )
    }

    fn render_metric_table(metric: Metric, series: &BTreeMap<String, Vec<StatsSample>>) -> String {
        let mut times = BTreeMap::<u64, ()>::new();
        for samples in series.values() {
            for sample in samples {
                times.insert(time_key(sample.time_secs), ());
            }
        }
        if times.is_empty() {
            return String::new();
        }

        let mut header = "<tr><th>time_s</th>".to_string();
        if metric == Metric::Cpu {
            header.push_str("<th>total</th>");
        }
        for payload in series.keys() {
            header.push_str(&format!("<th>{}</th>", escape_html(payload)));
        }
        header.push_str("</tr>");

        let total_cpu = if metric == Metric::Cpu {
            total_cpu_by_time(series)
        } else {
            BTreeMap::new()
        };
        let mut body = String::new();
        for time in times.keys().copied() {
            body.push_str(&format!("<tr><td>{:.1}</td>", time as f64 / 1000.0));
            if metric == Metric::Cpu {
                body.push_str(&format!(
                    "<td>{}</td>",
                    fmt_html_opt(total_cpu.get(&time).copied())
                ));
            }
            for samples in series.values() {
                let value = samples
                    .iter()
                    .find(|sample| time_key(sample.time_secs) == time)
                    .and_then(|sample| metric_value(sample, metric));
                body.push_str(&format!("<td>{}</td>", fmt_html_opt(value)));
            }
            body.push_str("</tr>");
        }

        format!(
            "<details class=\"metric-table\"><summary>Data table</summary><div class=\"table-wrap\"><table><thead>{}</thead><tbody>{}</tbody></table></div></details>",
            header, body
        )
    }

    fn render_slow_queries(slow_queries: &[SlowQuerySample]) -> String {
        if slow_queries.is_empty() {
            return "<section><h2>Slow Queries</h2><p>No slow queries were collected.</p></section>"
                .to_string();
        }
        let body = slow_queries
            .iter()
            .map(render_slow_query)
            .collect::<String>();
        format!(
            "<section><h2>Slow Queries</h2><p class=\"muted\"><span class=\"legend-chip state-wait\"></span>schedule wait <span class=\"legend-chip state-cpu\"></span>CPU thread <span class=\"legend-chip state-cpu-gap\"></span>CPU wall gap <span class=\"legend-chip state-rx\"></span>rx wake <span class=\"legend-chip state-other\"></span>other</p>{}</section>",
            body
        )
    }

    fn render_slow_query(query: &SlowQuerySample) -> String {
        let total_cpu_us = query.tasks.iter().map(|task| task.cpu_us).sum::<u64>();
        let total_cpu_wall_us = query.tasks.iter().map(|task| task.cpu_wall_us).sum::<u64>();
        let total_cpu_gap_us = query.tasks.iter().map(task_cpu_wall_gap_us).sum::<u64>();
        let total_rx_wake_us = query.rx_wake_delay_us;
        let lane_count = query
            .tasks
            .iter()
            .map(|task| task.lane_index)
            .max()
            .map(|lane| lane + 1)
            .unwrap_or(0);
        let timeline = render_slow_query_timeline(query, lane_count);
        let summary = render_kv_pairs(&[
            ("t", format!("{:.3}s", query.at_secs)),
            ("payload", query.payload.clone()),
            ("latency", fmt_ms(query.latency_us)),
            ("tasks", query.tasks.len().to_string()),
            ("concurrency", lane_count.to_string()),
            ("sched", fmt_ms(query.total_schedule_wait_us)),
            ("first_sched", fmt_ms(query.first_schedule_wait_us)),
            ("cpu", fmt_ms(total_cpu_us)),
            ("cpu_wall", fmt_ms(total_cpu_wall_us)),
            ("cpu_gap", fmt_ms(total_cpu_gap_us)),
            ("acquire", fmt_ms(query.acquire_wait_us)),
            ("rx_wake", fmt_ms(total_rx_wake_us)),
        ]);
        format!(
            "<details class=\"slow-query\"><summary>{}</summary><div class=\"slow-detail\">{}</div></details>",
            summary, timeline
        )
    }

    fn render_slow_query_timeline(query: &SlowQuerySample, lane_count: usize) -> String {
        if lane_count == 0 {
            return "<p>No task timeline was collected.</p>".to_string();
        }
        let max_elapsed_us = query
            .tasks
            .iter()
            .map(|task| task.created_after_us.saturating_add(task.elapsed_us))
            .max()
            .unwrap_or(query.latency_us)
            .max(query.latency_us)
            .max(1);
        let mut overview = render_horizontal_time_axis(max_elapsed_us);
        for lane in 0..lane_count {
            overview.push_str(&render_lane_row(query, lane, max_elapsed_us));
        }
        format!(
            "<div class=\"slow-viz\"><div class=\"slow-overview\">{}</div></div>",
            overview
        )
    }

    fn render_horizontal_time_axis(max_elapsed_us: u64) -> String {
        let ticks = (0..=4)
            .map(|i| {
                let left = i as f64 * 25.0;
                let value = (max_elapsed_us as f64 * left / 100.0).round() as u64;
                format!(
                    "<div class=\"time-tick-h\" style=\"left:{:.4}%\"><span>{}</span></div>",
                    left,
                    fmt_ms(value)
                )
            })
            .collect::<String>();
        format!("<div class=\"time-axis-h\">{}</div>", ticks)
    }

    fn render_lane_row(query: &SlowQuerySample, lane: usize, max_elapsed_us: u64) -> String {
        let blocks = query
            .tasks
            .iter()
            .filter(|task| task.lane_index == lane)
            .map(|task| render_task_block(task, max_elapsed_us))
            .collect::<String>();
        let details = query
            .tasks
            .iter()
            .filter(|task| task.lane_index == lane)
            .map(render_task_detail_panel)
            .collect::<String>();
        format!(
            "<div class=\"lane-section\"><div class=\"lane-main\"><div class=\"lane-label\">Line-{}</div><div class=\"lane-track\">{}</div></div><div class=\"lane-task-details\">{}</div></div>",
            lane, blocks, details
        )
    }

    fn render_task_block(task: &SlowQueryTaskSample, max_elapsed_us: u64) -> String {
        let left = task.created_after_us as f64 / max_elapsed_us as f64 * 100.0;
        let width = (task.elapsed_us as f64 / max_elapsed_us as f64 * 100.0).max(0.3);
        format!(
            "<div class=\"task-block\" data-task-id=\"{}\" style=\"left:{:.4}%;width:{:.4}%\" title=\"{}\">{}<span class=\"task-label\">Task-{}</span></div>",
            task_dom_id(task),
            left,
            width.min(100.0 - left.min(100.0)),
            escape_html(&task_title(task)),
            render_task_overview_segments(task),
            task.task_index
        )
    }

    fn render_task_overview_segments(task: &SlowQueryTaskSample) -> String {
        let mut offset_us = 0u64;
        let mut segments = String::new();
        let segment_count = task
            .schedule_waits_us
            .len()
            .max(task.cpu_slices_us.len())
            .max(task.cpu_wall_slices_us.len());
        for index in 0..segment_count {
            if let Some(wait_us) = task.schedule_waits_us.get(index).copied() {
                segments.push_str(&render_task_overview_segment(
                    "state-wait",
                    offset_us,
                    wait_us,
                    task.elapsed_us,
                ));
                offset_us = offset_us.saturating_add(wait_us);
            }
            let cpu_us = task.cpu_slices_us.get(index).copied().unwrap_or(0);
            let cpu_wall_us = task
                .cpu_wall_slices_us
                .get(index)
                .copied()
                .unwrap_or(cpu_us)
                .max(cpu_us);
            if cpu_us > 0 {
                segments.push_str(&render_task_overview_segment(
                    "state-cpu",
                    offset_us,
                    cpu_us,
                    task.elapsed_us,
                ));
                offset_us = offset_us.saturating_add(cpu_us);
            }
            let cpu_gap_us = cpu_wall_us.saturating_sub(cpu_us);
            if cpu_gap_us > 0 {
                segments.push_str(&render_task_overview_segment(
                    "state-cpu-gap",
                    offset_us,
                    cpu_gap_us,
                    task.elapsed_us,
                ));
                offset_us = offset_us.saturating_add(cpu_gap_us);
            }
        }
        if task.rx_wake_delay_us > 0 {
            segments.push_str(&render_task_overview_segment(
                "state-rx",
                offset_us,
                task.rx_wake_delay_us,
                task.elapsed_us,
            ));
            offset_us = offset_us.saturating_add(task.rx_wake_delay_us);
        }
        if task.elapsed_us > offset_us {
            segments.push_str(&render_task_overview_segment(
                "state-other",
                offset_us,
                task.elapsed_us - offset_us,
                task.elapsed_us,
            ));
        }
        segments
    }

    fn render_task_overview_segment(
        class_name: &str,
        offset_us: u64,
        duration_us: u64,
        total_us: u64,
    ) -> String {
        if duration_us == 0 || total_us == 0 {
            return String::new();
        }
        let left = offset_us as f64 / total_us as f64 * 100.0;
        let width = (duration_us as f64 / total_us as f64 * 100.0).max(0.3);
        format!(
            "<span class=\"task-overview-segment {}\" style=\"left:{:.4}%;width:{:.4}%\"></span>",
            class_name,
            left,
            width.min(100.0 - left.min(100.0))
        )
    }

    fn render_task_detail_panel(task: &SlowQueryTaskSample) -> String {
        format!(
            "<div class=\"task-detail\" data-task-id=\"{}\"><div class=\"detail-title\">{}</div><div class=\"detail-timeline\">{}</div><div class=\"slice-details\">{}</div></div>",
            task_dom_id(task),
            task_summary(task),
            render_task_slice_blocks(task),
            render_slice_detail_panels(task)
        )
    }

    fn render_task_slice_blocks(task: &SlowQueryTaskSample) -> String {
        let mut offset_us = 0u64;
        let mut blocks = String::new();
        let segment_count = task
            .schedule_waits_us
            .len()
            .max(task.cpu_slices_us.len())
            .max(task.cpu_wall_slices_us.len());
        for index in 0..segment_count {
            let wait_us = task.schedule_waits_us.get(index).copied().unwrap_or(0);
            let cpu_us = task.cpu_slices_us.get(index).copied().unwrap_or(0);
            let cpu_wall_us = task
                .cpu_wall_slices_us
                .get(index)
                .copied()
                .unwrap_or(cpu_us)
                .max(cpu_us);
            let duration_us = wait_us.saturating_add(cpu_wall_us);
            blocks.push_str(&render_slice_block(
                task,
                index,
                offset_us,
                wait_us,
                cpu_us,
                cpu_wall_us,
                task.elapsed_us,
            ));
            offset_us = offset_us.saturating_add(duration_us);
        }
        if task.rx_wake_delay_us > 0 {
            blocks.push_str(&render_terminal_slice_block(
                offset_us,
                task.rx_wake_delay_us,
                task.elapsed_us,
                "state-rx",
                "rx wake",
            ));
            offset_us = offset_us.saturating_add(task.rx_wake_delay_us);
        }
        if task.elapsed_us > offset_us {
            blocks.push_str(&render_terminal_slice_block(
                offset_us,
                task.elapsed_us - offset_us,
                task.elapsed_us,
                "state-other",
                "other",
            ));
        }
        blocks
    }

    fn render_slice_block(
        task: &SlowQueryTaskSample,
        slice_index: usize,
        offset_us: u64,
        wait_us: u64,
        cpu_us: u64,
        cpu_wall_us: u64,
        total_us: u64,
    ) -> String {
        let cpu_wall_us = cpu_wall_us.max(cpu_us);
        let cpu_gap_us = cpu_wall_us.saturating_sub(cpu_us);
        let duration_us = wait_us.saturating_add(cpu_wall_us);
        if duration_us == 0 || total_us == 0 {
            return String::new();
        }
        let left = offset_us as f64 / total_us as f64 * 100.0;
        let width = (duration_us as f64 / total_us as f64 * 100.0).max(0.3);
        let wait_width = wait_us as f64 / duration_us as f64 * 100.0;
        let cpu_width = cpu_us as f64 / duration_us as f64 * 100.0;
        let cpu_gap_width = cpu_gap_us as f64 / duration_us as f64 * 100.0;
        let priority_snapshot = task.priority_snapshots.get(slice_index).copied().flatten();
        let title = slice_title(
            task,
            slice_index,
            wait_us,
            cpu_us,
            cpu_wall_us,
            duration_us,
            priority_snapshot,
        );
        format!(
            "<div class=\"slice-block\" data-task-id=\"{}\" data-slice-id=\"{}\" style=\"left:{:.4}%;width:{:.4}%\" title=\"{}\"><span class=\"slice-segment state-wait\" style=\"left:0;width:{:.4}%\"></span><span class=\"slice-segment state-cpu\" style=\"left:{:.4}%;width:{:.4}%\"></span><span class=\"slice-segment state-cpu-gap\" style=\"left:{:.4}%;width:{:.4}%\"></span></div>",
            task_dom_id(task),
            slice_dom_id(task, slice_index),
            left,
            width.min(100.0 - left.min(100.0)),
            escape_html(&title),
            wait_width,
            wait_width,
            cpu_width,
            wait_width + cpu_width,
            cpu_gap_width
        )
    }

    fn render_terminal_slice_block(
        offset_us: u64,
        duration_us: u64,
        total_us: u64,
        class_name: &str,
        label: &str,
    ) -> String {
        if duration_us == 0 || total_us == 0 {
            return String::new();
        }
        let left = offset_us as f64 / total_us as f64 * 100.0;
        let width = (duration_us as f64 / total_us as f64 * 100.0).max(0.3);
        format!(
            "<div class=\"slice-block\" style=\"left:{:.4}%;width:{:.4}%\" title=\"{} {}\"><span class=\"slice-segment {}\" style=\"left:0;width:100%\"></span></div>",
            left,
            width.min(100.0 - left.min(100.0)),
            escape_html(label),
            fmt_ms(duration_us),
            class_name
        )
    }

    fn render_slice_detail_panels(task: &SlowQueryTaskSample) -> String {
        let mut panels = String::new();
        let segment_count = task
            .schedule_waits_us
            .len()
            .max(task.cpu_slices_us.len())
            .max(task.cpu_wall_slices_us.len());
        for index in 0..segment_count {
            let wait_us = task.schedule_waits_us.get(index).copied().unwrap_or(0);
            let cpu_us = task.cpu_slices_us.get(index).copied().unwrap_or(0);
            let cpu_wall_us = task
                .cpu_wall_slices_us
                .get(index)
                .copied()
                .unwrap_or(cpu_us)
                .max(cpu_us);
            let duration_us = wait_us.saturating_add(cpu_wall_us);
            if duration_us == 0 {
                continue;
            }
            panels.push_str(&format!(
                "<div class=\"slice-detail\" data-slice-id=\"{}\"><div class=\"slice-detail-title\">{}</div><div class=\"slice-timeline\">{}</div></div>",
                slice_dom_id(task, index),
                slice_summary(
                    task,
                    index,
                    wait_us,
                    cpu_us,
                    cpu_wall_us,
                    duration_us,
                    task.priority_snapshots.get(index).copied().flatten(),
                ),
                render_slice_detail_segments(wait_us, cpu_us, cpu_wall_us, duration_us)
            ));
        }
        panels
    }

    fn render_slice_detail_segments(
        wait_us: u64,
        cpu_us: u64,
        cpu_wall_us: u64,
        total_us: u64,
    ) -> String {
        let mut segments = String::new();
        let cpu_gap_us = cpu_wall_us.max(cpu_us).saturating_sub(cpu_us);
        if wait_us > 0 {
            segments.push_str(&render_slice_detail_segment(
                "state-wait",
                0,
                wait_us,
                total_us,
                &format!("schedule wait {}", fmt_ms(wait_us)),
            ));
        }
        if cpu_us > 0 {
            segments.push_str(&render_slice_detail_segment(
                "state-cpu",
                wait_us,
                cpu_us,
                total_us,
                &format!("CPU {}", fmt_ms(cpu_us)),
            ));
        }
        if cpu_gap_us > 0 {
            segments.push_str(&render_slice_detail_segment(
                "state-cpu-gap",
                wait_us.saturating_add(cpu_us),
                cpu_gap_us,
                total_us,
                &format!("CPU wall gap {}", fmt_ms(cpu_gap_us)),
            ));
        }
        segments
    }

    fn render_slice_detail_segment(
        class_name: &str,
        offset_us: u64,
        duration_us: u64,
        total_us: u64,
        title: &str,
    ) -> String {
        if duration_us == 0 || total_us == 0 {
            return String::new();
        }
        let left = offset_us as f64 / total_us as f64 * 100.0;
        let width = duration_us as f64 / total_us as f64 * 100.0;
        format!(
            "<span class=\"slice-detail-segment {}\" style=\"left:{:.4}%;width:{:.4}%\" title=\"{}\"></span>",
            class_name,
            left,
            width,
            escape_html(title)
        )
    }

    fn render_kv_pairs(pairs: &[(&str, String)]) -> String {
        pairs
            .iter()
            .map(|(key, value)| {
                format!(
                    "<span class=\"kv\"><span class=\"kv-key\">{}</span><span class=\"kv-value\">{}</span></span>",
                    escape_html(key),
                    escape_html(value)
                )
            })
            .collect::<String>()
    }

    fn task_dom_id(task: &SlowQueryTaskSample) -> String {
        format!("task-{}", task.task_index)
    }

    fn slice_dom_id(task: &SlowQueryTaskSample, slice_index: usize) -> String {
        format!("task-{}-slice-{}", task.task_index, slice_index)
    }

    fn task_summary(task: &SlowQueryTaskSample) -> String {
        render_kv_pairs(&[
            ("Task", task.task_index.to_string()),
            ("Line", task.lane_index.to_string()),
            ("start", fmt_ms(task.created_after_us)),
            ("elapsed", fmt_ms(task.elapsed_us)),
            ("cpu", fmt_ms(task.cpu_us)),
            ("cpu_wall", fmt_ms(task.cpu_wall_us)),
            ("cpu_gap", fmt_ms(task_cpu_wall_gap_us(task))),
            ("sched", fmt_ms(task.schedule_waits_us.iter().sum::<u64>())),
            (
                "first_sched",
                fmt_ms(task.schedule_waits_us.first().copied().unwrap_or(0)),
            ),
            ("acquire", fmt_ms(task.acquire_wait_us)),
            ("rx_wake", fmt_ms(task.rx_wake_delay_us)),
            ("other", fmt_ms(task_other_us(task))),
        ])
    }

    fn slice_summary(
        task: &SlowQueryTaskSample,
        slice_index: usize,
        wait_us: u64,
        cpu_us: u64,
        cpu_wall_us: u64,
        duration_us: u64,
        priority_snapshot: Option<ReadFlowPrioritySnapshot>,
    ) -> String {
        let cpu_gap_us = cpu_wall_us.max(cpu_us).saturating_sub(cpu_us);
        let mut pairs = vec![
            ("Task", task.task_index.to_string()),
            ("Slice", slice_index.to_string()),
            ("total", fmt_ms(duration_us)),
            ("schedule_wait", fmt_ms(wait_us)),
            ("cpu", fmt_ms(cpu_us)),
            ("cpu_wall", fmt_ms(cpu_wall_us)),
            ("cpu_gap", fmt_ms(cpu_gap_us)),
        ];
        if let Some(snapshot) = priority_snapshot {
            pairs.extend([
                ("vt", fmt_us(snapshot.virtual_time_us)),
                ("min_vt", fmt_us(snapshot.min_virtual_time_us)),
                ("priority", snapshot.priority.to_string()),
            ]);
        }
        render_kv_pairs(&pairs)
    }

    fn slice_title(
        task: &SlowQueryTaskSample,
        slice_index: usize,
        wait_us: u64,
        cpu_us: u64,
        cpu_wall_us: u64,
        duration_us: u64,
        priority_snapshot: Option<ReadFlowPrioritySnapshot>,
    ) -> String {
        let cpu_gap_us = cpu_wall_us.max(cpu_us).saturating_sub(cpu_us);
        let priority = priority_snapshot
            .map(|snapshot| {
                format!(
                    "\nvt {}\nmin_vt {}\npriority {}",
                    fmt_us(snapshot.virtual_time_us),
                    fmt_us(snapshot.min_virtual_time_us),
                    snapshot.priority
                )
            })
            .unwrap_or_default();
        format!(
            "Task-{} Slice-{}\ntotal {}\nschedule wait {}\nCPU thread {}\nCPU wall {}\nCPU wall gap {}{}",
            task.task_index,
            slice_index,
            fmt_ms(duration_us),
            fmt_ms(wait_us),
            fmt_ms(cpu_us),
            fmt_ms(cpu_wall_us),
            fmt_ms(cpu_gap_us),
            priority
        )
    }

    fn task_title(task: &SlowQueryTaskSample) -> String {
        format!(
            "Task-{} Line-{}\nstart {}\nelapsed {}\ncpu {}\ncpu wall {}\ncpu gap {}\nacquire {}\nrx wake {}\nother {}\nschedule waits [{}]\ncpu slices [{}]\ncpu wall slices [{}]",
            task.task_index,
            task.lane_index,
            fmt_ms(task.created_after_us),
            fmt_ms(task.elapsed_us),
            fmt_ms(task.cpu_us),
            fmt_ms(task.cpu_wall_us),
            fmt_ms(task_cpu_wall_gap_us(task)),
            fmt_ms(task.acquire_wait_us),
            fmt_ms(task.rx_wake_delay_us),
            fmt_ms(task_other_us(task)),
            format_us_list(&task.schedule_waits_us),
            format_us_list(&task.cpu_slices_us),
            format_us_list(&task.cpu_wall_slices_us)
        )
    }

    fn task_cpu_wall_gap_us(task: &SlowQueryTaskSample) -> u64 {
        task.cpu_wall_us.saturating_sub(task.cpu_us)
    }

    fn task_other_us(task: &SlowQueryTaskSample) -> u64 {
        let schedule_wait_us = task.schedule_waits_us.iter().sum::<u64>();
        task.elapsed_us
            .saturating_sub(schedule_wait_us)
            .saturating_sub(task.cpu_wall_us)
            .saturating_sub(task.rx_wake_delay_us)
    }

    fn render_total_cpu_dataset(series: &BTreeMap<String, Vec<StatsSample>>) -> String {
        let totals = total_cpu_by_time(series);
        if totals.is_empty() {
            return String::new();
        }
        let data = totals
            .into_iter()
            .map(|(time_ms, cpu)| format!("{{x:{:.3},y:{:.3}}}", time_ms as f64 / 1000.0, cpu))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "{{label:\"total\",borderColor:\"#111827\",backgroundColor:\"#111827\",data:[{}],pointRadius:2,borderWidth:3,tension:0.15}}",
            data
        )
    }

    fn total_cpu_by_time(series: &BTreeMap<String, Vec<StatsSample>>) -> BTreeMap<u64, f64> {
        let mut totals = BTreeMap::<u64, f64>::new();
        for samples in series.values() {
            for sample in samples {
                if let Some(cpu) = sample.cpu {
                    *totals.entry(time_key(sample.time_secs)).or_default() += cpu;
                }
            }
        }
        totals
    }

    fn time_key(time_secs: f64) -> u64 {
        (time_secs * 1000.0).round() as u64
    }

    fn render_interaction_script() -> &'static str {
        r#"<script>
document.addEventListener("DOMContentLoaded", function() {
  document.querySelectorAll(".slow-viz").forEach(function(root) {
    function showTask(taskId) {
      const block = root.querySelector('.task-block[data-task-id="' + taskId + '"]');
      const line = block ? block.closest(".lane-section") : root;
      line.querySelectorAll(".task-detail").forEach(function(detail) {
        detail.classList.toggle("is-active", detail.dataset.taskId === taskId);
      });
      line.querySelectorAll(".task-block").forEach(function(block) {
        block.classList.toggle("is-selected", block.dataset.taskId === taskId);
      });
      line.querySelectorAll(".slice-block").forEach(function(block) {
        block.classList.remove("is-selected");
      });
      line.querySelectorAll(".slice-detail").forEach(function(detail) {
        detail.classList.remove("is-active");
      });
    }

    function showSlice(sliceId) {
      const block = root.querySelector('.slice-block[data-slice-id="' + sliceId + '"]');
      const taskDetail = block ? block.closest(".task-detail") : root;
      taskDetail.querySelectorAll(".slice-detail").forEach(function(detail) {
        detail.classList.toggle("is-active", detail.dataset.sliceId === sliceId);
      });
      taskDetail.querySelectorAll(".slice-block").forEach(function(block) {
        block.classList.toggle("is-selected", block.dataset.sliceId === sliceId);
      });
    }

    root.querySelectorAll(".task-block").forEach(function(block) {
      block.addEventListener("click", function() {
        showTask(block.dataset.taskId);
      });
    });

    root.querySelectorAll(".slice-block[data-slice-id]").forEach(function(block) {
      block.addEventListener("click", function(event) {
        event.stopPropagation();
        showSlice(block.dataset.sliceId);
      });
    });
  });
});
</script>"#
    }

    fn render_chart_script(
        series: &BTreeMap<String, Vec<StatsSample>>,
        metrics: &[Metric],
    ) -> String {
        const COLORS: [&str; 10] = [
            "#2563eb", "#dc2626", "#16a34a", "#9333ea", "#ea580c", "#0891b2", "#be123c", "#4f46e5",
            "#65a30d", "#93370d",
        ];
        let mut configs = String::new();
        for metric in metrics {
            let mut datasets = series
                .iter()
                .enumerate()
                .map(|(index, (payload, samples))| {
                    let data = samples
                        .iter()
                        .filter_map(|sample| {
                            metric_value(sample, *metric).map(|value| {
                                format!("{{x:{:.3},y:{:.3}}}", sample.time_secs, value)
                            })
                        })
                        .collect::<Vec<_>>()
                        .join(",");
                    format!(
                        "{{label:\"{}\",borderColor:\"{}\",backgroundColor:\"{}\",data:[{}],pointRadius:2,borderWidth:2,tension:0.15}}",
                        escape_js_string(payload),
                        COLORS[index % COLORS.len()],
                        COLORS[index % COLORS.len()],
                        data
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            if *metric == Metric::Cpu {
                let total = render_total_cpu_dataset(series);
                if !total.is_empty() {
                    if !datasets.is_empty() {
                        datasets = format!("{total},{datasets}");
                    } else {
                        datasets = total;
                    }
                }
            }
            configs.push_str(&format!(
                "createLineChart(\"{}\",\"{}\",\"{}\",\"{}\",[{}]);",
                metric_id(*metric),
                escape_js_string(metric_title(*metric)),
                escape_js_string(metric_y_label(*metric)),
                escape_js_string(metric_unit(*metric)),
                datasets
            ));
        }
        format!(
            "<script>
function createLineChart(id, title, yLabel, unit, datasets) {{
  new Chart(document.getElementById(id), {{
    type: \"line\",
    data: {{ datasets: datasets }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      parsing: false,
      interaction: {{ mode: \"nearest\", axis: \"x\", intersect: false }},
      plugins: {{
        legend: {{
          position: \"bottom\",
          onClick: function(_event, legendItem, legend) {{
            const chart = legend.chart;
            const clicked = legendItem.datasetIndex;
            const onlyClickedVisible = chart.data.datasets.every(function(_dataset, index) {{
              return index === clicked ? chart.isDatasetVisible(index) : !chart.isDatasetVisible(index);
            }});
            chart.data.datasets.forEach(function(_dataset, index) {{
              chart.setDatasetVisibility(index, onlyClickedVisible || index === clicked);
            }});
            chart.update();
          }}
        }},
        tooltip: {{
          enabled: true,
          mode: \"nearest\",
          intersect: false,
          callbacks: {{
            title: function(items) {{
              if (!items.length) return \"\";
              return \"time \" + Number(items[0].parsed.x).toFixed(1) + \"s\";
            }},
            label: function(item) {{
              return item.dataset.label + \": \" + Number(item.parsed.y).toFixed(2) + unit;
            }}
          }}
        }}
      }},
      scales: {{
        x: {{ type: \"linear\", title: {{ display: true, text: \"time (s)\" }} }},
        y: {{ beginAtZero: true, title: {{ display: true, text: yLabel }} }}
      }}
    }}
  }});
}}
{}</script>",
            configs
        )
    }

    fn metric_id(metric: Metric) -> &'static str {
        match metric {
            Metric::Cpu => "chart-cpu",
            Metric::AvgTaskMs => "chart-avg-task-ms",
            Metric::Qps => "chart-qps",
            Metric::AvgMs => "chart-avg-ms",
            Metric::TotalScheduleMs => "chart-total-schedule-ms",
            Metric::FirstScheduleMs => "chart-first-schedule-ms",
            Metric::AcquireMs => "chart-acquire-ms",
            Metric::P50Ms => "chart-p50-ms",
            Metric::P80Ms => "chart-p80-ms",
            Metric::P99Ms => "chart-p99-ms",
            Metric::P999Ms => "chart-p999-ms",
            Metric::Errors => "chart-errors",
        }
    }

    fn metric_y_label(metric: Metric) -> &'static str {
        match metric {
            Metric::Cpu => "% of single CPU",
            Metric::Qps => "QPS",
            Metric::Errors => "errors/s",
            Metric::AvgTaskMs
            | Metric::AvgMs
            | Metric::TotalScheduleMs
            | Metric::FirstScheduleMs
            | Metric::AcquireMs
            | Metric::P50Ms
            | Metric::P80Ms
            | Metric::P99Ms
            | Metric::P999Ms => "ms",
        }
    }

    fn metric_unit(metric: Metric) -> &'static str {
        match metric {
            Metric::Cpu => "%",
            Metric::Qps => " QPS",
            Metric::Errors => " errors/s",
            Metric::AvgTaskMs
            | Metric::AvgMs
            | Metric::TotalScheduleMs
            | Metric::FirstScheduleMs
            | Metric::AcquireMs
            | Metric::P50Ms
            | Metric::P80Ms
            | Metric::P99Ms
            | Metric::P999Ms => "ms",
        }
    }

    fn metric_title(metric: Metric) -> &'static str {
        match metric {
            Metric::Cpu => "CPU (% of single CPU)",
            Metric::AvgTaskMs => "task_avg_ms",
            Metric::Qps => "qps",
            Metric::AvgMs => "avg_ms",
            Metric::TotalScheduleMs => "sched_ms",
            Metric::FirstScheduleMs => "first_sched_ms",
            Metric::AcquireMs => "acquire_ms",
            Metric::P50Ms => "p50_ms",
            Metric::P80Ms => "p80_ms",
            Metric::P99Ms => "p99_ms",
            Metric::P999Ms => "p999_ms",
            Metric::Errors => "err/s",
        }
    }

    fn metric_value(sample: &StatsSample, metric: Metric) -> Option<f64> {
        match metric {
            Metric::Cpu => sample.cpu,
            Metric::AvgTaskMs => sample.avg_task_ms,
            Metric::Qps => sample.qps,
            Metric::AvgMs => sample.avg_ms,
            Metric::TotalScheduleMs => sample.total_schedule_ms,
            Metric::FirstScheduleMs => sample.first_schedule_ms,
            Metric::AcquireMs => sample.acquire_ms,
            Metric::P50Ms => sample.p50_ms,
            Metric::P80Ms => sample.p80_ms,
            Metric::P99Ms => sample.p99_ms,
            Metric::P999Ms => sample.p999_ms,
            Metric::Errors => sample.errors,
        }
    }

    fn percentile(sorted: &[u64], p: f64) -> u64 {
        if sorted.is_empty() {
            return 0;
        }
        let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
        sorted[idx]
    }

    fn avg_ms_for_values(values: &[u64]) -> Option<f64> {
        if values.is_empty() {
            return None;
        }
        Some(values.iter().sum::<u64>() as f64 / values.len() as f64 / 1000.0)
    }

    fn fmt_opt(value: Option<f64>) -> String {
        value
            .map(|value| format!("{value:.2}"))
            .unwrap_or_else(|| "-".to_string())
    }

    fn fmt_html_opt(value: Option<f64>) -> String {
        value
            .map(|value| format!("{value:.2}"))
            .unwrap_or_else(|| "-".to_string())
    }

    fn fmt_ms(us: u64) -> String {
        format!("{:.2}ms", us as f64 / 1000.0)
    }

    fn fmt_us(us: u64) -> String {
        format!("{us}us")
    }

    fn format_us_list(values: &[u64]) -> String {
        values
            .iter()
            .map(|value| fmt_ms(*value))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn escape_html(value: &str) -> String {
        value
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
            .replace('\'', "&#39;")
    }

    fn escape_js_string(value: &str) -> String {
        value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
            .replace('\r', "\\r")
    }

    fn format_elapsed(duration: Duration) -> String {
        let mut secs = duration.as_secs();
        let hours = secs / 3600;
        secs %= 3600;
        let minutes = secs / 60;
        secs %= 60;

        if hours > 0 {
            format!("{hours}h{minutes}m{secs}s")
        } else if minutes > 0 {
            format!("{minutes}m{secs}s")
        } else {
            format!("{secs}s")
        }
    }
}

fn main() {
    let cfg = load_config();
    println!(
        "start read flow fairness sim: duration={:?}, yatp_threads={}, max_flow_concurrency={}, payloads={}",
        cfg.simulate.duration.0,
        cfg.simulate.yatp_threads,
        cfg.simulate.max_flow_concurrency,
        cfg.payloads.len()
    );
    let runtime = Builder::new_current_thread().enable_time().build().unwrap();
    let local = LocalSet::new();
    local.block_on(&runtime, run_simulation(cfg));
}
