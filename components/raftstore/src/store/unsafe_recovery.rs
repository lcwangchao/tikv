// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt, mem,
    sync::{mpsc::SyncSender, Arc, Mutex},
};

use collections::HashSet;
use crossbeam::channel::SendError;
use engine_traits::{KvEngine, RaftEngine};
use kvproto::{
    metapb,
    pdpb::{PeerReport, StoreReport},
};
use tikv_util::{box_err, error, info, time::Instant as TiInstant, warn};

use super::{PeerMsg, RaftRouter, SignificantMsg, SignificantRouter, StoreMsg};
use crate::Result;

/// A handle for PD to schedule online unsafe recovery commands back to
/// raftstore.
pub trait UnsafeRecoveryHandle: Sync + Send {
    fn send_enter_force_leader(
        &self,
        region_id: u64,
        syncer: UnsafeRecoveryForceLeaderSyncer,
        failed_stores: HashSet<u64>,
    ) -> Result<()>;

    fn broadcast_exit_force_leader(&self);

    fn send_create_peer(
        &self,
        region: metapb::Region,
        syncer: UnsafeRecoveryExecutePlanSyncer,
    ) -> Result<()>;

    fn send_destroy_peer(
        &self,
        region_id: u64,
        syncer: UnsafeRecoveryExecutePlanSyncer,
    ) -> Result<()>;

    fn broadcast_wait_apply(&self, syncer: UnsafeRecoveryWaitApplySyncer);

    fn broadcast_fill_out_report(&self, syncer: UnsafeRecoveryFillOutReportSyncer);

    fn send_report(&self, report: StoreReport) -> Result<()>;
}

impl<EK: KvEngine, ER: RaftEngine> UnsafeRecoveryHandle for Mutex<RaftRouter<EK, ER>> {
    fn send_enter_force_leader(
        &self,
        region_id: u64,
        syncer: UnsafeRecoveryForceLeaderSyncer,
        failed_stores: HashSet<u64>,
    ) -> Result<()> {
        let router = self.lock().unwrap();
        router.significant_send(
            region_id,
            SignificantMsg::EnterForceLeaderState {
                syncer,
                failed_stores,
            },
        )
    }

    fn broadcast_exit_force_leader(&self) {
        let router = self.lock().unwrap();
        router.broadcast_normal(|| PeerMsg::SignificantMsg(SignificantMsg::ExitForceLeaderState));
    }

    fn send_create_peer(
        &self,
        region: metapb::Region,
        syncer: UnsafeRecoveryExecutePlanSyncer,
    ) -> Result<()> {
        let router = self.lock().unwrap();
        match router.force_send_control(StoreMsg::UnsafeRecoveryCreatePeer {
            syncer,
            create: region,
        }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(box_err!("fail to send unsafe recovery create peer")),
        }
    }

    fn send_destroy_peer(
        &self,
        region_id: u64,
        syncer: UnsafeRecoveryExecutePlanSyncer,
    ) -> Result<()> {
        let router = self.lock().unwrap();
        match router.significant_send(region_id, SignificantMsg::UnsafeRecoveryDestroy(syncer)) {
            // The peer may be destroy already.
            Err(crate::Error::RegionNotFound(_)) => Ok(()),
            res => res,
        }
    }

    fn broadcast_wait_apply(&self, syncer: UnsafeRecoveryWaitApplySyncer) {
        let router = self.lock().unwrap();
        router.broadcast_normal(|| {
            PeerMsg::SignificantMsg(SignificantMsg::UnsafeRecoveryWaitApply(syncer.clone()))
        });
    }

    fn broadcast_fill_out_report(&self, syncer: UnsafeRecoveryFillOutReportSyncer) {
        let router = self.lock().unwrap();
        router.broadcast_normal(|| {
            PeerMsg::SignificantMsg(SignificantMsg::UnsafeRecoveryFillOutReport(syncer.clone()))
        });
    }

    fn send_report(&self, report: StoreReport) -> Result<()> {
        let router = self.lock().unwrap();
        match router.force_send_control(StoreMsg::UnsafeRecoveryReport(report)) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(box_err!("fail to send unsafe recovery store report")),
        }
    }
}

#[derive(Debug)]
/// ForceLeader process would be:
/// - If it's hibernated, enter wait ticks state, and wake up the peer
/// - Enter pre force leader state, become candidate and send request vote to
///   all peers
/// - Wait for the responses of the request vote, no reject should be received.
/// - Enter force leader state, become leader without leader lease
/// - Execute recovery plan(some remove-peer commands)
/// - After the plan steps are all applied, exit force leader state
pub enum ForceLeaderState {
    WaitTicks {
        syncer: UnsafeRecoveryForceLeaderSyncer,
        failed_stores: HashSet<u64>,
        ticks: usize,
    },
    PreForceLeader {
        syncer: UnsafeRecoveryForceLeaderSyncer,
        failed_stores: HashSet<u64>,
    },
    ForceLeader {
        time: TiInstant,
        failed_stores: HashSet<u64>,
    },
}

// Following shared states are used while reporting to PD for unsafe recovery
// and shared among all the regions per their life cycle.
// The work flow is like:
// 1. report phase
//   - start_unsafe_recovery_report
//      - broadcast wait-apply commands
//      - wait for all the peers' apply indices meet their targets
//      - broadcast fill out report commands
//      - wait for all the peers fill out the reports for themselves
//      - send a store report (through store heartbeat)
// 2. force leader phase
//   - dispatch force leader commands
//     - wait for all the peers that received the command become force leader
//     - start_unsafe_recovery_report
// 3. plan execution phase
//   - dispatch recovery plans
//     - wait for all the creates, deletes and demotes to finish, for the
//       demotes, procedures are:
//       - exit joint state if it is already in joint state
//       - demote failed voters, and promote self to be a voter if it is a
//         learner
//       - exit joint state
//     - start_unsafe_recovery_report

// A wrapper of a closure that will be invoked when it is dropped.
// This design has two benefits:
//   1. Using a closure (dynamically dispatched), so that it can avoid having
//      generic member fields like RaftRouter, thus avoid having Rust generic
//      type explosion problem.
//   2. Invoke on drop, so that it can be easily and safely used (together with
//      Arc) as a coordinator between all concerning peers. Each of the peers
//      holds a reference to the same strcuture, and whoever finishes the task
//      drops its reference. Once the last reference is dropped, indicating all
//      the peers have finished their own tasks, the closure is invoked.
pub struct InvokeClosureOnDrop(Option<Box<dyn FnOnce() + Send + Sync>>);

impl fmt::Debug for InvokeClosureOnDrop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InvokeClosureOnDrop")
    }
}

impl Drop for InvokeClosureOnDrop {
    fn drop(&mut self) {
        if let Some(on_drop) = self.0.take() {
            on_drop();
        }
    }
}

pub fn start_unsafe_recovery_report(
    router: Arc<dyn UnsafeRecoveryHandle>,
    report_id: u64,
    exit_force_leader: bool,
) {
    let wait_apply =
        UnsafeRecoveryWaitApplySyncer::new(report_id, router.clone(), exit_force_leader);
    router.broadcast_wait_apply(wait_apply);
}

#[derive(Clone, Debug)]
pub struct UnsafeRecoveryForceLeaderSyncer(Arc<InvokeClosureOnDrop>);

impl UnsafeRecoveryForceLeaderSyncer {
    pub fn new(report_id: u64, router: Arc<dyn UnsafeRecoveryHandle>) -> Self {
        let inner = InvokeClosureOnDrop(Some(Box::new(move || {
            info!("Unsafe recovery, force leader finished.");
            start_unsafe_recovery_report(router, report_id, false);
        })));
        UnsafeRecoveryForceLeaderSyncer(Arc::new(inner))
    }
}

#[derive(Clone, Debug)]
pub struct UnsafeRecoveryExecutePlanSyncer {
    _closure: Arc<InvokeClosureOnDrop>,
    abort: Arc<Mutex<bool>>,
}

impl UnsafeRecoveryExecutePlanSyncer {
    pub fn new(report_id: u64, router: Arc<dyn UnsafeRecoveryHandle>) -> Self {
        let abort = Arc::new(Mutex::new(false));
        let abort_clone = abort.clone();
        let closure = InvokeClosureOnDrop(Some(Box::new(move || {
            info!("Unsafe recovery, plan execution finished");
            if *abort_clone.lock().unwrap() {
                warn!("Unsafe recovery, plan execution aborted");
                return;
            }
            start_unsafe_recovery_report(router, report_id, true);
        })));
        UnsafeRecoveryExecutePlanSyncer {
            _closure: Arc::new(closure),
            abort,
        }
    }

    pub fn abort(&self) {
        *self.abort.lock().unwrap() = true;
    }
}
// Syncer only send to leader in 2nd BR restore
#[derive(Clone, Debug)]
pub struct SnapshotRecoveryWaitApplySyncer {
    _closure: Arc<InvokeClosureOnDrop>,
    abort: Arc<Mutex<bool>>,
}

impl SnapshotRecoveryWaitApplySyncer {
    pub fn new(region_id: u64, sender: SyncSender<u64>) -> Self {
        let thread_safe_router = Mutex::new(sender);
        let abort = Arc::new(Mutex::new(false));
        let abort_clone = abort.clone();
        let closure = InvokeClosureOnDrop(Some(Box::new(move || {
            info!("region {} wait apply finished", region_id);
            if *abort_clone.lock().unwrap() {
                warn!("wait apply aborted");
                return;
            }
            let router_ptr = thread_safe_router.lock().unwrap();

            _ = router_ptr.send(region_id).map_err(|_| {
                warn!("reply waitapply states failure.");
            });
        })));
        SnapshotRecoveryWaitApplySyncer {
            _closure: Arc::new(closure),
            abort,
        }
    }

    pub fn abort(&self) {
        *self.abort.lock().unwrap() = true;
    }
}

#[derive(Clone, Debug)]
pub struct UnsafeRecoveryWaitApplySyncer {
    _closure: Arc<InvokeClosureOnDrop>,
    abort: Arc<Mutex<bool>>,
}

impl UnsafeRecoveryWaitApplySyncer {
    pub fn new(
        report_id: u64,
        router: Arc<dyn UnsafeRecoveryHandle>,
        exit_force_leader: bool,
    ) -> Self {
        let abort = Arc::new(Mutex::new(false));
        let abort_clone = abort.clone();
        let closure = InvokeClosureOnDrop(Some(Box::new(move || {
            info!("Unsafe recovery, wait apply finished");
            if *abort_clone.lock().unwrap() {
                warn!("Unsafe recovery, wait apply aborted");
                return;
            }
            if exit_force_leader {
                router.broadcast_exit_force_leader();
            }
            let fill_out_report = UnsafeRecoveryFillOutReportSyncer::new(report_id, router.clone());
            router.broadcast_fill_out_report(fill_out_report);
        })));
        UnsafeRecoveryWaitApplySyncer {
            _closure: Arc::new(closure),
            abort,
        }
    }

    pub fn abort(&self) {
        *self.abort.lock().unwrap() = true;
    }
}

#[derive(Clone, Debug)]
pub struct UnsafeRecoveryFillOutReportSyncer {
    _closure: Arc<InvokeClosureOnDrop>,
    reports: Arc<Mutex<Vec<PeerReport>>>,
}

impl UnsafeRecoveryFillOutReportSyncer {
    pub fn new(report_id: u64, router: Arc<dyn UnsafeRecoveryHandle>) -> Self {
        let reports = Arc::new(Mutex::new(vec![]));
        let reports_clone = reports.clone();
        let closure = InvokeClosureOnDrop(Some(Box::new(move || {
            info!("Unsafe recovery, peer reports collected");
            let mut store_report = StoreReport::default();
            {
                let mut reports_ptr = reports_clone.lock().unwrap();
                store_report.set_peer_reports(mem::take(&mut *reports_ptr).into());
            }
            store_report.set_step(report_id);
            if let Err(e) = router.send_report(store_report) {
                error!("Unsafe recovery, fail to schedule reporting"; "err" => ?e);
            }
        })));
        UnsafeRecoveryFillOutReportSyncer {
            _closure: Arc::new(closure),
            reports,
        }
    }

    pub fn report_for_self(&self, report: PeerReport) {
        let mut reports_ptr = self.reports.lock().unwrap();
        (*reports_ptr).push(report);
    }
}

pub enum SnapshotRecoveryState {
    // This state is set by the leader peer fsm. Once set, it sync and check leader commit index
    // and force forward to last index once follower appended and then it also is checked
    // every time this peer applies a the last index, if the last index is met, this state is
    // reset / droppeds. The syncer is droped and send the response to the invoker, triggers
    // the next step of recovery process.
    WaitLogApplyToLast {
        target_index: u64,
        syncer: SnapshotRecoveryWaitApplySyncer,
    },
}

pub enum UnsafeRecoveryState {
    // Stores the state that is necessary for the wait apply stage of unsafe recovery process.
    // This state is set by the peer fsm. Once set, it is checked every time this peer applies a
    // new entry or a snapshot, if the target index is met, this state is reset / droppeds. The
    // syncer holds a reference counted inner object that is shared among all the peers, whose
    // destructor triggers the next step of unsafe recovery report process.
    WaitApply {
        target_index: u64,
        syncer: UnsafeRecoveryWaitApplySyncer,
    },
    DemoteFailedVoters {
        syncer: UnsafeRecoveryExecutePlanSyncer,
        failed_voters: Vec<metapb::Peer>,
        target_index: u64,
        // Failed regions may be stuck in joint state, if that is the case, we need to ask the
        // region to exit joint state before proposing the demotion.
        demote_after_exit: bool,
    },
    Destroy(UnsafeRecoveryExecutePlanSyncer),
    WaitInitialize(UnsafeRecoveryExecutePlanSyncer),
}
