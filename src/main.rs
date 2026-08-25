use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use muzanci_image::reqwest_registry_client::ReqwestRegistryClient;
use muzanci_runner::RunnerState;
use muzanci_runner::assignment_capacity::SharedAssignmentCapacity;
use muzanci_runner::debugger_scheduler::DebuggerScheduler;
use muzanci_runner::evaluation_capacity::SharedEvaluationCapacity;
use muzanci_runner::evaluator_scheduler::EvaluatorScheduler;
use muzanci_runner::sandbox::fake_sandboxer::FakeSandboxer;
use muzanci_runner::sandbox::jail_sandboxer::JailSandboxer;
use muzanci_runner::sandbox::zfs_image_store::ZfsImageStore;
use muzanci_runner::sandbox::zfs_image_store::ZfsPool;
use muzanci_runner::signal_receiver::SignalReceiver;
use muzanci_runner::worker_scheduler::WorkerScheduler;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let _guard = muzanci_runner::logging::init().unwrap();

    let cancellation_token = CancellationToken::new();
    let hostname = "localhost:8002";
    let (runner_id, mux_handle) = muzanci_runner::connect(hostname, cancellation_token.clone())
        .await
        .unwrap();
    tracing::info!("Assigned runner ID [{}]", runner_id);

    let evaluation_capacity = SharedEvaluationCapacity::new(10);
    let assignment_capacity = SharedAssignmentCapacity::spawn(10);

    // let sandboxer = {
    //     let zfs_pool = ZfsPool::new("zroot");
    //     let registry_client = Arc::new(ReqwestRegistryClient::new());
    //     let root_dir = PathBuf::from("/tmp/runner");
    //     let image_store =
    //         Arc::new(ZfsImageStore::try_new(&root_dir, zfs_pool, registry_client).unwrap());
    //     let bridge_if = "bridge0".to_string();
    //     let num_slots = 10;
    //     Arc::new(JailSandboxer::try_new(&root_dir, bridge_if, image_store, num_slots).unwrap())
    // };
    let sandboxer = Arc::new(FakeSandboxer::new());

    let evaluator_dir_root = PathBuf::from("/tmp/evaluators");

    let runner_state = Arc::new(RunnerState::new(
        cancellation_token,
        runner_id,
        mux_handle,
        evaluation_capacity,
        assignment_capacity,
        sandboxer,
        evaluator_dir_root,
    ));

    let evaluator_scheduler_handle = EvaluatorScheduler::spawn(runner_state.clone());
    let worker_scheduler_handle = WorkerScheduler::spawn(runner_state.clone());
    let signal_receiver_handle = SignalReceiver::spawn(runner_state.clone());
    let debugger_scheduler_handle = DebuggerScheduler::spawn(runner_state.clone());

    // TODO: Add cancellation token for graceful shutdown.
    let _ = tokio::join!(
        evaluator_scheduler_handle,
        worker_scheduler_handle,
        signal_receiver_handle,
        debugger_scheduler_handle
    );
}
