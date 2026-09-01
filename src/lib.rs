use std::path::PathBuf;
use std::sync::Arc;

use http::Request;
use tokio_util::sync::CancellationToken;

use muzanci_transport::MUZANCI_RUNNER_ID_HEADER;
use muzanci_transport::MUZANCI_TRANSPORT_V1;
use muzanci_transport::channel::FnChannelAcceptor;
use muzanci_transport::message::RunnerId;
use muzanci_transport::mux::Mux;
use muzanci_transport::mux::MuxHandle;

use crate::assignment_capacity::SharedAssignmentCapacityHandle;
use crate::evaluation_capacity::SharedEvaluationCapacity;
use crate::sandbox::Sandboxer;

pub mod assignment_capacity;
pub mod channel;
pub mod evaluation_capacity;
pub mod logging;
pub mod sandbox;
pub mod signal_receiver;
pub mod ssh;

#[derive(Clone)]
pub struct RunnerState {
    cancellation_token: CancellationToken,
    runner_id: RunnerId,
    mux_handle: MuxHandle,
    shared_evaluation_capacity: SharedEvaluationCapacity,
    shared_assignment_capacity_handle: SharedAssignmentCapacityHandle,
    sandboxer: Arc<dyn Sandboxer>,
    evaluator_dir_root: PathBuf,
}

impl RunnerState {
    pub fn new(
        cancellation_token: CancellationToken,
        runner_id: RunnerId,
        mux_handle: MuxHandle,
        shared_evaluation_capacity: SharedEvaluationCapacity,
        shared_assignment_capacity_handle: SharedAssignmentCapacityHandle,
        sandboxer: Arc<dyn Sandboxer>,
        evaluator_dir_root: PathBuf,
    ) -> Self {
        Self {
            cancellation_token,
            runner_id,
            mux_handle,
            shared_evaluation_capacity,
            shared_assignment_capacity_handle,
            sandboxer,
            evaluator_dir_root,
        }
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }
}

pub async fn connect(
    hostname: &str,
    cancellation_token: CancellationToken,
) -> anyhow::Result<(RunnerId, MuxHandle)> {
    let server_stream = {
        let stream = tokio::net::TcpStream::connect(hostname).await?;
        stream.set_nodelay(true)?;
        hyper_util::rt::TokioIo::new(stream)
    };

    let (mut send_request, connection) =
        hyper::client::conn::http1::handshake(server_stream).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.with_upgrades().await {
            eprintln!("Connection error: {:?}", e);
        }
    });

    let request = Request::builder()
        .method("POST")
        .uri("/runner/register")
        .header(http::header::HOST, hostname)
        .header(http::header::CONNECTION, "Upgrade")
        .header(http::header::UPGRADE, MUZANCI_TRANSPORT_V1)
        .body(http_body_util::Empty::<bytes::Bytes>::new())
        .unwrap();

    let response = send_request.send_request(request).await?;

    if response.status() != http::StatusCode::SWITCHING_PROTOCOLS {
        return Err(anyhow::anyhow!(
            "Failed to upgrade connection. Server responded with status: {}",
            response.status()
        ));
    }

    let runner_id = response
        .headers()
        .get(MUZANCI_RUNNER_ID_HEADER)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<RunnerId>().ok())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Missing or invalid {} header in response",
                MUZANCI_RUNNER_ID_HEADER
            )
        })?;

    let server_stream = hyper::upgrade::on(response).await?;
    let server_stream = hyper_util::rt::TokioIo::new(server_stream);
    let (read_half, write_half) = tokio::io::split(server_stream);

    let channel_acceptor = FnChannelAcceptor::new(move |channel_id, channel_type| {
        panic!(
            "Runner received request to open channel [{}] of type {:?}",
            channel_id, channel_type
        );
    });

    let (_join_handle, mux_handle) =
        Mux::spawn(read_half, write_half, channel_acceptor, cancellation_token);

    Ok((runner_id, mux_handle))
}
