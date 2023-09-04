use crate::network::BeaconNetwork;
use portalnet::events::OverlayRequest;
use portalnet::types::messages::Message;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{error, warn, Instrument};

pub struct BeaconEvents {
    pub network: Arc<BeaconNetwork>,
    pub message_rx: UnboundedReceiver<OverlayRequest>,
}

impl BeaconEvents {
    pub async fn start(mut self) {
        loop {
            tokio::select! {
                Some(msg) = self.message_rx.recv() => {
                    self.handle_beacon_message(msg);
                } else => {
                    error!("Beacon event channel closed, shutting down");
                    break;
                }
            }
        }
    }

    /// Handle beacon network TalkRequest event
    fn handle_beacon_message(&self, msg: OverlayRequest) {
        let network = Arc::clone(&self.network);
        tokio::spawn(async move {
            match msg {
                OverlayRequest::Talk(talk_request) => {
                    let talk_request_id = talk_request.id().clone();
                    let reply = match network
                        .overlay
                        .process_one_request(&talk_request)
                        .instrument(tracing::info_span!("beacon_network", req = %talk_request_id))
                        .await
                    {
                        Ok(response) => Message::from(response).into(),
                        Err(error) => {
                            error!(
                                error = %error,
                                request.discv5.id = %talk_request_id,
                                "Error processing portal beacon request, responding with empty TALKRESP"
                            );
                            // Return an empty TALKRESP if there was an error executing the request
                            "".into()
                        }
                    };
                    if let Err(error) = talk_request.respond(reply) {
                        warn!(error = %error, request.discv5.id = %talk_request_id, "Error responding to TALKREQ");
                    }
                }
                OverlayRequest::Event(event) => {
                    let _ = network.overlay.process_one_event(event).await;
                }
            }
        });
    }
}
