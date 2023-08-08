use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use discv5::enr::NodeId;
use discv5::TalkRequest;
use futures::stream::{select_all, StreamExt};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::BroadcastStream;
use tracing::{error, warn};

use super::types::messages::ProtocolId;
use ethportal_api::types::enr::Enr;
use ethportal_api::utils::bytes::{hex_encode, hex_encode_upper};

/// Main handler for portal network events
pub struct PortalnetEvents {
    /// Receive Discv5 talk requests.
    pub talk_req_receiver: mpsc::Receiver<TalkRequest>,
    /// Send overlay `TalkReq` to history network && receive event subscriptions
    pub history_overlay_tx_rx: (
        Option<mpsc::UnboundedSender<OverlayMessage>>,
        Option<broadcast::Receiver<EventEnvelope>>,
    ),
    /// Send overlay `TalkReq` to state network && receive event subscriptions
    pub state_overlay_tx_rx: (
        Option<mpsc::UnboundedSender<OverlayMessage>>,
        Option<broadcast::Receiver<EventEnvelope>>,
    ),
    /// Send overlay `TalkReq` to beacon network && receive event subscriptions
    pub beacon_overlay_tx_rx: (
        Option<mpsc::UnboundedSender<OverlayMessage>>,
        Option<broadcast::Receiver<EventEnvelope>>,
    ),
    /// Send TalkReq events with "utp" protocol id to `UtpListener`
    pub utp_talk_reqs: mpsc::UnboundedSender<TalkRequest>,
}

impl PortalnetEvents {
    pub async fn new(
        talk_req_receiver: mpsc::Receiver<TalkRequest>,
        history_overlay_tx_rx: (
            Option<mpsc::UnboundedSender<OverlayMessage>>,
            Option<broadcast::Receiver<EventEnvelope>>,
        ),
        state_overlay_tx_rx: (
            Option<mpsc::UnboundedSender<OverlayMessage>>,
            Option<broadcast::Receiver<EventEnvelope>>,
        ),
        beacon_overlay_tx_rx: (
            Option<mpsc::UnboundedSender<OverlayMessage>>,
            Option<broadcast::Receiver<EventEnvelope>>,
        ),
        utp_talk_reqs: mpsc::UnboundedSender<TalkRequest>,
    ) -> Self {
        Self {
            talk_req_receiver,
            history_overlay_tx_rx,
            state_overlay_tx_rx,
            beacon_overlay_tx_rx,
            utp_talk_reqs,
        }
    }

    /// Main loop to dispatch `Discv5` and uTP events
    pub async fn start(mut self) {
        let mut receivers = vec![];

        if let Some(rx) = self.history_overlay_tx_rx.1.take() {
            receivers.push(rx);
        }
        if let Some(rx) = self.beacon_overlay_tx_rx.1.take() {
            receivers.push(rx);
        }
        if let Some(rx) = self.state_overlay_tx_rx.1.take() {
            receivers.push(rx);
        }

        if receivers.is_empty() {
            error!("Fused stream has zero receivers");
        }
        let mut fused_streams = select_all(receivers.into_iter().map(BroadcastStream::new));

        loop {
            tokio::select! {
                Some(talk_req) = self.talk_req_receiver.recv() => {
                    self.dispatch_discv5_talk_req(talk_req);
                }
                Some(event) = fused_streams.next() => {
                    match event {
                        Ok(event) => self.dispatch_overlay_event(event),
                        Err(e) => error!(
                            error = %e,
                            "Error receiving from event stream"
                        )
                    }
                }
            }
        }
    }

    /// Dispatch Discv5 TalkRequest event to overlay networks or uTP socket
    fn dispatch_discv5_talk_req(&self, request: TalkRequest) {
        let protocol_id = ProtocolId::from_str(&hex_encode_upper(request.protocol()));

        match protocol_id {
            Ok(protocol) => match protocol {
                ProtocolId::History => self.send_overlay_message(
                    &self.history_overlay_tx_rx.0,
                    OverlayMessage::Request(request),
                    "history",
                ),
                ProtocolId::Beacon => self.send_overlay_message(
                    &self.beacon_overlay_tx_rx.0,
                    OverlayMessage::Request(request),
                    "beacon",
                ),
                ProtocolId::State => self.send_overlay_message(
                    &self.state_overlay_tx_rx.0,
                    OverlayMessage::Request(request),
                    "state",
                ),
                ProtocolId::Utp => {
                    if let Err(err) = self.utp_talk_reqs.send(request) {
                        error!(%err, "Error forwarding talk request to uTP socket");
                    }
                }
                _ => {
                    warn!(
                        "Received TalkRequest on unknown protocol from={} protocol={} body={}",
                        request.node_id(),
                        hex_encode_upper(request.protocol()),
                        hex_encode(request.body()),
                    );
                }
            },
            Err(_) => warn!("Unable to decode protocol id"),
        }
    }

    fn dispatch_overlay_event(&self, event: EventEnvelope) {
        let protocol_id = &event.protocol;

        if protocol_id != &ProtocolId::History {
            self.send_overlay_message(
                &self.history_overlay_tx_rx.0,
                OverlayMessage::Event(event.clone()),
                "history",
            );
        }
        if protocol_id != &ProtocolId::Beacon {
            self.send_overlay_message(
                &self.beacon_overlay_tx_rx.0,
                OverlayMessage::Event(event.clone()),
                "beacon",
            );
        }
        if protocol_id != &ProtocolId::State {
            self.send_overlay_message(
                &self.beacon_overlay_tx_rx.0,
                OverlayMessage::Event(event.clone()),
                "state",
            );
        }
    }

    fn send_overlay_message(
        &self,
        tx: &Option<mpsc::UnboundedSender<OverlayMessage>>,
        msg: OverlayMessage,
        network: &'static str,
    ) {
        match tx {
            Some(tx) => {
                if matches!(msg, OverlayMessage::Event(_)) {
                    tracing::trace!("Dispatching overlay event {:?} to {} service", msg, network);
                }

                if let Err(err) = tx.send(msg) {
                    error!("Error sending discv5 talk request to {network} network: {err}");
                }
            }
            None => error!("{network} event handler not initialized!"),
        };
    }
}

#[derive(Debug)]
/// Messages that can be sent to an Overlay.
pub enum OverlayMessage {
    /// A TALK-REQ message.
    Request(TalkRequest),
    /// An event discovered by a different overlay.
    Event(EventEnvelope),
}

/// Events that can be produced by the `OverlayProtocol` event stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OverlayEvent {
    LightClientOptimisticUpdate,
    LightClientFinalityUpdate,
    /// A peer went offline
    PeerDisconnected(NodeId),
    /// A peer updated its ENR
    EnrUpdated {
        node_id: NodeId,
        new_record: Arc<Enr>,
    },
}

/// Timestamp of an overlay event.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Timestamp {
    /// Timestamp not available.
    NotAvailable,
    /// Event creation time.
    CreateTime(i64),
}

impl Timestamp {
    /// Convert the timestamp to milliseconds since epoch.
    pub fn to_millis(self) -> Option<i64> {
        match self {
            Timestamp::NotAvailable | Timestamp::CreateTime(-1) => None,
            Timestamp::CreateTime(t) => Some(t),
        }
    }

    /// Creates a new `Timestamp::CreateTime` representing the current time.
    pub fn now() -> Timestamp {
        Timestamp::from(SystemTime::now())
    }
}

impl From<i64> for Timestamp {
    fn from(system_time: i64) -> Timestamp {
        Timestamp::CreateTime(system_time)
    }
}

impl From<SystemTime> for Timestamp {
    fn from(system_time: SystemTime) -> Timestamp {
        Timestamp::CreateTime(millis_to_epoch(system_time))
    }
}

/// A wrapper around an overlay event that includes additional metadata.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct EventEnvelope {
    /// The time at which this event was generated.
    pub timestamp: Timestamp,
    /// The protocol that generated this event.
    pub protocol: ProtocolId,
    /// The event.
    pub payload: OverlayEvent,
}

impl EventEnvelope {
    pub fn new(payload: OverlayEvent, protocol: ProtocolId) -> Self {
        let timestamp = Timestamp::now();
        Self {
            timestamp,
            protocol,
            payload,
        }
    }
}

/// Converts the given time to the number of milliseconds since the Unix epoch.
pub fn millis_to_epoch(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as i64
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::SystemTime;

    #[test]
    fn test_timestamp_creation() {
        let now = SystemTime::now();
        let t1 = Timestamp::now();
        let t2 = Timestamp::from(now);
        let expected = Timestamp::CreateTime(millis_to_epoch(now));

        assert_eq!(t2, expected);
        assert!(t1.to_millis().unwrap() - t2.to_millis().unwrap() < 10);
    }

    #[test]
    fn test_timestamp_conversion() {
        assert_eq!(Timestamp::CreateTime(100).to_millis(), Some(100));
        assert_eq!(Timestamp::CreateTime(-1).to_millis(), None);
        assert_eq!(Timestamp::NotAvailable.to_millis(), None);
        let t: Timestamp = 100.into();
        assert_eq!(t, Timestamp::CreateTime(100));
    }
}
