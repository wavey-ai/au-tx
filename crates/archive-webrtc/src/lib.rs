//! Reliable, direct-only archive transport shared by the AU and iOS app.

use matchbox_socket::{ChannelConfig, PeerId, PeerState, RtcIceServerConfig, WebRtcSocket};
use std::ffi::{c_char, CStr};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

const CHANNEL_ID: usize = 0;
const MAX_MESSAGE_BYTES: usize = 16 * 1024;
const QUEUE_CAPACITY: usize = 64;
const STATE_DISCOVERING: u32 = 1;
const STATE_CONNECTED: u32 = 2;
const STATE_FAILED: u32 = 3;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct ArchiveWebRtcStatus {
    pub state: u32,
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
}

struct SharedState {
    state: AtomicU32,
    messages_sent: AtomicU64,
    messages_received: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    cancelled: AtomicBool,
    last_error: Mutex<String>,
}

impl SharedState {
    fn new() -> Self {
        Self {
            state: AtomicU32::new(STATE_DISCOVERING),
            messages_sent: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            cancelled: AtomicBool::new(false),
            last_error: Mutex::new(String::new()),
        }
    }

    fn fail(&self, message: impl Into<String>) {
        if let Ok(mut error) = self.last_error.lock() {
            *error = message.into();
        }
        self.state.store(STATE_FAILED, Ordering::Release);
    }

    fn snapshot(&self) -> ArchiveWebRtcStatus {
        ArchiveWebRtcStatus {
            state: self.state.load(Ordering::Acquire),
            messages_sent: self.messages_sent.load(Ordering::Acquire),
            messages_received: self.messages_received.load(Ordering::Acquire),
            bytes_sent: self.bytes_sent.load(Ordering::Acquire),
            bytes_received: self.bytes_received.load(Ordering::Acquire),
        }
    }
}

pub struct ArchiveWebRtcHandle {
    state: Arc<SharedState>,
    outbound: SyncSender<Vec<u8>>,
    inbound: Mutex<Receiver<Vec<u8>>>,
}

static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("infidelity-archive-webrtc")
            .enable_all()
            .build()
            .expect("create archive WebRTC runtime")
    })
}

#[no_mangle]
pub unsafe extern "C" fn archive_webrtc_new(
    signaling_url: *const c_char,
) -> *mut ArchiveWebRtcHandle {
    if signaling_url.is_null() {
        return ptr::null_mut();
    }
    let Ok(url) = CStr::from_ptr(signaling_url).to_str() else {
        return ptr::null_mut();
    };
    if !valid_signaling_url(url) {
        return ptr::null_mut();
    }

    let (outbound_tx, outbound_rx) = mpsc::sync_channel(QUEUE_CAPACITY);
    let (inbound_tx, inbound_rx) = mpsc::sync_channel(QUEUE_CAPACITY);
    let state = Arc::new(SharedState::new());
    runtime().spawn(run_socket(
        url.to_owned(),
        Arc::clone(&state),
        outbound_rx,
        inbound_tx,
    ));

    Box::into_raw(Box::new(ArchiveWebRtcHandle {
        state,
        outbound: outbound_tx,
        inbound: Mutex::new(inbound_rx),
    }))
}

#[no_mangle]
pub unsafe extern "C" fn archive_webrtc_destroy(handle: *mut ArchiveWebRtcHandle) {
    if handle.is_null() {
        return;
    }
    let handle = Box::from_raw(handle);
    handle.state.cancelled.store(true, Ordering::Release);
}

#[no_mangle]
pub unsafe extern "C" fn archive_webrtc_status(
    handle: *const ArchiveWebRtcHandle,
) -> ArchiveWebRtcStatus {
    handle
        .as_ref()
        .map(|handle| handle.state.snapshot())
        .unwrap_or_default()
}

#[no_mangle]
pub unsafe extern "C" fn archive_webrtc_send(
    handle: *const ArchiveWebRtcHandle,
    bytes: *const u8,
    length: usize,
) -> bool {
    let Some(handle) = handle.as_ref() else {
        return false;
    };
    if bytes.is_null() || length == 0 || length > MAX_MESSAGE_BYTES {
        return false;
    }
    let message = std::slice::from_raw_parts(bytes, length).to_vec();
    match handle.outbound.try_send(message) {
        Ok(()) => true,
        Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => false,
    }
}

#[no_mangle]
pub unsafe extern "C" fn archive_webrtc_receive(
    handle: *const ArchiveWebRtcHandle,
    destination: *mut u8,
    capacity: usize,
) -> usize {
    let Some(handle) = handle.as_ref() else {
        return 0;
    };
    if destination.is_null() || capacity < MAX_MESSAGE_BYTES {
        return 0;
    }
    let Ok(inbound) = handle.inbound.lock() else {
        return 0;
    };
    let Ok(message) = inbound.try_recv() else {
        return 0;
    };
    ptr::copy_nonoverlapping(message.as_ptr(), destination, message.len());
    message.len()
}

#[no_mangle]
pub unsafe extern "C" fn archive_webrtc_copy_last_error(
    handle: *const ArchiveWebRtcHandle,
    destination: *mut c_char,
    capacity: usize,
) -> usize {
    let Some(handle) = handle.as_ref() else {
        return 0;
    };
    let Ok(error) = handle.state.last_error.lock() else {
        return 0;
    };
    let required = error.len().saturating_add(1);
    if destination.is_null() || capacity == 0 {
        return required;
    }
    let copied = error.len().min(capacity.saturating_sub(1));
    ptr::copy_nonoverlapping(error.as_ptr(), destination.cast::<u8>(), copied);
    *destination.add(copied) = 0;
    required
}

async fn run_socket(
    signaling_url: String,
    state: Arc<SharedState>,
    outbound: Receiver<Vec<u8>>,
    inbound: SyncSender<Vec<u8>>,
) {
    let (mut socket, mut loop_future) = WebRtcSocket::builder(signaling_url)
        .ice_server(RtcIceServerConfig {
            urls: vec!["stun:stun.cloudflare.com:3478".to_owned()],
            username: None,
            credential: None,
        })
        .reconnect_attempts(None)
        .signaling_keep_alive_interval(Some(Duration::from_secs(10)))
        .add_channel(ChannelConfig::reliable())
        .build();
    let mut peer: Option<PeerId> = None;
    let mut tick = tokio::time::interval(Duration::from_millis(5));

    loop {
        tokio::select! {
            result = &mut loop_future => {
                state.fail(match result {
                    Ok(()) => "The direct archive connection ended.".to_owned(),
                    Err(error) => format!("The direct archive connection failed: {error}"),
                });
                return;
            }
            _ = tick.tick() => {
                if state.cancelled.load(Ordering::Acquire) {
                    return;
                }
                for (next_peer, peer_state) in socket.update_peers() {
                    match peer_state {
                        PeerState::Connected => {
                            peer = Some(next_peer);
                            state.state.store(STATE_CONNECTED, Ordering::Release);
                        }
                        PeerState::Disconnected if peer == Some(next_peer) => {
                            peer = None;
                            state.state.store(STATE_DISCOVERING, Ordering::Release);
                        }
                        PeerState::Disconnected => {}
                    }
                }

                for (_, message) in socket.channel_mut(CHANNEL_ID).receive() {
                    let message = message.to_vec();
                    let length = message.len() as u64;
                    match inbound.try_send(message) {
                        Ok(()) => {
                            state.messages_received.fetch_add(1, Ordering::Relaxed);
                            state.bytes_received.fetch_add(length, Ordering::Relaxed);
                        }
                        Err(_) => {
                            state.fail("The direct archive receive queue is full.");
                            return;
                        }
                    }
                }

                let Some(peer) = peer else { continue; };
                for _ in 0..8 {
                    let message = match outbound.try_recv() {
                        Ok(message) => message,
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => return,
                    };
                    let length = message.len() as u64;
                    socket.channel_mut(CHANNEL_ID).send(message.into(), peer);
                    state.messages_sent.fetch_add(1, Ordering::Relaxed);
                    state.bytes_sent.fetch_add(length, Ordering::Relaxed);
                }
            }
        }
    }
}

fn valid_signaling_url(url: &str) -> bool {
    let production = url.starts_with("wss://api.infidelity.io/v1/archive-rendezvous/");
    let direct_protocol = url.contains("protocol=matchbox-v1");
    if production && direct_protocol {
        return true;
    }
    cfg!(debug_assertions)
        && direct_protocol
        && (url.starts_with("ws://127.0.0.1:") || url.starts_with("ws://localhost:"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    #[test]
    fn production_signaling_is_origin_and_protocol_scoped() {
        assert!(valid_signaling_url(
            "wss://api.infidelity.io/v1/archive-rendezvous/id?protocol=matchbox-v1"
        ));
        assert!(!valid_signaling_url(
            "wss://example.com/v1/archive-rendezvous/id?protocol=matchbox-v1"
        ));
        assert!(!valid_signaling_url(
            "wss://api.infidelity.io/v1/archive-rendezvous/id"
        ));
    }

    #[test]
    fn status_layout_defaults_to_idle_values() {
        let status = ArchiveWebRtcStatus::default();
        assert_eq!(status.state, 0);
        assert_eq!(status.messages_sent, 0);
        assert_eq!(status.bytes_received, 0);
    }

    #[test]
    #[ignore = "connects to the production Cloudflare rendezvous"]
    fn production_reliable_data_channel_round_trip() {
        let pairing_id = std::env::var("ARCHIVE_WEBRTC_SMOKE_PAIRING_ID")
            .expect("set ARCHIVE_WEBRTC_SMOKE_PAIRING_ID to a fresh UUID");
        let expires = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + 60_000;
        let base = format!(
            "wss://api.infidelity.io/v1/archive-rendezvous/{pairing_id}?token={}&expires={expires}&protocol=matchbox-v1",
            "A".repeat(43),
        );
        let plugin_url = std::ffi::CString::new(format!("{base}&role=plugin")).unwrap();
        let phone_url = std::ffi::CString::new(format!("{base}&role=phone")).unwrap();

        let plugin = unsafe { archive_webrtc_new(plugin_url.as_ptr()) };
        assert!(!plugin.is_null());
        thread::sleep(Duration::from_millis(750));
        let phone = unsafe { archive_webrtc_new(phone_url.as_ptr()) };
        assert!(!phone.is_null());

        wait_until(Duration::from_secs(20), || unsafe {
            archive_webrtc_status(plugin).state == STATE_CONNECTED
                && archive_webrtc_status(phone).state == STATE_CONNECTED
        });

        let expected = b"infidelity-direct-archive-smoke";
        assert!(unsafe { archive_webrtc_send(plugin, expected.as_ptr(), expected.len()) });
        let mut received = [0_u8; MAX_MESSAGE_BYTES];
        wait_until(Duration::from_secs(10), || {
            let length =
                unsafe { archive_webrtc_receive(phone, received.as_mut_ptr(), received.len()) };
            length == expected.len() && &received[..length] == expected
        });

        unsafe {
            archive_webrtc_destroy(phone);
            archive_webrtc_destroy(plugin);
        }
    }

    fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if condition() {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!("condition was not met within {timeout:?}");
    }
}
