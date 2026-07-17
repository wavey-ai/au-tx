use frame_header::{EncodingFlag, Endianness, FrameHeader};
use rtrb::{Consumer, Producer, PushError, RingBuffer};
use std::cell::UnsafeCell;
use std::ffi::{c_char, c_void, CStr};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub use archive_webrtc::*;

const BITS_PER_SAMPLE: u8 = 24;
const RECONNECT_INTERVAL: Duration = Duration::from_millis(50);
const METADATA_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);
// Keep enough preallocated slots to cover the socket write timeout without
// ever allocating on the render thread. The worker coalesces every accumulated
// burst to its newest quantum before writing, so this capacity is an outage
// cushion rather than an 85 ms FIFO playout queue.
const RING_SIZE: usize = 128;
const DEFAULT_MAX_FRAMES: usize = 4095;
const MAX_HEADER_BYTES: usize = 20;
const SAMPLE_CLOCK_BYTES: usize = 12;
const MAX_METADATA_FIELD_BYTES: usize = 512;
const ARCHIVE_CHUNK_MAGIC: &[u8; 4] = b"IAR1";
const ARCHIVE_CHUNK_VERSION: u16 = 1;
const ARCHIVE_CHUNK_HEADER_BYTES: u16 = 52;
const ARCHIVE_FLAG_DISCONTINUITY: u8 = 1;
const DEFAULT_ARCHIVE_QUOTA_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MIN_ARCHIVE_QUOTA_BYTES: u64 = 16 * 1024 * 1024;
const MAX_ARCHIVE_QUOTA_BYTES: u64 = 64 * 1024 * 1024 * 1024;

#[derive(Debug)]
struct QueuedFrame {
    sample_position: i64,
    transport_generation: u32,
    archive_capture: bool,
    data: Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct TrackMetadata {
    instance_id: Option<String>,
    label: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ArchiveConfig {
    directory: Option<PathBuf>,
    quota_bytes: u64,
    enabled: bool,
}

impl ArchiveConfig {
    fn normalized(directory: PathBuf, quota_bytes: u64) -> Self {
        Self {
            directory: Some(directory),
            quota_bytes: if quota_bytes == 0 {
                DEFAULT_ARCHIVE_QUOTA_BYTES
            } else {
                quota_bytes.clamp(MIN_ARCHIVE_QUOTA_BYTES, MAX_ARCHIVE_QUOTA_BYTES)
            },
            enabled: false,
        }
    }
}

impl TrackMetadata {
    fn is_empty(&self) -> bool {
        self.instance_id.is_none() && self.label.is_none()
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct AudioProcessorStatus {
    pub started: bool,
    pub connected: bool,
    pub frames_queued: u64,
    pub frames_sent: u64,
    pub frames_dropped: u64,
    pub connection_attempts: u64,
    pub connection_failures: u64,
    pub last_connected_unix_ms: u64,
    pub last_send_unix_ms: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct AudioProcessorArchiveStatus {
    pub enabled: bool,
    pub flush_complete: bool,
    pub spool_full: bool,
    pub chunks_spooled: u64,
    pub bytes_spooled: u64,
    pub frames_spooled: u64,
    pub frames_lost: u64,
    pub last_spool_unix_ms: u64,
}

#[derive(Debug, Default)]
struct ArchiveStatusCounters {
    flush_complete: AtomicBool,
    spool_full: AtomicBool,
    chunks_spooled: AtomicU64,
    bytes_spooled: AtomicU64,
    frames_spooled: AtomicU64,
    frames_lost: AtomicU64,
    last_spool_unix_ms: AtomicU64,
}

#[derive(Debug, Default)]
struct ArchiveControl {
    config: RwLock<ArchiveConfig>,
    capture_enabled: AtomicBool,
    version: AtomicU64,
    status: ArchiveStatusCounters,
}

impl ArchiveControl {
    fn configure(&self, config: ArchiveConfig) {
        self.capture_enabled
            .store(config.enabled, Ordering::Release);
        let mut current = self
            .config
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *current == config {
            return;
        }
        *current = config;
        self.status.flush_complete.store(false, Ordering::Release);
        self.status.spool_full.store(false, Ordering::Release);
        self.version.fetch_add(1, Ordering::Release);
    }

    fn set_enabled(&self, enabled: bool) {
        self.capture_enabled.store(enabled, Ordering::Release);
        let mut current = self
            .config
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if current.enabled == enabled {
            return;
        }
        current.enabled = enabled;
        self.status.flush_complete.store(false, Ordering::Release);
        self.status.spool_full.store(false, Ordering::Release);
        self.version.fetch_add(1, Ordering::Release);
    }

    fn snapshot(&self) -> AudioProcessorArchiveStatus {
        let enabled = self
            .config
            .read()
            .map(|config| config.enabled)
            .unwrap_or(false);
        AudioProcessorArchiveStatus {
            enabled,
            flush_complete: self.status.flush_complete.load(Ordering::Acquire),
            spool_full: self.status.spool_full.load(Ordering::Acquire),
            chunks_spooled: self.status.chunks_spooled.load(Ordering::Acquire),
            bytes_spooled: self.status.bytes_spooled.load(Ordering::Acquire),
            frames_spooled: self.status.frames_spooled.load(Ordering::Acquire),
            frames_lost: self.status.frames_lost.load(Ordering::Acquire),
            last_spool_unix_ms: self.status.last_spool_unix_ms.load(Ordering::Acquire),
        }
    }
}

#[derive(Debug, Default)]
struct PendingArchiveChunk {
    sequence: u64,
    sample_position: i64,
    transport_generation: u32,
    frame_count: u32,
    flags: u8,
    pcm: Vec<u8>,
}

struct ArchiveSpoolWriter {
    control: Arc<ArchiveControl>,
    config_version: u64,
    directory: Option<PathBuf>,
    quota_bytes: u64,
    enabled: bool,
    next_sequence: u64,
    pending_gap: bool,
    pending: PendingArchiveChunk,
    num_channels: u8,
    sample_rate: u32,
}

impl ArchiveSpoolWriter {
    fn new(control: Arc<ArchiveControl>, num_channels: u8, sample_rate: u32) -> Self {
        Self {
            control,
            config_version: u64::MAX,
            directory: None,
            quota_bytes: DEFAULT_ARCHIVE_QUOTA_BYTES,
            enabled: false,
            next_sequence: 1,
            pending_gap: false,
            pending: PendingArchiveChunk::default(),
            num_channels,
            sample_rate,
        }
    }

    fn sync_config(&mut self) {
        let version = self.control.version.load(Ordering::Acquire);
        if version == self.config_version {
            return;
        }

        let config = self
            .control
            .config
            .read()
            .map(|config| config.clone())
            .unwrap_or_default();
        let directory_changed = config.directory != self.directory;
        if directory_changed {
            self.finish_pending();
        }

        self.directory = config.directory;
        self.quota_bytes = config.quota_bytes.max(MIN_ARCHIVE_QUOTA_BYTES);
        self.enabled = config.enabled && self.directory.is_some();
        if directory_changed {
            self.next_sequence = self
                .directory
                .as_deref()
                .map(next_archive_sequence)
                .unwrap_or(1);
            self.pending = PendingArchiveChunk::default();
            self.pending_gap = false;
        }
        self.config_version = version;
    }

    fn push(&mut self, frame: &QueuedFrame) {
        self.sync_config();
        if !frame.archive_capture || self.directory.is_none() {
            return;
        }

        let bytes_per_frame = usize::from(self.num_channels) * 3;
        if frame.data.is_empty() || !frame.data.len().is_multiple_of(bytes_per_frame) {
            return;
        }

        let total_frames = frame.data.len() / bytes_per_frame;
        let expected_position = self
            .pending
            .sample_position
            .saturating_add(i64::from(self.pending.frame_count));
        if self.pending.frame_count > 0
            && (self.pending.transport_generation != frame.transport_generation
                || expected_position != frame.sample_position)
        {
            self.finish_pending();
            self.pending_gap = true;
        }

        let mut source_frame_offset = 0usize;
        while source_frame_offset < total_frames {
            if self.pending.frame_count == 0 {
                self.pending.sequence = self.next_sequence;
                self.pending.sample_position = frame
                    .sample_position
                    .saturating_add(source_frame_offset.min(i64::MAX as usize) as i64);
                self.pending.transport_generation = frame.transport_generation;
                self.pending.flags = if self.pending_gap {
                    ARCHIVE_FLAG_DISCONTINUITY
                } else {
                    0
                };
                self.pending_gap = false;
            }

            let target_frames = self.sample_rate as usize;
            let pending_frames = self.pending.frame_count as usize;
            let copied_frames =
                (target_frames - pending_frames).min(total_frames - source_frame_offset);
            let byte_start = source_frame_offset * bytes_per_frame;
            let byte_end = byte_start + copied_frames * bytes_per_frame;
            self.pending
                .pcm
                .extend_from_slice(&frame.data[byte_start..byte_end]);
            self.pending.frame_count = self
                .pending
                .frame_count
                .saturating_add(copied_frames as u32);
            source_frame_offset += copied_frames;

            if self.pending.frame_count as usize == target_frames {
                self.finish_pending();
            }
        }
    }

    fn finish_capture_if_stopped(&mut self) {
        self.sync_config();
        if self.enabled {
            return;
        }
        self.finish_pending();
        self.control
            .status
            .flush_complete
            .store(true, Ordering::Release);
    }

    fn finish_pending(&mut self) {
        if self.pending.frame_count == 0 {
            return;
        }

        let frame_count = u64::from(self.pending.frame_count);
        let result = self.write_pending();
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.pending = PendingArchiveChunk::default();
        match result {
            Ok(file_bytes) => {
                self.control
                    .status
                    .spool_full
                    .store(false, Ordering::Release);
                self.control
                    .status
                    .chunks_spooled
                    .fetch_add(1, Ordering::Relaxed);
                self.control
                    .status
                    .bytes_spooled
                    .fetch_add(file_bytes, Ordering::Relaxed);
                self.control
                    .status
                    .frames_spooled
                    .fetch_add(frame_count, Ordering::Relaxed);
                self.control
                    .status
                    .last_spool_unix_ms
                    .store(now_unix_ms(), Ordering::Release);
            }
            Err(error) => {
                if error.kind() == std::io::ErrorKind::StorageFull {
                    self.control
                        .status
                        .spool_full
                        .store(true, Ordering::Release);
                }
                self.control
                    .status
                    .frames_lost
                    .fetch_add(frame_count, Ordering::Relaxed);
                self.pending_gap = true;
            }
        }
    }

    fn write_pending(&self) -> Result<u64, std::io::Error> {
        let directory = self.directory.as_deref().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "archive directory missing")
        })?;
        fs::create_dir_all(directory)?;

        let file_bytes =
            u64::from(ARCHIVE_CHUNK_HEADER_BYTES).saturating_add(self.pending.pcm.len() as u64);
        let existing_bytes = archive_directory_bytes(directory)?;
        if existing_bytes.saturating_add(file_bytes) > self.quota_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::StorageFull,
                "archive spool quota reached",
            ));
        }

        let stem = format!("{:020}", self.pending.sequence);
        let partial_path = directory.join(format!("{stem}.partial"));
        let final_path = directory.join(format!("{stem}.iarc"));
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&partial_path)?;
        file.write_all(ARCHIVE_CHUNK_MAGIC)?;
        file.write_all(&ARCHIVE_CHUNK_VERSION.to_le_bytes())?;
        file.write_all(&ARCHIVE_CHUNK_HEADER_BYTES.to_le_bytes())?;
        file.write_all(&self.pending.sequence.to_le_bytes())?;
        file.write_all(&self.pending.sample_position.to_le_bytes())?;
        file.write_all(&self.pending.transport_generation.to_le_bytes())?;
        file.write_all(&self.sample_rate.to_le_bytes())?;
        file.write_all(&self.pending.frame_count.to_le_bytes())?;
        file.write_all(&(self.pending.pcm.len() as u32).to_le_bytes())?;
        file.write_all(&u16::from(self.num_channels).to_le_bytes())?;
        file.write_all(&[BITS_PER_SAMPLE, self.pending.flags])?;
        file.write_all(&now_unix_ms().to_le_bytes())?;
        file.write_all(&self.pending.pcm)?;
        file.sync_all()?;
        fs::rename(&partial_path, &final_path)?;
        File::open(directory)?.sync_all()?;
        Ok(file_bytes)
    }
}

impl Drop for ArchiveSpoolWriter {
    fn drop(&mut self) {
        self.finish_pending();
    }
}

fn next_archive_sequence(directory: &Path) -> u64 {
    fs::read_dir(directory)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            (path.extension().and_then(|value| value.to_str()) == Some("iarc"))
                .then(|| path.file_stem()?.to_str()?.parse::<u64>().ok())
                .flatten()
        })
        .max()
        .unwrap_or(0)
        .saturating_add(1)
}

fn archive_directory_bytes(directory: &Path) -> Result<u64, std::io::Error> {
    let mut total = 0u64;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        if matches!(
            path.extension().and_then(|value| value.to_str()),
            Some("iarc" | "partial")
        ) {
            total = total.saturating_add(entry.metadata()?.len());
        }
    }
    Ok(total)
}

#[derive(Debug, Default)]
struct AudioProcessorStatusCounters {
    started: AtomicBool,
    connected: AtomicBool,
    frames_queued: AtomicU64,
    frames_sent: AtomicU64,
    frames_dropped: AtomicU64,
    connection_attempts: AtomicU64,
    connection_failures: AtomicU64,
    last_connected_unix_ms: AtomicU64,
    last_send_unix_ms: AtomicU64,
}

impl AudioProcessorStatusCounters {
    fn snapshot(&self) -> AudioProcessorStatus {
        AudioProcessorStatus {
            started: self.started.load(Ordering::Acquire),
            connected: self.connected.load(Ordering::Acquire),
            frames_queued: self.frames_queued.load(Ordering::Acquire),
            frames_sent: self.frames_sent.load(Ordering::Acquire),
            frames_dropped: self.frames_dropped.load(Ordering::Acquire),
            connection_attempts: self.connection_attempts.load(Ordering::Acquire),
            connection_failures: self.connection_failures.load(Ordering::Acquire),
            last_connected_unix_ms: self.last_connected_unix_ms.load(Ordering::Acquire),
            last_send_unix_ms: self.last_send_unix_ms.load(Ordering::Acquire),
        }
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }

    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn metadata_field_from_c(value: *const c_char) -> Option<String> {
    if value.is_null() {
        return None;
    }

    let value = unsafe { CStr::from_ptr(value) }.to_string_lossy();
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    Some(truncate_utf8(trimmed, MAX_METADATA_FIELD_BYTES).to_owned())
}

struct AudioProcessorRenderState {
    data_producer: Producer<QueuedFrame>,
    free_consumer: Consumer<Vec<u8>>,
    rejected_buffer: Option<Vec<u8>>,
}

struct AudioProcessorRenderLease<'a> {
    state: &'a mut AudioProcessorRenderState,
    active: &'a AtomicBool,
}

impl Drop for AudioProcessorRenderLease<'_> {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
    }
}

pub struct AudioProcessor {
    render_state: UnsafeCell<AudioProcessorRenderState>,
    render_active: AtomicBool,
    max_frame_bytes: usize,
    shutdown: Arc<AtomicBool>,
    status: Arc<AudioProcessorStatusCounters>,
    metadata: Arc<RwLock<TrackMetadata>>,
    metadata_version: Arc<AtomicU64>,
    archive: Arc<ArchiveControl>,
    tx_thread: Option<JoinHandle<()>>,
    tx_waker: thread::Thread,
    num_channels: u8,
}

// Producer-side ring state is accessed only while `render_active` is held.
// Status, metadata, and shutdown touch disjoint synchronized fields. Final
// destruction is additionally excluded by the stable-handle reader lease.
unsafe impl Sync for AudioProcessor {}

impl AudioProcessor {
    pub fn new(socket_path: String, num_channels: u8, sample_rate: u32) -> Self {
        Self::new_configured(
            socket_path,
            num_channels,
            sample_rate,
            DEFAULT_MAX_FRAMES,
            None,
        )
    }

    pub fn new_preallocated(
        socket_path: String,
        num_channels: u8,
        sample_rate: u32,
        max_frames: usize,
    ) -> Self {
        Self::new_configured(socket_path, num_channels, sample_rate, max_frames, None)
    }

    pub fn new_with_frame_id(
        socket_path: String,
        num_channels: u8,
        sample_rate: u32,
        max_frames: usize,
        frame_id: u16,
    ) -> Self {
        Self::new_configured(
            socket_path,
            num_channels,
            sample_rate,
            max_frames,
            Some(frame_id),
        )
    }

    fn new_configured(
        socket_path: String,
        num_channels: u8,
        sample_rate: u32,
        max_frames: usize,
        frame_id: Option<u16>,
    ) -> Self {
        assert!((1..=16).contains(&num_channels), "invalid channel count");
        assert!(
            matches!(sample_rate, 16_000 | 44_100 | 48_000 | 96_000),
            "unsupported frame-header sample rate"
        );

        let max_frames = max_frames.clamp(1, DEFAULT_MAX_FRAMES);
        let max_frame_bytes = max_frames * usize::from(num_channels) * 3;
        let (data_producer, data_consumer) = RingBuffer::<QueuedFrame>::new(RING_SIZE);
        let (mut free_producer, free_consumer) = RingBuffer::<Vec<u8>>::new(RING_SIZE);
        for _ in 0..RING_SIZE {
            free_producer
                .push(Vec::with_capacity(max_frame_bytes))
                .expect("fresh free-buffer ring has capacity");
        }

        let shutdown = Arc::new(AtomicBool::new(false));
        let status = Arc::new(AudioProcessorStatusCounters::default());
        status.started.store(true, Ordering::Release);
        let metadata = Arc::new(RwLock::new(TrackMetadata::default()));
        let metadata_version = Arc::new(AtomicU64::new(0));
        let archive = Arc::new(ArchiveControl::default());

        let worker_shutdown = Arc::clone(&shutdown);
        let worker_status = Arc::clone(&status);
        let worker_metadata = Arc::clone(&metadata);
        let worker_metadata_version = Arc::clone(&metadata_version);
        let worker_archive = Arc::clone(&archive);
        let worker_socket_path = socket_path.clone();
        let tx_thread = thread::Builder::new()
            .name("infidelity-au-tx".to_owned())
            .spawn(move || {
                Self::run_tx(
                    worker_socket_path,
                    num_channels,
                    sample_rate,
                    max_frame_bytes,
                    frame_id,
                    worker_shutdown,
                    worker_status,
                    worker_metadata,
                    worker_metadata_version,
                    worker_archive,
                    data_consumer,
                    free_producer,
                );
            })
            .expect("failed to start Infidelity TX worker");
        let tx_waker = tx_thread.thread().clone();

        Self {
            render_state: UnsafeCell::new(AudioProcessorRenderState {
                data_producer,
                free_consumer,
                rejected_buffer: None,
            }),
            render_active: AtomicBool::new(false),
            max_frame_bytes,
            shutdown,
            status,
            metadata,
            metadata_version,
            archive,
            tx_thread: Some(tx_thread),
            tx_waker,
            num_channels,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_connection(
        mut stream: UnixStream,
        socket_path: &str,
        num_channels: u8,
        sample_rate: u32,
        max_frame_bytes: usize,
        shutdown: Arc<AtomicBool>,
        consumer: &mut Consumer<QueuedFrame>,
        free_producer: &mut Producer<Vec<u8>>,
        frame_id: Option<u16>,
        status: Arc<AudioProcessorStatusCounters>,
        metadata: Arc<RwLock<TrackMetadata>>,
        metadata_version: Arc<AtomicU64>,
        archive_writer: &mut ArchiveSpoolWriter,
    ) -> Result<(), std::io::Error> {
        stream.write_all(b"AUD2")?;

        let mut id_buf = [0u8; 2];
        stream.read_exact(&mut id_buf)?;

        let id = if let Some(frame_id) = frame_id {
            frame_id
        } else {
            u16::from_le_bytes(id_buf)
        } as u64;

        let stream_id = id as u16;
        let mut sent_metadata_version = metadata_version.load(Ordering::Acquire);
        let mut last_metadata_attempt = Instant::now();
        if let Ok(snapshot) = metadata.read().map(|metadata| metadata.clone()) {
            Self::send_metadata_control(socket_path, stream_id, &snapshot).ok();
        }

        status.connected.store(true, Ordering::Release);
        status
            .last_connected_unix_ms
            .store(now_unix_ms(), Ordering::Release);

        let mut header_data = Vec::with_capacity(MAX_HEADER_BYTES);
        let mut send_buffer =
            Vec::with_capacity(max_frame_bytes + MAX_HEADER_BYTES + SAMPLE_CLOCK_BYTES + 4);

        // Reconnection is a live-edge operation. Never replay the AU backlog
        // accumulated while Nexus was unavailable; retain only the newest frame.
        if let Some(frame) = Self::take_live_edge(consumer, free_producer, &status, archive_writer)
        {
            Self::send_frame(
                &mut stream,
                frame,
                id,
                num_channels,
                sample_rate,
                &mut header_data,
                &mut send_buffer,
                free_producer,
                &status,
            )?;
        }

        loop {
            if shutdown.load(Ordering::Acquire) {
                return Ok(());
            }

            let current_metadata_version = metadata_version.load(Ordering::Acquire);
            if current_metadata_version != sent_metadata_version
                || last_metadata_attempt.elapsed() >= METADATA_HEARTBEAT_INTERVAL
            {
                last_metadata_attempt = Instant::now();
                if let Ok(snapshot) = metadata.read().map(|metadata| metadata.clone()) {
                    if Self::send_metadata_control(socket_path, stream_id, &snapshot).is_ok() {
                        sent_metadata_version = current_metadata_version;
                    }
                } else {
                    sent_metadata_version = current_metadata_version;
                }
            }

            let sent_audio = if let Some(frame) =
                Self::take_live_edge(consumer, free_producer, &status, archive_writer)
            {
                Self::send_frame(
                    &mut stream,
                    frame,
                    id,
                    num_channels,
                    sample_rate,
                    &mut header_data,
                    &mut send_buffer,
                    free_producer,
                    &status,
                )?;
                true
            } else {
                false
            };

            if shutdown.load(Ordering::Acquire) {
                return Ok(());
            }

            if !sent_audio {
                let until_heartbeat =
                    METADATA_HEARTBEAT_INTERVAL.saturating_sub(last_metadata_attempt.elapsed());
                thread::park_timeout(until_heartbeat.max(Duration::from_millis(1)));
            }
        }
    }

    /// Consume an accumulated producer burst at its live edge.
    ///
    /// The render callback cannot evict from an SPSC queue. The socket worker
    /// therefore returns every superseded buffer to the preallocated free pool
    /// before it performs a potentially blocking write and sends only the
    /// newest quantum. Under normal load this pops exactly one frame; after a
    /// local scheduling or socket stall it prevents stale audio from being
    /// replayed merely because it was queued first.
    fn take_live_edge(
        consumer: &mut Consumer<QueuedFrame>,
        free_producer: &mut Producer<Vec<u8>>,
        status: &AudioProcessorStatusCounters,
        archive_writer: &mut ArchiveSpoolWriter,
    ) -> Option<QueuedFrame> {
        let Some(mut newest) = consumer.pop().ok() else {
            archive_writer.finish_capture_if_stopped();
            return None;
        };
        archive_writer.push(&newest);
        while let Ok(frame) = consumer.pop() {
            archive_writer.push(&frame);
            let stale = std::mem::replace(&mut newest, frame);
            let _ = free_producer.push(stale.data);
            status.frames_dropped.fetch_add(1, Ordering::Relaxed);
        }
        archive_writer.finish_capture_if_stopped();
        Some(newest)
    }

    #[allow(clippy::too_many_arguments)]
    fn send_frame(
        stream: &mut UnixStream,
        frame: QueuedFrame,
        id: u64,
        num_channels: u8,
        sample_rate: u32,
        header_data: &mut Vec<u8>,
        send_buffer: &mut Vec<u8>,
        free_producer: &mut Producer<Vec<u8>>,
        status: &AudioProcessorStatusCounters,
    ) -> Result<(), std::io::Error> {
        let bytes_per_frame = usize::from(num_channels) * 3;
        let samples_per_channel = frame.data.len() / bytes_per_frame;
        if frame.data.is_empty()
            || !frame.data.len().is_multiple_of(bytes_per_frame)
            || samples_per_channel > DEFAULT_MAX_FRAMES
        {
            status.frames_dropped.fetch_add(1, Ordering::Relaxed);
            let _ = free_producer.push(frame.data);
            return Ok(());
        }

        let header = FrameHeader::new(
            EncodingFlag::PCMSigned,
            samples_per_channel as u16,
            sample_rate,
            num_channels,
            BITS_PER_SAMPLE,
            Endianness::LittleEndian,
            Some(id),
            None,
        )
        .map_err(|message| std::io::Error::new(std::io::ErrorKind::InvalidData, message))?;

        header_data.clear();
        header.encode(&mut *header_data)?;
        let total_size = 4 + header_data.len() + frame.data.len();
        send_buffer.clear();
        let total_size = total_size + SAMPLE_CLOCK_BYTES;
        send_buffer.extend_from_slice(&(total_size as u32).to_le_bytes());
        send_buffer.extend_from_slice(&frame.sample_position.to_le_bytes());
        send_buffer.extend_from_slice(&frame.transport_generation.to_le_bytes());
        send_buffer.extend_from_slice(header_data);
        send_buffer.extend_from_slice(&frame.data);
        let result = stream.write_all(send_buffer);
        let _ = free_producer.push(frame.data);
        result?;

        status.frames_sent.fetch_add(1, Ordering::Relaxed);
        status
            .last_send_unix_ms
            .store(now_unix_ms(), Ordering::Release);
        Ok(())
    }

    fn send_metadata_control(
        socket_path: &str,
        stream_id: u16,
        metadata: &TrackMetadata,
    ) -> Result<(), std::io::Error> {
        if metadata.is_empty() {
            return Ok(());
        }

        let instance_id = metadata.instance_id.as_deref().unwrap_or("");
        let label = metadata.label.as_deref().unwrap_or("");
        let mut stream = UnixStream::connect(socket_path)?;
        stream.write_all(b"META")?;
        stream.write_all(&stream_id.to_le_bytes())?;
        stream.write_all(&(instance_id.len() as u16).to_le_bytes())?;
        stream.write_all(&(label.len() as u16).to_le_bytes())?;
        stream.write_all(instance_id.as_bytes())?;
        stream.write_all(label.as_bytes())?;
        Ok(())
    }

    pub fn set_track_metadata(&self, instance_id: Option<String>, label: Option<String>) {
        let next = TrackMetadata { instance_id, label };
        let Ok(mut metadata) = self.metadata.write() else {
            return;
        };

        if *metadata == next {
            return;
        }

        *metadata = next;
        self.metadata_version.fetch_add(1, Ordering::Release);
        self.tx_waker.unpark();
    }

    pub fn status(&self) -> AudioProcessorStatus {
        self.status.snapshot()
    }

    fn configure_archive(&self, config: ArchiveConfig) {
        self.archive.configure(config);
        self.tx_waker.unpark();
    }

    pub fn set_archive_enabled(&self, enabled: bool) {
        self.archive.set_enabled(enabled);
        if !enabled {
            while self.render_active.load(Ordering::Acquire) {
                thread::yield_now();
            }
        }
        self.tx_waker.unpark();
    }

    pub fn archive_status(&self) -> AudioProcessorArchiveStatus {
        self.archive.snapshot()
    }

    pub fn add(&self, data: &[u8], sample_position: i64, transport_generation: u32) {
        let bytes_per_frame = usize::from(self.num_channels) * 3;
        if data.is_empty()
            || data.len() > self.max_frame_bytes
            || !data.len().is_multiple_of(bytes_per_frame)
        {
            self.status.frames_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }

        let Some(render) = self.try_render_state() else {
            self.status.frames_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let Some(mut buf) = Self::take_free_buffer(render.state) else {
            self.status.frames_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        buf.clear();
        buf.extend_from_slice(data);
        self.enqueue_buffer(render.state, buf, sample_position, transport_generation);
    }

    pub fn add_f32_mono(&self, samples: &[f32], sample_position: i64, transport_generation: u32) {
        let frame_size = samples.len().saturating_mul(3);
        if self.num_channels != 1
            || samples.is_empty()
            || frame_size > self.max_frame_bytes
            || samples.len() > DEFAULT_MAX_FRAMES
        {
            self.status.frames_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }

        let Some(render) = self.try_render_state() else {
            self.status.frames_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let Some(mut buf) = Self::take_free_buffer(render.state) else {
            self.status.frames_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        buf.clear();
        for sample in samples {
            let value = if sample.is_finite() { *sample } else { 0.0 };
            let value = (value.clamp(-1.0, 1.0) * 8_388_607.0).round() as i32;
            buf.push((value & 0xff) as u8);
            buf.push(((value >> 8) & 0xff) as u8);
            buf.push(((value >> 16) & 0xff) as u8);
        }

        self.enqueue_buffer(render.state, buf, sample_position, transport_generation);
    }

    fn try_render_state(&self) -> Option<AudioProcessorRenderLease<'_>> {
        if self
            .render_active
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return None;
        }

        Some(AudioProcessorRenderLease {
            // `render_active` is the sole admission path, so no other thread
            // can form a mutable reference to producer-side ring state.
            state: unsafe { &mut *self.render_state.get() },
            active: &self.render_active,
        })
    }

    fn take_free_buffer(render: &mut AudioProcessorRenderState) -> Option<Vec<u8>> {
        render
            .rejected_buffer
            .take()
            .or_else(|| render.free_consumer.pop().ok())
    }

    fn enqueue_buffer(
        &self,
        render: &mut AudioProcessorRenderState,
        buf: Vec<u8>,
        sample_position: i64,
        transport_generation: u32,
    ) {
        match render.data_producer.push(QueuedFrame {
            sample_position,
            transport_generation,
            archive_capture: self.archive.capture_enabled.load(Ordering::Acquire),
            data: buf,
        }) {
            Ok(()) => {
                self.status.frames_queued.fetch_add(1, Ordering::Relaxed);
                self.tx_waker.unpark();
            }
            Err(PushError::Full(frame)) => {
                // Keep ownership of the preallocated buffer on the producer side.
                // It will be retried with the next (fresher) callback, without an
                // allocation or deallocation on the render thread.
                render.rejected_buffer = Some(frame.data);
                self.status.frames_dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn establish_connection(socket_path: &str) -> Option<UnixStream> {
        match UnixStream::connect(socket_path) {
            Ok(stream) => {
                stream.set_read_timeout(Some(RECONNECT_INTERVAL)).ok()?;
                stream.set_write_timeout(Some(RECONNECT_INTERVAL)).ok()?;
                Some(stream)
            }
            Err(_) => None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn run_tx(
        socket_path: String,
        num_channels: u8,
        sample_rate: u32,
        max_frame_bytes: usize,
        frame_id: Option<u16>,
        shutdown: Arc<AtomicBool>,
        status: Arc<AudioProcessorStatusCounters>,
        metadata: Arc<RwLock<TrackMetadata>>,
        metadata_version: Arc<AtomicU64>,
        archive: Arc<ArchiveControl>,
        mut data_consumer: Consumer<QueuedFrame>,
        mut free_producer: Producer<Vec<u8>>,
    ) {
        let mut archive_writer = ArchiveSpoolWriter::new(archive, num_channels, sample_rate);
        while !shutdown.load(Ordering::Acquire) {
            archive_writer.sync_config();
            status.connection_attempts.fetch_add(1, Ordering::Relaxed);
            match Self::establish_connection(&socket_path) {
                Some(stream) => {
                    if Self::handle_connection(
                        stream,
                        &socket_path,
                        num_channels,
                        sample_rate,
                        max_frame_bytes,
                        Arc::clone(&shutdown),
                        &mut data_consumer,
                        &mut free_producer,
                        frame_id,
                        Arc::clone(&status),
                        Arc::clone(&metadata),
                        Arc::clone(&metadata_version),
                        &mut archive_writer,
                    )
                    .is_err()
                    {
                        status.connection_failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
                None => {
                    status.connection_failures.fetch_add(1, Ordering::Relaxed);
                    while let Ok(frame) = data_consumer.pop() {
                        archive_writer.push(&frame);
                        let _ = free_producer.push(frame.data);
                    }
                    archive_writer.finish_capture_if_stopped();
                }
            }
            status.connected.store(false, Ordering::Release);
            if !shutdown.load(Ordering::Acquire) {
                thread::park_timeout(RECONNECT_INTERVAL);
            }
        }
        status.connected.store(false, Ordering::Release);
        status.started.store(false, Ordering::Release);
        archive_writer.finish_pending();
    }

    fn request_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.status.connected.store(false, Ordering::Release);
        self.tx_waker.unpark();
    }

    pub fn shutdown(&mut self) {
        self.request_shutdown();
        if let Some(handle) = self.tx_thread.take() {
            let _ = handle.join();
        }
        self.status.started.store(false, Ordering::Release);
    }
}

impl Drop for AudioProcessor {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct AudioProcessorLease<'a> {
    processor: *mut AudioProcessor,
    reader_count: &'a AtomicUsize,
}

impl AudioProcessorLease<'_> {
    fn processor(&self) -> Option<&AudioProcessor> {
        if self.processor.is_null() {
            None
        } else {
            Some(unsafe { &*self.processor })
        }
    }
}

impl Drop for AudioProcessorLease<'_> {
    fn drop(&mut self) {
        // Read-side release is atomic only. It can never become the final
        // owner or run the worker join/destructor on the render thread.
        self.reader_count.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Stable AU-lifetime handle around replaceable processor instances.
///
/// Render/status/metadata calls enter one of two atomic reader epochs.
/// Lifecycle operations publish a replacement, move new callers to the other
/// epoch, and reclaim the old processor only after its readers finish. The
/// lifecycle thread is therefore the sole path that can join the socket worker
/// or finally destroy its queues and buffers.
pub struct AudioProcessorHandle {
    current: AtomicPtr<AudioProcessor>,
    reader_epoch: AtomicUsize,
    reader_counts: [AtomicUsize; 2],
    lifecycle: Mutex<()>,
    desired_metadata: Mutex<TrackMetadata>,
    desired_archive: Mutex<ArchiveConfig>,
}

impl Default for AudioProcessorHandle {
    fn default() -> Self {
        Self {
            current: AtomicPtr::new(ptr::null_mut()),
            reader_epoch: AtomicUsize::new(0),
            reader_counts: [AtomicUsize::new(0), AtomicUsize::new(0)],
            lifecycle: Mutex::new(()),
            desired_metadata: Mutex::new(TrackMetadata::default()),
            desired_archive: Mutex::new(ArchiveConfig::default()),
        }
    }
}

impl AudioProcessorHandle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn initialize(
        &self,
        socket_path: String,
        channels: u8,
        sample_rate: u32,
        max_frames: usize,
        frame_id: Option<u16>,
    ) {
        let processor = Box::new(AudioProcessor::new_configured(
            socket_path,
            channels,
            sample_rate,
            max_frames,
            frame_id,
        ));

        // Seed the worker before publication. A racing metadata setter is
        // reconciled again below after this processor becomes current.
        let initial_metadata = self
            .desired_metadata
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        processor.set_track_metadata(
            initial_metadata.instance_id.clone(),
            initial_metadata.label.clone(),
        );
        let initial_archive = self
            .desired_archive
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        processor.configure_archive(initial_archive);
        let processor = Box::into_raw(processor);

        let _lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.publish_and_reclaim(processor);

        // Holding desired_metadata prevents a delayed setter from applying an
        // older value after this final synchronization. A later setter sees
        // the published processor and updates it directly.
        let desired = self
            .desired_metadata
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        unsafe { &*processor }
            .set_track_metadata(desired.instance_id.clone(), desired.label.clone());
        let desired_archive = self
            .desired_archive
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        unsafe { &*processor }.configure_archive(desired_archive);
    }

    pub fn deinitialize(&self) {
        let _lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.publish_and_reclaim(ptr::null_mut());
    }

    pub fn add(&self, data: &[u8], sample_position: i64, transport_generation: u32) {
        let lease = self.acquire();
        if let Some(processor) = lease.processor() {
            processor.add(data, sample_position, transport_generation);
        }
    }

    pub fn status(&self) -> AudioProcessorStatus {
        let lease = self.acquire();
        lease
            .processor()
            .map(AudioProcessor::status)
            .unwrap_or_default()
    }

    pub fn set_track_metadata(&self, instance_id: Option<String>, label: Option<String>) {
        let mut desired = self
            .desired_metadata
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        desired.instance_id = instance_id;
        desired.label = label;

        let lease = self.acquire();
        if let Some(processor) = lease.processor() {
            processor.set_track_metadata(desired.instance_id.clone(), desired.label.clone());
        }
    }

    pub fn configure_archive(&self, directory: PathBuf, quota_bytes: u64) {
        let mut desired = self
            .desired_archive
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let enabled = desired.enabled;
        *desired = ArchiveConfig::normalized(directory, quota_bytes);
        desired.enabled = enabled;

        let lease = self.acquire();
        if let Some(processor) = lease.processor() {
            processor.configure_archive(desired.clone());
        }
    }

    pub fn set_archive_enabled(&self, enabled: bool) {
        let mut desired = self
            .desired_archive
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        desired.enabled = enabled;

        let lease = self.acquire();
        if let Some(processor) = lease.processor() {
            processor.set_archive_enabled(enabled);
        }
    }

    pub fn archive_status(&self) -> AudioProcessorArchiveStatus {
        let lease = self.acquire();
        lease
            .processor()
            .map(AudioProcessor::archive_status)
            .unwrap_or_else(|| {
                let enabled = self
                    .desired_archive
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .enabled;
                AudioProcessorArchiveStatus {
                    enabled,
                    flush_complete: !enabled,
                    ..AudioProcessorArchiveStatus::default()
                }
            })
    }

    pub fn copy_desired_metadata_from(&self, source: &Self) {
        if ptr::eq(self, source) {
            return;
        }
        let desired = source
            .desired_metadata
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        self.set_track_metadata(desired.instance_id, desired.label);
    }

    fn acquire(&self) -> AudioProcessorLease<'_> {
        loop {
            let epoch = self.reader_epoch.load(Ordering::SeqCst) & 1;
            let reader_count = &self.reader_counts[epoch];
            reader_count.fetch_add(1, Ordering::SeqCst);
            let processor = self.current.load(Ordering::SeqCst);
            if self.reader_epoch.load(Ordering::SeqCst) & 1 == epoch {
                return AudioProcessorLease {
                    processor,
                    reader_count,
                };
            }
            reader_count.fetch_sub(1, Ordering::SeqCst);
            std::hint::spin_loop();
        }
    }

    /// Must be called with `lifecycle` held.
    fn publish_and_reclaim(&self, replacement: *mut AudioProcessor) {
        let old_epoch = self.reader_epoch.load(Ordering::SeqCst) & 1;
        let next_epoch = old_epoch ^ 1;
        debug_assert_eq!(self.reader_counts[next_epoch].load(Ordering::SeqCst), 0);

        let retired = self.current.swap(replacement, Ordering::SeqCst);
        self.reader_epoch.store(next_epoch, Ordering::SeqCst);

        if !retired.is_null() {
            unsafe { &*retired }.request_shutdown();
        }

        let mut spins = 0usize;
        while self.reader_counts[old_epoch].load(Ordering::SeqCst) != 0 {
            if spins < 64 {
                std::hint::spin_loop();
                spins += 1;
            } else {
                thread::yield_now();
            }
        }

        if !retired.is_null() {
            unsafe { drop(Box::from_raw(retired)) };
        }
    }
}

impl Drop for AudioProcessorHandle {
    fn drop(&mut self) {
        self.deinitialize();
    }
}

fn valid_processor_config(channels: u8, sample_rate: u32, max_frames: usize) -> bool {
    (1..=16).contains(&channels)
        && matches!(sample_rate, 16_000 | 44_100 | 48_000 | 96_000)
        && max_frames > 0
}

fn socket_path_from_c(socket_path: *const c_char) -> Option<String> {
    if socket_path.is_null() {
        return None;
    }
    Some(
        unsafe { CStr::from_ptr(socket_path) }
            .to_string_lossy()
            .into_owned(),
    )
}

fn path_from_c(path: *const c_char) -> Option<PathBuf> {
    let value = socket_path_from_c(path)?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(PathBuf::from(trimmed))
    }
}

fn new_initialized_processor_handle(
    socket_path: *const c_char,
    channels: u8,
    sample_rate: u32,
    max_frames: usize,
    frame_id: Option<u16>,
) -> *mut c_void {
    let Some(path) = socket_path_from_c(socket_path) else {
        return ptr::null_mut();
    };
    if !valid_processor_config(channels, sample_rate, max_frames) {
        return ptr::null_mut();
    }

    let handle = Box::new(AudioProcessorHandle::new());
    handle.initialize(path, channels, sample_rate, max_frames, frame_id);
    Box::into_raw(handle) as *mut c_void
}

/// Creates an inactive stable processor handle. It remains valid across any
/// number of initialize/deinitialize cycles.
#[no_mangle]
pub extern "C" fn audio_processor_handle_new() -> *mut c_void {
    Box::into_raw(Box::new(AudioProcessorHandle::new())) as *mut c_void
}

/// Creates and publishes a fully prewarmed processor behind a stable handle.
/// Allocation and worker-thread creation complete before this function returns.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn audio_processor_handle_initialize_preallocated(
    handle: *mut c_void,
    socket_path: *const c_char,
    channels: u8,
    sample_rate: u32,
    max_frames: usize,
) -> bool {
    if handle.is_null() || !valid_processor_config(channels, sample_rate, max_frames) {
        return false;
    }
    let Some(path) = socket_path_from_c(socket_path) else {
        return false;
    };

    unsafe { &*(handle as *mut AudioProcessorHandle) }.initialize(
        path,
        channels,
        sample_rate,
        max_frames,
        None,
    );
    true
}

/// Copies persistent desired metadata between stable handles. The destination
/// may be inactive and will apply the copied values on its next initialize.
#[no_mangle]
pub extern "C" fn audio_processor_handle_copy_track_metadata(
    destination: *mut c_void,
    source: *mut c_void,
) {
    if destination.is_null() || source.is_null() || destination == source {
        return;
    }
    let destination = unsafe { &*(destination as *mut AudioProcessorHandle) };
    let source = unsafe { &*(source as *mut AudioProcessorHandle) };
    destination.copy_desired_metadata_from(source);
}

/// Legacy constructors now return the same stable opaque handle used by the AU.
#[no_mangle]
pub extern "C" fn audio_processor_new(
    socket_path: *const c_char,
    channels: u8,
    sample_rate: u32,
) -> *mut c_void {
    new_initialized_processor_handle(socket_path, channels, sample_rate, DEFAULT_MAX_FRAMES, None)
}

#[no_mangle]
pub extern "C" fn audio_processor_new_with_id(
    socket_path: *const c_char,
    channels: u8,
    sample_rate: u32,
    frame_id: u16,
) -> *mut c_void {
    new_initialized_processor_handle(
        socket_path,
        channels,
        sample_rate,
        DEFAULT_MAX_FRAMES,
        Some(frame_id),
    )
}

#[no_mangle]
pub extern "C" fn audio_processor_new_preallocated(
    socket_path: *const c_char,
    channels: u8,
    sample_rate: u32,
    max_frames: usize,
) -> *mut c_void {
    new_initialized_processor_handle(socket_path, channels, sample_rate, max_frames, None)
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn audio_processor_add(
    handle: *mut c_void,
    data_ptr: *const u8,
    length: usize,
    sample_position: i64,
    transport_generation: u32,
) {
    if handle.is_null() || data_ptr.is_null() || length == 0 {
        return;
    }
    let data = unsafe { std::slice::from_raw_parts(data_ptr, length) };
    unsafe { &*(handle as *mut AudioProcessorHandle) }.add(
        data,
        sample_position,
        transport_generation,
    );
}

#[no_mangle]
pub extern "C" fn audio_processor_status(handle: *mut c_void) -> AudioProcessorStatus {
    if handle.is_null() {
        AudioProcessorStatus::default()
    } else {
        unsafe { &*(handle as *mut AudioProcessorHandle) }.status()
    }
}

#[no_mangle]
pub extern "C" fn audio_processor_set_track_metadata(
    handle: *mut c_void,
    instance_id: *const c_char,
    label: *const c_char,
) {
    if !handle.is_null() {
        unsafe { &*(handle as *mut AudioProcessorHandle) }.set_track_metadata(
            metadata_field_from_c(instance_id),
            metadata_field_from_c(label),
        );
    }
}

#[no_mangle]
pub extern "C" fn audio_processor_archive_configure(
    handle: *mut c_void,
    spool_directory: *const c_char,
    quota_bytes: u64,
) -> bool {
    if handle.is_null() {
        return false;
    }
    let Some(directory) = path_from_c(spool_directory) else {
        return false;
    };
    unsafe { &*(handle as *mut AudioProcessorHandle) }.configure_archive(directory, quota_bytes);
    true
}

#[no_mangle]
pub extern "C" fn audio_processor_archive_set_enabled(handle: *mut c_void, enabled: bool) {
    if !handle.is_null() {
        unsafe { &*(handle as *mut AudioProcessorHandle) }.set_archive_enabled(enabled);
    }
}

#[no_mangle]
pub extern "C" fn audio_processor_archive_status(
    handle: *mut c_void,
) -> AudioProcessorArchiveStatus {
    if handle.is_null() {
        AudioProcessorArchiveStatus::default()
    } else {
        unsafe { &*(handle as *mut AudioProcessorHandle) }.archive_status()
    }
}

/// Deinitializes the current inner processor but leaves the stable handle and
/// its desired metadata valid for a later initialize.
#[no_mangle]
pub extern "C" fn audio_processor_shutdown(handle: *mut c_void) {
    if !handle.is_null() {
        unsafe { &*(handle as *mut AudioProcessorHandle) }.deinitialize();
    }
}

/// Final stable-handle destruction. Callers must first stop every callback that
/// could use the handle; inner processor destruction may race callbacks safely.
#[no_mangle]
pub extern "C" fn audio_processor_destroy(handle: *mut c_void) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle as *mut AudioProcessorHandle)) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::os::unix::net::UnixListener;
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;
    use std::time::Duration;

    static SOCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn unique_socket_path() -> String {
        let counter = SOCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
        format!(
            "/tmp/au-tx-lifecycle-{}-{}-{}.sock",
            std::process::id(),
            now_unix_ms(),
            counter
        )
    }

    fn unique_archive_path() -> PathBuf {
        let counter = SOCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "au-tx-archive-{}-{}-{}",
            std::process::id(),
            now_unix_ms(),
            counter
        ))
    }

    #[derive(Debug)]
    struct ReceivedFrame {
        header: FrameHeader,
        sample_position: i64,
        transport_generation: u32,
        audio_data: Vec<u8>,
    }

    struct MockAudioServer {
        listener: UnixListener,
        socket_path: String,
        received_frames: Arc<Mutex<Vec<ReceivedFrame>>>,
    }

    impl MockAudioServer {
        fn new(socket_path: &str) -> Self {
            let _ = std::fs::remove_file(socket_path);
            let listener = UnixListener::bind(socket_path).expect("Failed to bind socket");
            Self {
                listener,
                socket_path: socket_path.to_string(),
                received_frames: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn start(&self) -> Arc<Mutex<Vec<ReceivedFrame>>> {
            let listener = self.listener.try_clone().unwrap();
            let frames = Arc::clone(&self.received_frames);

            thread::spawn(move || {
                if let Ok((mut stream, _)) = listener.accept() {
                    println!("Server: Accepted connection");

                    let mut hello_buf = [0u8; 4];
                    if stream.read_exact(&mut hello_buf).is_ok() {
                        assert_eq!(&hello_buf, b"AUD2", "Expected AUD2 handshake");
                        println!("Server: Received AUD2");

                        stream.write_all(&1u16.to_le_bytes()).unwrap();
                        println!("Server: Sent frame ID");

                        loop {
                            let mut size_buf = [0u8; 4];
                            if stream.read_exact(&mut size_buf).is_err() {
                                break;
                            }
                            let total_size = u32::from_le_bytes(size_buf) as usize;

                            let mut frame_data = vec![0u8; total_size - 4];
                            if stream.read_exact(&mut frame_data).is_err() {
                                break;
                            }

                            if frame_data.len() < SAMPLE_CLOCK_BYTES {
                                break;
                            }
                            let sample_position =
                                i64::from_le_bytes(frame_data[..8].try_into().unwrap());
                            let transport_generation =
                                u32::from_le_bytes(frame_data[8..12].try_into().unwrap());
                            let encoded_frame = &frame_data[SAMPLE_CLOCK_BYTES..];
                            if let Ok(header) = FrameHeader::decode(&mut &encoded_frame[..]) {
                                let header_size = header.size();
                                let audio_data = encoded_frame[header_size..].to_vec();
                                frames.lock().unwrap().push(ReceivedFrame {
                                    header,
                                    sample_position,
                                    transport_generation,
                                    audio_data,
                                });
                            }
                        }
                    }
                }
            });

            Arc::clone(&self.received_frames)
        }
    }

    impl Drop for MockAudioServer {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.socket_path);
        }
    }

    fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if predicate() {
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }
        assert!(predicate(), "condition was not met before timeout");
    }

    #[test]
    fn test_audio_processor_lifecycle() {
        const SOCKET_PATH: &str = "/tmp/au-tx-test.sock";

        let server = MockAudioServer::new(SOCKET_PATH);
        let received_frames = server.start();

        thread::sleep(Duration::from_millis(100));

        let socket_path_c = std::ffi::CString::new(SOCKET_PATH).unwrap();
        let processor = audio_processor_new(socket_path_c.as_ptr(), 2, 48_000);
        assert!(!processor.is_null(), "AudioProcessor creation failed");

        let num_samples = 10;
        let mut test_data = vec![0u8; 3 * 2 * num_samples];

        for i in (0..test_data.len()).step_by(6) {
            test_data[i] = 0x12;
            test_data[i + 1] = 0x34;
            test_data[i + 2] = 0x56;
            test_data[i + 3] = 0x78;
            test_data[i + 4] = 0x9A;
            test_data[i + 5] = 0xBC;
        }

        audio_processor_add(processor, test_data.as_ptr(), test_data.len(), 48_000, 1);
        thread::sleep(Duration::from_millis(500));
        audio_processor_add(processor, test_data.as_ptr(), test_data.len(), 48_010, 1);
        thread::sleep(Duration::from_millis(500));
        audio_processor_add(processor, test_data.as_ptr(), test_data.len(), 48_020, 1);

        let frames = received_frames.lock().unwrap();
        assert!(!frames.is_empty(), "No frames received");

        let first_frame = &frames[0];
        assert_eq!(first_frame.header.channels(), 2, "Expected stereo");
        assert_eq!(
            first_frame.header.bits_per_sample(),
            24,
            "Expected 24-bit audio"
        );
        assert_eq!(
            first_frame.header.sample_rate(),
            48_000,
            "Expected 48kHz sample rate"
        );
        assert_eq!(first_frame.header.pts(), None);
        assert_eq!(first_frame.sample_position, 48_000);
        assert_eq!(first_frame.transport_generation, 1);
        assert_eq!(
            first_frame.audio_data.len(),
            3 * 2 * num_samples,
            "Incorrect audio data length. Expected {}, got {}",
            3 * 2 * num_samples,
            first_frame.audio_data.len()
        );

        for i in (0..first_frame.audio_data.len()).step_by(6) {
            assert_eq!(
                &first_frame.audio_data[i..i + 3],
                &[0x12, 0x34, 0x56],
                "Incorrect left channel data at offset {}",
                i
            );
            assert_eq!(
                &first_frame.audio_data[i + 3..i + 6],
                &[0x78, 0x9A, 0xBC],
                "Incorrect right channel data at offset {}",
                i
            );
        }

        audio_processor_shutdown(processor);
        audio_processor_destroy(processor);
    }

    #[test]
    fn f32_mobile_input_is_quantized_directly_into_reused_s24_buffers() {
        const SOCKET_PATH: &str = "/tmp/au-tx-f32-mobile-test.sock";
        let server = MockAudioServer::new(SOCKET_PATH);
        let received_frames = server.start();
        let mut processor = AudioProcessor::new(SOCKET_PATH.to_string(), 1, 48_000);

        processor.add_f32_mono(&[-1.0, 0.0, 1.0], -120, 7);
        thread::sleep(Duration::from_millis(250));

        let frames = received_frames.lock().unwrap();
        let frame = frames.first().expect("mobile PCM frame");
        assert_eq!(frame.header.sample_size(), 3);
        assert_eq!(frame.header.channels(), 1);
        assert_eq!(frame.header.pts(), None);
        assert_eq!(frame.sample_position, -120);
        assert_eq!(frame.transport_generation, 7);
        assert_eq!(
            frame.audio_data,
            vec![0x01, 0x00, 0x80, 0x00, 0x00, 0x00, 0xff, 0xff, 0x7f]
        );
        drop(frames);
        processor.shutdown();
    }

    #[test]
    fn variable_host_quanta_are_framed_with_their_actual_sample_counts() {
        const SOCKET_PATH: &str = "/tmp/au-tx-variable-quantum-test.sock";
        let server = MockAudioServer::new(SOCKET_PATH);
        let received_frames = server.start();
        let mut processor =
            AudioProcessor::new_preallocated(SOCKET_PATH.to_owned(), 2, 48_000, 1024);

        wait_until(Duration::from_secs(1), || processor.status().connected);
        for (index, frames) in [64usize, 512, 17].into_iter().enumerate() {
            let data = vec![index as u8 + 1; frames * 2 * 3];
            processor.add(&data, 10_000 + index as i64, 4);
            wait_until(Duration::from_secs(1), || {
                received_frames.lock().unwrap().len() == index + 1
            });
        }

        let frames = received_frames.lock().unwrap();
        assert_eq!(
            frames
                .iter()
                .map(|frame| usize::from(frame.header.sample_size()))
                .collect::<Vec<_>>(),
            vec![64, 512, 17]
        );
        assert_eq!(
            frames
                .iter()
                .map(|frame| frame.audio_data.len())
                .collect::<Vec<_>>(),
            vec![64 * 6, 512 * 6, 17 * 6]
        );
        assert_eq!(
            frames
                .iter()
                .map(|frame| frame.sample_position)
                .collect::<Vec<_>>(),
            vec![10_000, 10_001, 10_002]
        );
        drop(frames);
        processor.shutdown();
    }

    #[test]
    fn constructor_prewarms_and_pool_pressure_drops_without_growing_backlog() {
        let socket_path = unique_socket_path();
        let listener = UnixListener::bind(&socket_path).unwrap();
        let accepted = Arc::new(Barrier::new(2));
        let server_accepted = Arc::clone(&accepted);
        let server = thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            server_accepted.wait();
            thread::sleep(Duration::from_millis(100));
        });
        let mut processor = AudioProcessor::new_preallocated(socket_path.clone(), 2, 48_000, 64);
        assert!(processor.status().started);
        accepted.wait();

        let data = vec![0x7f; 64 * 2 * 3];
        for sample_position in 0..(RING_SIZE as i64 + 5) {
            processor.add(&data, sample_position, 1);
        }

        let status = processor.status();
        assert_eq!(status.frames_queued, RING_SIZE as u64);
        assert_eq!(status.frames_dropped, 5);
        processor.shutdown();
        assert!(!processor.status().started);
        server.join().unwrap();
        let _ = std::fs::remove_file(socket_path);
    }

    #[test]
    fn socket_worker_coalesces_an_accumulated_burst_to_the_live_edge() {
        let (mut producer, mut consumer) = RingBuffer::<QueuedFrame>::new(RING_SIZE);
        let (mut free_producer, mut free_consumer) = RingBuffer::<Vec<u8>>::new(RING_SIZE);
        let status = AudioProcessorStatusCounters::default();
        let mut archive_writer =
            ArchiveSpoolWriter::new(Arc::new(ArchiveControl::default()), 2, 48_000);

        for sample_position in [240, 480, 720] {
            producer
                .push(QueuedFrame {
                    sample_position,
                    transport_generation: 3,
                    archive_capture: false,
                    data: vec![(sample_position / 240) as u8; 12],
                })
                .expect("test queue has capacity");
        }

        let live = AudioProcessor::take_live_edge(
            &mut consumer,
            &mut free_producer,
            &status,
            &mut archive_writer,
        )
        .expect("newest frame");

        assert_eq!(live.sample_position, 720);
        assert_eq!(live.transport_generation, 3);
        assert_eq!(live.data, vec![3; 12]);
        assert_eq!(status.frames_dropped.load(Ordering::Relaxed), 2);
        assert_eq!(
            free_consumer.pop().expect("first recycled buffer"),
            vec![1; 12]
        );
        assert_eq!(
            free_consumer.pop().expect("second recycled buffer"),
            vec![2; 12]
        );
        assert!(consumer.pop().is_err());
    }

    #[test]
    fn archive_spool_batches_exact_sample_clock_chunks_atomically() {
        let directory = unique_archive_path();
        let control = Arc::new(ArchiveControl::default());
        control.configure(ArchiveConfig {
            directory: Some(directory.clone()),
            quota_bytes: MIN_ARCHIVE_QUOTA_BYTES,
            enabled: true,
        });
        let mut writer = ArchiveSpoolWriter::new(Arc::clone(&control), 2, 8);

        writer.push(&QueuedFrame {
            sample_position: -3,
            transport_generation: 7,
            archive_capture: true,
            data: vec![0x11; 3 * 2 * 3],
        });
        writer.push(&QueuedFrame {
            sample_position: 0,
            transport_generation: 7,
            archive_capture: true,
            data: vec![0x22; 5 * 2 * 3],
        });

        let path = directory.join("00000000000000000001.iarc");
        let bytes = fs::read(&path).expect("committed archive chunk");
        assert_eq!(&bytes[0..4], ARCHIVE_CHUNK_MAGIC);
        assert_eq!(u16::from_le_bytes(bytes[4..6].try_into().unwrap()), 1);
        assert_eq!(u16::from_le_bytes(bytes[6..8].try_into().unwrap()), 52);
        assert_eq!(u64::from_le_bytes(bytes[8..16].try_into().unwrap()), 1);
        assert_eq!(i64::from_le_bytes(bytes[16..24].try_into().unwrap()), -3);
        assert_eq!(u32::from_le_bytes(bytes[24..28].try_into().unwrap()), 7);
        assert_eq!(u32::from_le_bytes(bytes[28..32].try_into().unwrap()), 8);
        assert_eq!(u32::from_le_bytes(bytes[32..36].try_into().unwrap()), 8);
        assert_eq!(u32::from_le_bytes(bytes[36..40].try_into().unwrap()), 48);
        assert_eq!(u16::from_le_bytes(bytes[40..42].try_into().unwrap()), 2);
        assert_eq!(bytes[42], 24);
        assert_eq!(bytes[43], 0);
        assert_eq!(&bytes[52..70], vec![0x11; 18]);
        assert_eq!(&bytes[70..], vec![0x22; 30]);
        assert!(!directory.join("00000000000000000001.partial").exists());
        let status = control.snapshot();
        assert_eq!(status.chunks_spooled, 1);
        assert_eq!(status.frames_spooled, 8);
        assert!(!status.spool_full);

        fs::remove_dir_all(directory).ok();
    }

    #[test]
    fn archive_spool_marks_discontinuity_and_recovers_sequence() {
        let directory = unique_archive_path();
        fs::create_dir_all(&directory).unwrap();
        fs::write(directory.join("00000000000000000007.iarc"), b"prior").unwrap();
        assert_eq!(next_archive_sequence(&directory), 8);

        let control = Arc::new(ArchiveControl::default());
        control.configure(ArchiveConfig {
            directory: Some(directory.clone()),
            quota_bytes: MIN_ARCHIVE_QUOTA_BYTES,
            enabled: true,
        });
        let mut writer = ArchiveSpoolWriter::new(Arc::clone(&control), 1, 8);
        writer.push(&QueuedFrame {
            sample_position: 0,
            transport_generation: 1,
            archive_capture: true,
            data: vec![0x10; 4 * 3],
        });
        writer.push(&QueuedFrame {
            sample_position: 100,
            transport_generation: 2,
            archive_capture: true,
            data: vec![0x20; 8 * 3],
        });
        writer.finish_pending();

        let first = fs::read(directory.join("00000000000000000008.iarc")).unwrap();
        let second = fs::read(directory.join("00000000000000000009.iarc")).unwrap();
        assert_eq!(i64::from_le_bytes(first[16..24].try_into().unwrap()), 0);
        assert_eq!(first[43], 0);
        assert_eq!(i64::from_le_bytes(second[16..24].try_into().unwrap()), 100);
        assert_eq!(u32::from_le_bytes(second[24..28].try_into().unwrap()), 2);
        assert_eq!(second[43], ARCHIVE_FLAG_DISCONTINUITY);
        assert_eq!(control.snapshot().chunks_spooled, 2);

        fs::remove_dir_all(directory).ok();
    }

    #[test]
    fn archive_stop_flushes_only_render_quanta_inside_the_capture_boundary() {
        let directory = unique_archive_path();
        let socket_path = unique_socket_path();
        let mut processor = AudioProcessor::new_preallocated(socket_path, 1, 48_000, 8);
        processor.configure_archive(ArchiveConfig {
            directory: Some(directory.clone()),
            quota_bytes: MIN_ARCHIVE_QUOTA_BYTES,
            enabled: false,
        });

        processor.add(&[0x10; 2 * 3], -2, 1);
        processor.set_archive_enabled(true);
        let captured = vec![0x20; 4 * 3];
        processor.add(&captured, 0, 1);
        processor.set_archive_enabled(false);
        processor.add(&[0x30; 2 * 3], 4, 1);

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !processor.archive_status().flush_complete && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        let status = processor.archive_status();
        assert!(status.flush_complete);
        assert!(!status.enabled);
        assert_eq!(status.frames_spooled, 4);

        let chunk = fs::read(directory.join("00000000000000000001.iarc"))
            .expect("final partial archive chunk");
        assert_eq!(i64::from_le_bytes(chunk[16..24].try_into().unwrap()), 0);
        assert_eq!(u32::from_le_bytes(chunk[32..36].try_into().unwrap()), 4);
        assert_eq!(&chunk[52..], captured);
        assert_eq!(
            fs::read_dir(&directory)
                .unwrap()
                .filter_map(Result::ok)
                .filter(
                    |entry| entry.path().extension().and_then(|value| value.to_str())
                        == Some("iarc")
                )
                .count(),
            1
        );

        processor.shutdown();
        fs::remove_dir_all(directory).ok();
    }

    #[test]
    fn stable_handle_waits_for_a_render_lease_before_reclaiming() {
        let handle = Arc::new(AudioProcessorHandle::new());
        handle.initialize(unique_socket_path(), 2, 48_000, 128, None);
        let render_lease = handle.acquire();
        assert!(render_lease.processor().is_some());

        let teardown_finished = Arc::new(AtomicBool::new(false));
        let teardown_handle = Arc::clone(&handle);
        let teardown_finished_thread = Arc::clone(&teardown_finished);
        let teardown = thread::spawn(move || {
            teardown_handle.deinitialize();
            teardown_finished_thread.store(true, Ordering::Release);
        });

        for _ in 0..10_000 {
            if handle.current.load(Ordering::SeqCst).is_null() {
                break;
            }
            thread::yield_now();
        }
        assert!(handle.current.load(Ordering::SeqCst).is_null());
        assert!(!teardown_finished.load(Ordering::Acquire));
        drop(render_lease);
        teardown.join().unwrap();
        assert!(teardown_finished.load(Ordering::Acquire));
        assert!(!handle.status().started);
    }

    #[test]
    fn stable_handle_survives_render_status_metadata_and_reinitialize_races() {
        let handle = Arc::new(AudioProcessorHandle::new());
        handle.initialize(unique_socket_path(), 2, 48_000, 128, None);

        let start = Arc::new(Barrier::new(4));
        let stop = Arc::new(AtomicBool::new(false));
        let render_calls = Arc::new(AtomicU64::new(0));
        let status_calls = Arc::new(AtomicU64::new(0));
        let metadata_calls = Arc::new(AtomicU64::new(0));

        let render = {
            let handle = Arc::clone(&handle);
            let start = Arc::clone(&start);
            let stop = Arc::clone(&stop);
            let render_calls = Arc::clone(&render_calls);
            thread::spawn(move || {
                let data = vec![0x5a; 128 * 2 * 3];
                handle.add(&data, 0, 1);
                render_calls.fetch_add(1, Ordering::Relaxed);
                start.wait();
                let mut sample_position = 1i64;
                while !stop.load(Ordering::Acquire) {
                    handle.add(&data, sample_position, 1);
                    render_calls.fetch_add(1, Ordering::Relaxed);
                    sample_position += 1;
                    thread::yield_now();
                }
            })
        };
        let poll_status = {
            let handle = Arc::clone(&handle);
            let start = Arc::clone(&start);
            let stop = Arc::clone(&stop);
            let status_calls = Arc::clone(&status_calls);
            thread::spawn(move || {
                let _ = handle.status();
                status_calls.fetch_add(1, Ordering::Relaxed);
                start.wait();
                while !stop.load(Ordering::Acquire) {
                    let _ = handle.status();
                    status_calls.fetch_add(1, Ordering::Relaxed);
                    thread::yield_now();
                }
            })
        };
        let update_metadata = {
            let handle = Arc::clone(&handle);
            let start = Arc::clone(&start);
            let stop = Arc::clone(&stop);
            let metadata_calls = Arc::clone(&metadata_calls);
            thread::spawn(move || {
                let mut iteration = 0u64;
                handle
                    .set_track_metadata(Some("instance-0".to_owned()), Some("track-0".to_owned()));
                metadata_calls.fetch_add(1, Ordering::Relaxed);
                start.wait();
                while !stop.load(Ordering::Acquire) {
                    handle.set_track_metadata(
                        Some(format!("instance-{}", iteration % 8)),
                        Some(format!("track-{}", iteration % 16)),
                    );
                    metadata_calls.fetch_add(1, Ordering::Relaxed);
                    iteration += 1;
                    thread::yield_now();
                }
            })
        };

        start.wait();
        for iteration in 0..12 {
            handle.deinitialize();
            assert!(!handle.status().started);
            handle.initialize(
                unique_socket_path(),
                if iteration % 2 == 0 { 1 } else { 2 },
                [44_100, 48_000, 96_000][iteration % 3],
                128,
                None,
            );
        }

        stop.store(true, Ordering::Release);
        render.join().unwrap();
        poll_status.join().unwrap();
        update_metadata.join().unwrap();

        let desired = handle
            .desired_metadata
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let lease = handle.acquire();
        let processor = lease.processor().expect("final initialized processor");
        let applied = processor
            .metadata
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(applied, desired);
        drop(lease);

        handle.deinitialize();
        assert!(render_calls.load(Ordering::Acquire) > 0);
        assert!(status_calls.load(Ordering::Acquire) > 0);
        assert!(metadata_calls.load(Ordering::Acquire) > 0);
        assert_eq!(handle.current.load(Ordering::SeqCst), ptr::null_mut());
        assert_eq!(handle.reader_counts[0].load(Ordering::SeqCst), 0);
        assert_eq!(handle.reader_counts[1].load(Ordering::SeqCst), 0);
    }
}
