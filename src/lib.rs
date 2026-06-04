use frame_header::{EncodingFlag, Endianness, FrameHeader};
use rtrb::{Consumer, Producer, RingBuffer};
use std::ffi::{c_char, c_void, CStr};
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const BITS_PER_SAMPLE: u8 = 24;
const RECONNECT_INTERVAL: Duration = Duration::from_millis(50);
const METADATA_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);
const RING_SIZE: usize = 256;
const MAX_METADATA_FIELD_BYTES: usize = 512;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct TrackMetadata {
    instance_id: Option<String>,
    label: Option<String>,
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

pub struct AudioProcessor {
    socket_path: String,
    data_producer: Producer<(u64, Vec<u8>)>,
    data_consumer: Option<Consumer<(u64, Vec<u8>)>>,
    free_consumer: Consumer<Vec<u8>>,
    free_producer: Option<Producer<Vec<u8>>>,
    samples_per_channel: Option<usize>,
    shutdown: Arc<AtomicBool>,
    started: Arc<AtomicBool>,
    status: Arc<AudioProcessorStatusCounters>,
    data_ready: Arc<(Mutex<bool>, Condvar)>,
    metadata: Arc<RwLock<TrackMetadata>>,
    metadata_version: Arc<AtomicU64>,
    tx_thread: Option<JoinHandle<()>>,
    num_channels: u8,
    sample_rate: u32,
    frame_id: Option<u16>,
}

impl AudioProcessor {
    pub fn new(socket_path: String, num_channels: u8, sample_rate: u32) -> Self {
        let (data_producer, data_consumer) = RingBuffer::<(u64, Vec<u8>)>::new(RING_SIZE);
        let (free_producer, free_consumer) = RingBuffer::<Vec<u8>>::new(RING_SIZE);
        Self {
            socket_path,
            data_producer,
            data_consumer: Some(data_consumer),
            free_consumer,
            free_producer: Some(free_producer),
            samples_per_channel: None,
            num_channels,
            shutdown: Arc::new(AtomicBool::new(false)),
            started: Arc::new(AtomicBool::new(false)),
            status: Arc::new(AudioProcessorStatusCounters::default()),
            data_ready: Arc::new((Mutex::new(false), Condvar::new())),
            metadata: Arc::new(RwLock::new(TrackMetadata::default())),
            metadata_version: Arc::new(AtomicU64::new(0)),
            tx_thread: None,
            sample_rate,
            frame_id: None,
        }
    }

    pub fn with_frame_id(mut self, frame_id: u16) -> Self {
        self.frame_id = Some(frame_id);
        self
    }

    fn handle_connection(
        mut stream: UnixStream,
        socket_path: &str,
        samples_per_channel: usize,
        num_channels: u8,
        sample_rate: u32,
        shutdown: Arc<AtomicBool>,
        consumer: &mut Consumer<(u64, Vec<u8>)>,
        free_producer: &mut Producer<Vec<u8>>,
        data_ready: Arc<(Mutex<bool>, Condvar)>,
        frame_id: Option<u16>,
        status: Arc<AudioProcessorStatusCounters>,
        metadata: Arc<RwLock<TrackMetadata>>,
        metadata_version: Arc<AtomicU64>,
    ) -> Result<(), std::io::Error> {
        stream.write_all(b"HELO")?;

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

        let header = FrameHeader::new(
            EncodingFlag::PCMSigned,
            samples_per_channel as u16,
            sample_rate,
            num_channels,
            BITS_PER_SAMPLE,
            Endianness::LittleEndian,
            Some(id),
            Some(123),
        )
        .unwrap();

        let mut header_data = Vec::with_capacity(header.size());
        header.encode(&mut header_data).ok();

        let frame_size = samples_per_channel * num_channels as usize * 3; // 3 bytes per sample
        let total_size = frame_size + header_data.len() + 4;
        let mut send_buffer = Vec::with_capacity(total_size);

        loop {
            if shutdown.load(Ordering::Acquire) {
                return Ok(());
            }

            // Wait for data or shutdown
            {
                let (lock, cvar) = &*data_ready;
                let mut ready = lock.lock().unwrap();
                while !*ready && !shutdown.load(Ordering::Acquire) {
                    let (guard, _) = cvar.wait_timeout(ready, RECONNECT_INTERVAL).unwrap();
                    ready = guard;
                }
                *ready = false;
            }

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

            'inner: for _ in 0..consumer.slots() {
                match consumer.pop() {
                    Ok((ts, buf)) => {
                        FrameHeader::patch_pts(&mut header_data, Some(ts)).unwrap();
                        send_buffer.clear();
                        send_buffer.extend_from_slice(&(total_size as u32).to_le_bytes());
                        send_buffer.extend_from_slice(&header_data);
                        send_buffer.extend_from_slice(&buf);
                        let result = stream.write_all(&send_buffer);
                        // Return buffer to free pool before propagating any error
                        free_producer.push(buf).ok();
                        result?;
                        status.frames_sent.fetch_add(1, Ordering::Relaxed);
                        status
                            .last_send_unix_ms
                            .store(now_unix_ms(), Ordering::Release);
                    }
                    Err(_) => {
                        break 'inner;
                    }
                }
            }

            if shutdown.load(Ordering::Acquire) {
                return Ok(());
            }
        }
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
    }

    pub fn add(&mut self, data: &[u8], ts: u64) {
        if !self.started.load(Ordering::Acquire) {
            let frame_size = data.len();
            let spc = frame_size / (self.num_channels as usize * 3);
            self.samples_per_channel = Some(spc);
            self.start_tx(frame_size);
        }

        // Pop a pre-allocated buffer from the free pool; fall back to a fresh allocation
        // only if the pool is exhausted (tx thread is lagging).
        let mut buf = match self.free_consumer.pop() {
            Ok(mut b) => {
                b.clear();
                b
            }
            Err(_) => Vec::with_capacity(data.len()),
        };
        buf.extend_from_slice(data);

        if self.data_producer.push((ts, buf)).is_err() {
            self.status.frames_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.status.frames_queued.fetch_add(1, Ordering::Relaxed);

        // Notify tx thread (skip syscall if already signaled)
        let (lock, cvar) = &*self.data_ready;
        let mut ready = lock.lock().unwrap();
        if !*ready {
            *ready = true;
            cvar.notify_one();
        }
    }

    fn establish_connection(socket_path: &str) -> Option<UnixStream> {
        match UnixStream::connect(socket_path) {
            Ok(stream) => Some(stream),
            Err(_) => None,
        }
    }

    fn start_tx(&mut self, frame_size: usize) {
        let shutdown_flag = Arc::clone(&self.shutdown);
        let data_ready = Arc::clone(&self.data_ready);
        let status = Arc::clone(&self.status);
        let metadata = Arc::clone(&self.metadata);
        let metadata_version = Arc::clone(&self.metadata_version);
        let socket_path = self.socket_path.clone();

        let mut data_consumer = self
            .data_consumer
            .take()
            .expect("Consumer was already taken or never existed");

        // Pre-fill the free pool with correctly sized buffers before the tx thread starts,
        // so the audio thread never needs to allocate in the steady state.
        let mut free_producer = self
            .free_producer
            .take()
            .expect("free_producer was already taken");
        for _ in 0..RING_SIZE {
            free_producer.push(vec![0u8; frame_size]).ok();
        }

        let samples_per_channel = frame_size / (self.num_channels as usize * 3);
        let num_channels = self.num_channels;
        let sample_rate = self.sample_rate;
        let frame_id = self.frame_id;

        let handle = thread::spawn(move || {
            while !shutdown_flag.load(Ordering::Acquire) {
                status.connection_attempts.fetch_add(1, Ordering::Relaxed);
                match Self::establish_connection(&socket_path) {
                    Some(stream) => {
                        if Self::handle_connection(
                            stream,
                            &socket_path,
                            samples_per_channel,
                            num_channels,
                            sample_rate,
                            Arc::clone(&shutdown_flag),
                            &mut data_consumer,
                            &mut free_producer,
                            Arc::clone(&data_ready),
                            frame_id,
                            Arc::clone(&status),
                            Arc::clone(&metadata),
                            Arc::clone(&metadata_version),
                        )
                        .is_err()
                        {
                            status.connected.store(false, Ordering::Release);
                            status.connection_failures.fetch_add(1, Ordering::Relaxed);
                            thread::sleep(RECONNECT_INTERVAL);
                        } else {
                            status.connected.store(false, Ordering::Release);
                        }
                    }
                    None => {
                        status.connected.store(false, Ordering::Release);
                        status.connection_failures.fetch_add(1, Ordering::Relaxed);
                        thread::sleep(RECONNECT_INTERVAL);
                    }
                }
            }
            status.connected.store(false, Ordering::Release);
        });

        self.tx_thread = Some(handle);
        self.started.store(true, Ordering::Release);
        self.status.started.store(true, Ordering::Release);
    }

    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        self.status.connected.store(false, Ordering::Release);
        // Notify in case Tx thread is waiting
        let (_, cvar) = &*self.data_ready;
        cvar.notify_all();
        self.tx_thread.take();
    }
}

impl Drop for AudioProcessor {
    fn drop(&mut self) {
        if !self.shutdown.load(Ordering::Acquire) {
            self.shutdown();
        }
    }
}

/// Creates an AudioProcessor using a Unix domain socket path.
/// `socket_path` must be a valid UTF-8 C string (null-terminated).
#[no_mangle]
pub extern "C" fn audio_processor_new(
    socket_path: *const std::ffi::c_char,
    channels: u8,
    sample_rate: u32,
) -> *mut c_void {
    let path = unsafe { std::ffi::CStr::from_ptr(socket_path) }
        .to_string_lossy()
        .into_owned();
    let processor = AudioProcessor::new(path, channels, sample_rate);
    Box::into_raw(Box::new(processor)) as *mut c_void
}

#[no_mangle]
pub extern "C" fn audio_processor_new_with_id(
    socket_path: *const std::ffi::c_char,
    channels: u8,
    sample_rate: u32,
    frame_id: u16,
) -> *mut c_void {
    let path = unsafe { std::ffi::CStr::from_ptr(socket_path) }
        .to_string_lossy()
        .into_owned();
    let processor = AudioProcessor::new(path, channels, sample_rate).with_frame_id(frame_id);
    Box::into_raw(Box::new(processor)) as *mut c_void
}

#[no_mangle]
pub extern "C" fn audio_processor_add(
    instance: *mut c_void,
    data_ptr: *const u8,
    length: usize,
    ts: u64,
) {
    unsafe {
        let processor = &mut *(instance as *mut AudioProcessor);
        let data = std::slice::from_raw_parts(data_ptr, length);
        processor.add(data, ts);
    }
}

#[no_mangle]
pub extern "C" fn audio_processor_status(instance: *mut c_void) -> AudioProcessorStatus {
    if instance.is_null() {
        return AudioProcessorStatus::default();
    }

    unsafe {
        let processor = &*(instance as *mut AudioProcessor);
        processor.status.snapshot()
    }
}

#[no_mangle]
pub extern "C" fn audio_processor_set_track_metadata(
    instance: *mut c_void,
    instance_id: *const c_char,
    label: *const c_char,
) {
    if instance.is_null() {
        return;
    }

    unsafe {
        let processor = &*(instance as *mut AudioProcessor);
        processor.set_track_metadata(
            metadata_field_from_c(instance_id),
            metadata_field_from_c(label),
        );
    }
}

#[no_mangle]
pub extern "C" fn audio_processor_shutdown(instance: *mut c_void) {
    unsafe {
        let processor = &mut *(instance as *mut AudioProcessor);
        processor.shutdown();
    }
}

#[no_mangle]
pub extern "C" fn audio_processor_destroy(instance: *mut c_void) {
    if !instance.is_null() {
        unsafe {
            drop(Box::from_raw(instance as *mut AudioProcessor));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::os::unix::net::UnixListener;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[derive(Debug)]
    struct ReceivedFrame {
        header: FrameHeader,
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
                    if let Ok(_) = stream.read_exact(&mut hello_buf) {
                        assert_eq!(&hello_buf, b"HELO", "Expected HELO handshake");
                        println!("Server: Received HELO");

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

                            if let Ok(header) = FrameHeader::decode(&mut &frame_data[..]) {
                                let header_size = header.size();
                                let audio_data = frame_data[header_size..].to_vec();
                                frames
                                    .lock()
                                    .unwrap()
                                    .push(ReceivedFrame { header, audio_data });
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

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        audio_processor_add(processor, test_data.as_ptr(), test_data.len(), now);
        thread::sleep(Duration::from_millis(500));
        audio_processor_add(processor, test_data.as_ptr(), test_data.len(), now);
        thread::sleep(Duration::from_millis(500));
        audio_processor_add(processor, test_data.as_ptr(), test_data.len(), now);

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
}
