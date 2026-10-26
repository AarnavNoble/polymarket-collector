//! One websocket leg: connects, keeps its symbol set subscribed, timestamps
//! and decodes frames, and reconnects on any failure. A leg never blocks on
//! the engine; if the engine falls behind, the leg drops its connection and
//! resyncs rather than silently losing frames.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures_util::{Sink, SinkExt, StreamExt};
use rustc_hash::FxHashSet;
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior, interval, sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_util::sync::CancellationToken;

use crate::engine::{EngineMsg, LegId};
use crate::exchange::Exchange;

/// Commands from the subscription manager to a leg.
#[derive(Debug, Clone)]
pub enum Command {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
    /// Drop and re-establish the connection (stall recovery or recycling).
    Reconnect,
}

pub struct LegConfig {
    pub stall_timeout: Duration,
    pub heartbeat: Duration,
    /// Recycle the connection after roughly this long, to clear any state the
    /// exchange may have accumulated. Jittered so legs never recycle together.
    pub max_lifetime: Duration,
    /// Shared by every leg to space out connection attempts.
    pub pacer: Pacer,
}

/// Spaces connection attempts process-wide. Exchanges throttle bursts of
/// websocket upgrades from one IP (measured: see LATENCY.md), and at startup
/// or after a mass disconnect every leg would otherwise connect at once.
pub struct Pacer {
    next: std::sync::Mutex<Instant>,
    interval: Duration,
}

impl Pacer {
    pub fn new(interval: Duration) -> Self {
        Self {
            next: std::sync::Mutex::new(Instant::now()),
            interval,
        }
    }

    /// Waits for this caller's slot.
    pub async fn wait(&self) {
        let at = {
            let mut next = self.next.lock().unwrap();
            let at = (*next).max(Instant::now());
            *next = at + self.interval;
            at
        };
        tokio::time::sleep_until(at).await;
    }
}

/// How long a leg waits for room in the engine's queue before giving up its
/// connection.
const ENGINE_GRACE: Duration = Duration::from_millis(500);

/// How long one websocket write may take. A peer that stopped reading fills
/// the TCP send buffer, and an unbounded write would block stall detection
/// and shutdown until the OS gave up.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Payload bytes received across every leg, for the dashboard: a saturated
/// downlink shows up as delivery latency (see FINDINGS.md).
pub static BYTES_RECEIVED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

enum Exit {
    Cancelled,
    Reconnect(&'static str),
}

pub async fn run<E: Exchange>(
    id: LegId,
    exchange: Arc<E>,
    cfg: Arc<LegConfig>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    engine: mpsc::Sender<EngineMsg>,
    cancel: CancellationToken,
) {
    let mut symbols = FxHashSet::default();
    let mut backoff = Duration::from_millis(250);
    loop {
        // Wait until there is something to subscribe to.
        while symbols.is_empty() {
            tokio::select! {
                _ = cancel.cancelled() => return,
                cmd = commands.recv() => match cmd {
                    Some(cmd) => apply(&mut symbols, cmd),
                    None => return,
                },
            }
        }

        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = cfg.pacer.wait() => {}
        }
        // Commands queued while disconnected are folded into the symbol set
        // the new session subscribes, rather than replayed against it (a
        // stale `Reconnect` would kill the fresh connection).
        while let Ok(cmd) = commands.try_recv() {
            apply(&mut symbols, cmd);
        }
        if symbols.is_empty() {
            continue;
        }
        let started = Instant::now();
        match session(
            id,
            &*exchange,
            &cfg,
            &mut symbols,
            &mut commands,
            &engine,
            &cancel,
        )
        .await
        {
            Ok(Exit::Cancelled) => return,
            Ok(Exit::Reconnect(reason)) => tracing::info!(?id, reason, "reconnecting"),
            Err(e) => tracing::warn!(?id, error = %e, "connection failed"),
        }
        let _ = engine.send(EngineMsg::Disconnected(id)).await;

        // A session that lived a while was healthy; start backoff over.
        if started.elapsed() > Duration::from_secs(60) {
            backoff = Duration::from_millis(250);
        }
        let jittered = backoff.mul_f64(0.5 + fastrand::f64());
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = sleep(jittered) => {}
        }
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

fn apply(symbols: &mut FxHashSet<String>, cmd: Command) {
    match cmd {
        Command::Subscribe(s) => symbols.extend(s),
        Command::Unsubscribe(s) => {
            for s in &s {
                symbols.remove(s);
            }
        }
        Command::Reconnect => {}
    }
}

async fn send<S>(tx: &mut S, text: impl Into<String>) -> anyhow::Result<()>
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    timeout(WRITE_TIMEOUT, tx.send(Message::text(text.into())))
        .await
        .map_err(|_| anyhow::anyhow!("write timed out"))??;
    Ok(())
}

async fn session<E: Exchange>(
    id: LegId,
    exchange: &E,
    cfg: &LegConfig,
    symbols: &mut FxHashSet<String>,
    commands: &mut mpsc::UnboundedReceiver<Command>,
    engine: &mpsc::Sender<EngineMsg>,
    cancel: &CancellationToken,
) -> anyhow::Result<Exit> {
    // Initial book dumps arrive as one large frame; disable Nagle so small
    // control frames are not delayed.
    let config = WebSocketConfig::default()
        .max_message_size(Some(256 << 20))
        .max_frame_size(Some(256 << 20));
    let connect =
        tokio_tungstenite::connect_async_with_config(exchange.ws_url(), Some(config), true);
    let (ws, _) = tokio::select! {
        _ = cancel.cancelled() => return Ok(Exit::Cancelled),
        r = timeout(Duration::from_secs(10), connect) => r.map_err(|_| anyhow::anyhow!("connect timed out"))??,
    };
    let (mut tx, mut rx) = ws.split();

    let _ = engine.send(EngineMsg::Connected(id)).await;
    let initial: Vec<String> = symbols.iter().cloned().collect();
    let subscribe = async {
        for (i, batch) in initial.chunks(exchange.max_batch()).enumerate() {
            send(&mut tx, exchange.subscribe(batch, i == 0)).await?;
        }
        anyhow::Ok(())
    };
    tokio::select! {
        _ = cancel.cancelled() => return Ok(Exit::Cancelled),
        r = subscribe => r?,
    }
    tracing::debug!(?id, n = initial.len(), "subscribed");

    let lifetime = cfg.max_lifetime.mul_f64(0.75 + fastrand::f64() / 2.0);
    let recycle_at = sleep(lifetime);
    tokio::pin!(recycle_at);
    let mut heartbeat = interval(cfg.heartbeat);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Any frame, keepalive replies included, proves the TCP path is alive.
    // A socket that answers pings but stopped delivering data is caught by
    // the engine comparing it with its sibling legs.
    let mut last_frame = Instant::now();
    let mut events = Vec::new();

    let exit = loop {
        tokio::select! {
            frame = rx.next() => {
                // Timestamp before anything else touches the frame.
                let recv_ts = Utc::now();
                let Some(frame) = frame else { break Exit::Reconnect("stream ended") };
                last_frame = Instant::now();
                match frame? {
                    Message::Text(text) => {
                        BYTES_RECEIVED.fetch_add(text.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        if let Err(e) = exchange.decode(&text, &mut events) {
                            let head: String = text.chars().take(200).collect();
                            tracing::warn!(?id, error = %e, frame = %head, "undecodable frame");
                        }
                        if !events.is_empty() {
                            let msg = EngineMsg::Events { leg: id, recv_ts, events: std::mem::take(&mut events) };
                            // A brief stall (e.g. the minute snapshot) is
                            // absorbed; a stuck engine costs this connection,
                            // which then resyncs, rather than frames lost
                            // silently.
                            if !matches!(timeout(ENGINE_GRACE, engine.send(msg)).await, Ok(Ok(()))) {
                                break Exit::Reconnect("engine backlog full");
                            }
                        }
                    }
                    Message::Close(frame) => {
                        tracing::info!(?id, ?frame, "server closed connection");
                        break Exit::Reconnect("server close");
                    }
                    // Pings are answered by tungstenite on the next write/flush.
                    _ => {}
                }
            }
            cmd = commands.recv() => {
                let Some(cmd) = cmd else { break Exit::Cancelled };
                let frames: Vec<String> = match &cmd {
                    Command::Subscribe(s) => s.chunks(exchange.max_batch()).map(|b| exchange.subscribe(b, false)).collect(),
                    Command::Unsubscribe(s) => s.chunks(exchange.max_batch()).map(|b| exchange.unsubscribe(b)).collect(),
                    Command::Reconnect => break Exit::Reconnect("requested"),
                };
                apply(symbols, cmd);
                for frame in frames {
                    send(&mut tx, frame).await?;
                }
            }
            _ = heartbeat.tick() => {
                if last_frame.elapsed() > cfg.stall_timeout {
                    break Exit::Reconnect("stalled");
                }
                match exchange.heartbeat() {
                    Some(text) => send(&mut tx, text).await?,
                    None => timeout(WRITE_TIMEOUT, tx.send(Message::Ping(Default::default())))
                        .await
                        .map_err(|_| anyhow::anyhow!("write timed out"))??,
                }
            }
            _ = &mut recycle_at => break Exit::Reconnect("recycle"),
            _ = cancel.cancelled() => break Exit::Cancelled,
        }
    };

    // Best-effort close handshake; the peer may already be gone.
    let _ = timeout(Duration::from_secs(2), tx.send(Message::Close(None))).await;
    Ok(exit)
}
