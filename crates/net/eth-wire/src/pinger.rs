use crate::errors::PingerError;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::time::{Instant, Interval, Sleep};
use tokio_stream::Stream;

/// The pinger is a simple state machine that sends a ping, waits for a pong,
/// and transitions to timeout if the pong is not received within the timeout.
#[derive(Debug)]
pub(crate) struct Pinger {
    /// The timer used for the next ping.
    ping_interval: Interval,
    /// The timer used to detect a ping timeout.
    timeout_timer: Pin<Box<Sleep>>,
    /// The timeout duration for each ping.
    timeout: Duration,
    /// Keeps track of the state
    state: PingState,
    /// Time when the ping was sent (for RTT calculation)
    ping_sent_at: Option<Instant>,
    /// Last measured RTT in microseconds (set once on first pong, never updated)
    last_rtt_us: Option<u64>,
}

// === impl Pinger ===

impl Pinger {
    /// Creates a new [`Pinger`] with the given ping interval duration,
    /// and timeout duration.
    pub(crate) fn new(ping_interval: Duration, timeout_duration: Duration) -> Self {
        let now = Instant::now();
        let timeout_timer = tokio::time::sleep(timeout_duration);
        Self {
            state: PingState::Ready,
            ping_interval: tokio::time::interval_at(now + ping_interval, ping_interval),
            timeout_timer: Box::pin(timeout_timer),
            timeout: timeout_duration,
            ping_sent_at: None,
            last_rtt_us: None,
        }
    }

    /// Creates a new [`Pinger`] that will send the first ping immediately for RTT measurement.
    /// Subsequent pings will use the regular interval.
    pub(crate) fn new_with_immediate_ping(
        ping_interval: Duration,
        timeout_duration: Duration,
    ) -> Self {
        let now = Instant::now();
        let timeout_timer = tokio::time::sleep(timeout_duration);
        // Set interval to fire immediately for the first ping
        let mut interval = tokio::time::interval_at(now, ping_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Self {
            state: PingState::Ready,
            ping_interval: interval,
            timeout_timer: Box::pin(timeout_timer),
            timeout: timeout_duration,
            ping_sent_at: None,
            last_rtt_us: None,
        }
    }

    /// Mark a pong as received, and transition the pinger to the `Ready` state if it was in the
    /// `WaitingForPong` state. Resets readiness by resetting the ping interval.
    /// Calculates RTT on first pong only.
    ///
    /// Returns `Ok(Some(rtt_ms))` if this was the first RTT measurement.
    /// Returns `Ok(None)` if RTT was already measured or couldn't be calculated.
    pub(crate) fn on_pong(&mut self) -> Result<Option<u64>, PingerError> {
        match self.state {
            PingState::Ready => Err(PingerError::UnexpectedPong),
            PingState::WaitingForPong => {
                let mut first_rtt_ms = None;
                // Calculate RTT only on first pong (set once, never updated)
                if self.last_rtt_us.is_none() {
                    if let Some(sent_at) = self.ping_sent_at {
                        let rtt_us = sent_at.elapsed().as_micros() as u64;
                        self.last_rtt_us = Some(rtt_us);
                        first_rtt_ms = Some(rtt_us / 1000);
                    }
                }
                self.ping_sent_at = None;
                self.state = PingState::Ready;
                self.ping_interval.reset();
                Ok(first_rtt_ms)
            }
            PingState::TimedOut => {
                // if we receive a pong after timeout then we also reset the state, since the
                // connection was kept alive after timeout
                self.ping_sent_at = None;
                self.state = PingState::Ready;
                self.ping_interval.reset();
                Ok(None)
            }
        }
    }

    /// Returns the measured RTT in microseconds (None if not yet measured)
    pub(crate) fn rtt_us(&self) -> Option<u64> {
        self.last_rtt_us
    }

    /// Returns the measured RTT in milliseconds (None if not yet measured)
    pub(crate) fn rtt_ms(&self) -> Option<u64> {
        self.last_rtt_us.map(|us| us / 1000)
    }

    /// Start an immediate ping for RTT measurement.
    /// This transitions to WaitingForPong state and records the send time.
    pub(crate) fn start_immediate_ping(&mut self) {
        if self.state == PingState::Ready {
            self.state = PingState::WaitingForPong;
            self.ping_sent_at = Some(Instant::now());
            self.timeout_timer.as_mut().reset(Instant::now() + self.timeout);
        }
    }

    /// Returns the current state of the pinger.
    pub(crate) const fn state(&self) -> PingState {
        self.state
    }

    /// Polls the state of the pinger and returns whether a new ping needs to be sent or if a
    /// previous ping timed out.
    pub(crate) fn poll_ping(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<PingerEvent, PingerError>> {
        match self.state() {
            PingState::Ready => {
                if self.ping_interval.poll_tick(cx).is_ready() {
                    self.timeout_timer.as_mut().reset(Instant::now() + self.timeout);
                    self.state = PingState::WaitingForPong;
                    // Record ping sent time for RTT calculation
                    self.ping_sent_at = Some(Instant::now());
                    return Poll::Ready(Ok(PingerEvent::Ping))
                }
            }
            PingState::WaitingForPong => {
                if self.timeout_timer.as_mut().poll(cx).is_ready() {
                    self.state = PingState::TimedOut;
                    self.ping_sent_at = None;
                    return Poll::Ready(Ok(PingerEvent::Timeout))
                }
            }
            PingState::TimedOut => {
                // we treat continuous calls while in timeout as pending, since the connection is
                // not yet terminated
                return Poll::Pending
            }
        };
        Poll::Pending
    }
}

impl Stream for Pinger {
    type Item = Result<PingerEvent, PingerError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_ping(cx).map(Some)
    }
}

/// This represents the possible states of the pinger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PingState {
    /// There are no pings in flight, or all pings have been responded to, and we are ready to send
    /// a ping at a later point.
    Ready,
    /// We have sent a ping and are waiting for a pong, but the peer has missed n pongs.
    WaitingForPong,
    /// The peer has failed to respond to a ping.
    TimedOut,
}

/// The element type produced by a [`Pinger`], representing either a new
/// [`Ping`](super::P2PMessage::Ping)
/// message to send, or an indication that the peer should be timed out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PingerEvent {
    /// A new [`Ping`](super::P2PMessage::Ping) message should be sent.
    Ping,

    /// The peer should be timed out.
    Timeout,
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_ping_timeout() {
        let interval = Duration::from_millis(300);
        // we should wait for the interval to elapse and receive a pong before the timeout elapses
        let mut pinger = Pinger::new(interval, Duration::from_millis(20));
        assert_eq!(pinger.next().await.unwrap().unwrap(), PingerEvent::Ping);
        let _ = pinger.on_pong().unwrap();
        assert_eq!(pinger.next().await.unwrap().unwrap(), PingerEvent::Ping);

        tokio::time::sleep(interval).await;
        assert_eq!(pinger.next().await.unwrap().unwrap(), PingerEvent::Timeout);
        let _ = pinger.on_pong().unwrap();

        assert_eq!(pinger.next().await.unwrap().unwrap(), PingerEvent::Ping);
    }

    #[tokio::test]
    async fn test_rtt_measurement() {
        let interval = Duration::from_millis(50);
        let timeout = Duration::from_millis(1000);
        let mut pinger = Pinger::new_with_immediate_ping(interval, timeout);

        // RTT should be None before any ping/pong
        assert!(pinger.rtt_ms().is_none());

        // First ping should fire immediately
        assert_eq!(pinger.next().await.unwrap().unwrap(), PingerEvent::Ping);

        // Simulate some network delay
        tokio::time::sleep(Duration::from_millis(10)).await;

        // First pong should return Some(rtt_ms)
        let first_rtt = pinger.on_pong().unwrap();
        assert!(first_rtt.is_some());
        assert!(first_rtt.unwrap() >= 10); // At least 10ms delay

        // RTT should now be available
        assert!(pinger.rtt_ms().is_some());
        assert!(pinger.rtt_ms().unwrap() >= 10);

        // Wait for next ping
        assert_eq!(pinger.next().await.unwrap().unwrap(), PingerEvent::Ping);
        tokio::time::sleep(Duration::from_millis(5)).await;

        // Second pong should return None (RTT already measured)
        let second_rtt = pinger.on_pong().unwrap();
        assert!(second_rtt.is_none());

        // RTT should remain the same (first measurement)
        assert!(pinger.rtt_ms().is_some());
    }
}
