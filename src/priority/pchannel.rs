use crate::metrics::{PriorityQueueMetrics, Sampler};

use super::Priority;
use std::{
    future::poll_fn,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::mpsc;

/// Channels with polling prioritization.
pub fn unbounded_channel<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let (high_tx, high_rx) = mpsc::unbounded_channel();
    let (medium_tx, medium_rx) = mpsc::unbounded_channel();
    let (low_tx, low_rx) = mpsc::unbounded_channel();

    let sampler_high =
        Sampler::default().with_sample_size(1024).with_interval(Duration::from_secs(4));
    let sampler_mid = sampler_high.clone();
    let sampler_low = sampler_high.clone();
    (
        UnboundedSender { high: high_tx, medium: medium_tx, low: low_tx },
        UnboundedReceiver {
            high: high_rx,
            medium: medium_rx,
            low: low_rx,
            sampler_high,
            sampler_medium: sampler_mid,
            sampler_low,
        },
    )
}

/// Send values to the associated [`UnboundedReceiver`] with given priority.
#[derive(Debug)]
pub struct UnboundedSender<T> {
    high: mpsc::UnboundedSender<T>,
    medium: mpsc::UnboundedSender<T>,
    low: mpsc::UnboundedSender<T>,
}

impl<T> UnboundedSender<T> {
    /// Attempts to send a message without blocking with the given priority.
    pub fn send(&self, priority: Priority, message: T) -> Result<(), mpsc::error::SendError<T>> {
        match priority {
            Priority::High => self.high.send(message),
            Priority::Medium => self.medium.send(message),
            Priority::Low => self.low.send(message),
        }
    }
}

/// Receive values from the associated [`UnboundedSender`] according to message priority.
#[derive(Debug)]
pub struct UnboundedReceiver<T> {
    high: mpsc::UnboundedReceiver<T>,
    medium: mpsc::UnboundedReceiver<T>,
    low: mpsc::UnboundedReceiver<T>,

    // Metrics samplers.
    sampler_high: Sampler,
    sampler_medium: Sampler,
    sampler_low: Sampler,
}

impl<T> UnboundedReceiver<T> {
    /// Receives the next value for this receiver.
    pub async fn recv(&mut self) -> Option<T> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// Polls underlying channels according to the priority to receive the next message.
    ///
    /// This method returns:
    ///
    ///  * `Poll::Pending` if no messages are available on any of the channels and none of them is
    ///    closed.
    ///  * `Poll::Ready(Some(message))` if a message is available.
    ///  * `Poll::Ready(None)` if any of the channels has been closed.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        if let Poll::Ready(message) = self.high.poll_recv(cx) {
            self.sampler_high
                .sample(|| PriorityQueueMetrics::set_queue_size(self.high.len(), "high"));
            return Poll::Ready(message);
        }

        if let Poll::Ready(message) = self.medium.poll_recv(cx) {
            self.sampler_medium
                .sample(|| PriorityQueueMetrics::set_queue_size(self.medium.len(), "medium"));
            return Poll::Ready(message);
        }

        if let Poll::Ready(message) = self.low.poll_recv(cx) {
            self.sampler_low.sample(|| PriorityQueueMetrics::set_queue_size(self.low.len(), "low"));
            return Poll::Ready(message);
        }

        Poll::Pending
    }

    /// Closes all the receiving halves of the underlying channels, without dropping them.
    ///
    /// This prevents any further messages from being sent on the channel while
    /// still enabling the receiver to drain messages that are buffered.
    ///
    /// To guarantee that no messages are dropped, after calling `close()`,
    /// `recv()` must be called until `None` is returned.
    pub fn close(&mut self) {
        self.high.close();
        self.medium.close();
        self.low.close();
    }

    /// Checks if a channel is closed.
    ///
    /// This method returns `true` if any of the underlying channels has been closed. The channel
    /// entire priority channel should be considered closed as well.
    pub fn is_closed(&self) -> bool {
        self.high.is_closed() || self.medium.is_closed() || self.low.is_closed()
    }

    /// Checks if a channel is empty.
    ///
    /// This method returns `true` if the channel has no messages.
    pub fn is_empty(&self) -> bool {
        self.high.is_empty() && self.medium.is_empty() && self.low.is_empty()
    }

    /// Returns the number of messages in the channel.
    pub fn len(&self) -> usize {
        self.high.len() + self.medium.len() + self.low.len()
    }
}
