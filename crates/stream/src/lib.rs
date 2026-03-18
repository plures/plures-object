//! Stream Engine — event bus for object storage operations.
//!
//! Captures and distributes [`StreamEvent`]s for:
//! - Replication triggers
//! - Workflow automation
//! - Audit logging
//! - External notifications

use std::sync::Arc;

use tokio::sync::{broadcast, RwLock};

use plures_object_core::StreamEvent;

/// A subscriber callback.
pub type EventHandler = Arc<dyn Fn(&StreamEvent) + Send + Sync>;

/// The stream engine — pub/sub for object storage events.
pub struct StreamEngine {
    tx: broadcast::Sender<StreamEvent>,
    _rx: broadcast::Receiver<StreamEvent>,
    handlers: Arc<RwLock<Vec<EventHandler>>>,
}

impl StreamEngine {
    /// Create a new stream engine with the given channel capacity.
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = broadcast::channel(capacity);
        Self {
            tx,
            _rx: rx,
            handlers: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Publish an event to all subscribers.
    pub fn emit(&self, event: StreamEvent) {
        // Best-effort — if no receivers, that's fine
        let _ = self.tx.send(event);
    }

    /// Subscribe to the event stream.
    pub fn subscribe(&self) -> broadcast::Receiver<StreamEvent> {
        self.tx.subscribe()
    }

    /// Register a synchronous handler that's called on each event.
    pub async fn on_event(&self, handler: EventHandler) {
        self.handlers.write().await.push(handler);
    }

    /// Dispatch an event to all registered handlers (sync).
    pub async fn dispatch(&self, event: &StreamEvent) {
        let handlers = self.handlers.read().await;
        for handler in handlers.iter() {
            handler(event);
        }
    }

    /// Emit and dispatch.
    pub async fn publish(&self, event: StreamEvent) {
        self.dispatch(&event).await;
        self.emit(event);
    }
}

impl Default for StreamEngine {
    fn default() -> Self {
        Self::new(1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plures_object_core::ObjectKey;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn emit_and_subscribe() {
        let engine = StreamEngine::new(16);
        let mut rx = engine.subscribe();

        engine.emit(StreamEvent::ObjectCreated {
            key: ObjectKey("test".into()),
            size: 42,
            etag: "abc".into(),
            timestamp: chrono::Utc::now(),
        });

        let event = rx.recv().await.unwrap();
        match event {
            StreamEvent::ObjectCreated { key, size, .. } => {
                assert_eq!(key.0, "test");
                assert_eq!(size, 42);
            }
            _ => panic!("wrong event type"),
        }
    }

    #[tokio::test]
    async fn multiple_subscribers() {
        let engine = StreamEngine::new(16);
        let mut rx1 = engine.subscribe();
        let mut rx2 = engine.subscribe();

        engine.emit(StreamEvent::ObjectDeleted {
            key: ObjectKey("gone".into()),
            timestamp: chrono::Utc::now(),
        });

        assert!(rx1.recv().await.is_ok());
        assert!(rx2.recv().await.is_ok());
    }

    #[tokio::test]
    async fn handler_dispatch() {
        let engine = StreamEngine::new(16);
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        engine
            .on_event(Arc::new(move |_| {
                count_clone.fetch_add(1, Ordering::Relaxed);
            }))
            .await;

        let event = StreamEvent::ObjectCreated {
            key: ObjectKey("x".into()),
            size: 1,
            etag: "e".into(),
            timestamp: chrono::Utc::now(),
        };

        engine.dispatch(&event).await;
        assert_eq!(count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn publish_emits_and_dispatches() {
        let engine = StreamEngine::new(16);
        let mut rx = engine.subscribe();
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        engine
            .on_event(Arc::new(move |_| {
                count_clone.fetch_add(1, Ordering::Relaxed);
            }))
            .await;

        engine
            .publish(StreamEvent::ObjectDeleted {
                key: ObjectKey("p".into()),
                timestamp: chrono::Utc::now(),
            })
            .await;

        assert_eq!(count.load(Ordering::Relaxed), 1);
        assert!(rx.recv().await.is_ok());
    }
}
