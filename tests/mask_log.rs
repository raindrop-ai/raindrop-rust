//! The mask failure warning. Its own test binary: the warning is rate limited
//! process-wide, so another test's failing mask must not consume it first.

mod common;

use std::sync::{Arc, Mutex};

use wiremock::MockServer;

use raindrop::{AiEvent, MASK_FAILED};

use crate::common::{fast_client_builder, mount_path};

#[test]
fn the_failure_warning_never_quotes_content_or_the_error() {
    #[derive(Clone)]
    struct VecWriter(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for VecWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let sink = Arc::new(Mutex::new(Vec::new()));
    let writer = sink.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_writer(move || VecWriter(writer.clone()))
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let server = MockServer::start().await;
            let _ = mount_path(&server, "POST", "/events/track").await;
            let client = fast_client_builder(&server)
                .mask(|_, _| Err("secret-in-error-message".into()))
                .build()
                .unwrap();
            client
                .track_ai(AiEvent {
                    user_id: "u1".into(),
                    event: "chat".into(),
                    input: "secret-in-content".into(),
                    ..Default::default()
                })
                .await
                .unwrap();
            client.flush().await.unwrap();
            let _ = client.close().await;
        });
    });
    let logs = String::from_utf8(sink.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("mask function failed"), "{logs}");
    assert!(logs.contains(MASK_FAILED));
    assert!(!logs.contains("secret-in-content"), "{logs}");
    assert!(!logs.contains("secret-in-error-message"), "{logs}");
}
