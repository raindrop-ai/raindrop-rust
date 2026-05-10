mod common;

use std::time::Duration;

use wiremock::MockServer;

use raindrop::{Client, Event, User};

use crate::common::{mount_any_post, mount_path};

fn local_url(server: &MockServer) -> String {
    format!("{}/v1/", server.uri())
}

#[tokio::test]
async fn no_key_no_local_fires_no_http() {
    let cloud = MockServer::start().await;
    let cloud_recorder = mount_any_post(&cloud).await;

    let client = Client::builder()
        .endpoint(format!("{}/", cloud.uri()))
        .disable_local_workshop()
        .partial_flush_interval(Duration::ZERO)
        .trace_flush_interval(Duration::ZERO)
        .build()
        .expect("build");

    assert!(
        !client.is_enabled(),
        "client without key + local should be no-op"
    );

    client
        .identify(User {
            user_id: "u1".into(),
            ..Default::default()
        })
        .await
        .expect("identify noop");
    client
        .track_event(Event {
            user_id: "u1".into(),
            event: "demo".into(),
            ..Default::default()
        })
        .await
        .expect("track_event noop");
    let _ = client.close().await;

    assert_eq!(cloud_recorder.count(), 0, "no cloud POST expected");
}

#[tokio::test]
async fn key_only_ships_to_cloud_only() {
    let cloud = MockServer::start().await;
    let cloud_recorder = mount_path(&cloud, "POST", "/v1/users/identify").await;

    let client = Client::builder()
        .write_key("rk_test")
        .endpoint(format!("{}/v1/", cloud.uri()))
        .disable_local_workshop()
        .partial_flush_interval(Duration::ZERO)
        .trace_flush_interval(Duration::ZERO)
        .base_delay(Duration::from_millis(1))
        .jitter_fraction(0.0)
        .build()
        .expect("build");

    client
        .identify(User {
            user_id: "u1".into(),
            ..Default::default()
        })
        .await
        .expect("identify");
    let _ = client.close().await;

    assert_eq!(cloud_recorder.count(), 1, "exactly one cloud POST expected");
}

#[tokio::test]
async fn key_plus_local_dual_ships() {
    let cloud = MockServer::start().await;
    let local = MockServer::start().await;
    let cloud_recorder = mount_path(&cloud, "POST", "/v1/users/identify").await;
    let local_recorder = mount_path(&local, "POST", "/v1/users/identify").await;

    let client = Client::builder()
        .write_key("rk_test")
        .endpoint(format!("{}/v1/", cloud.uri()))
        .local_workshop_url(local_url(&local))
        .partial_flush_interval(Duration::ZERO)
        .trace_flush_interval(Duration::ZERO)
        .base_delay(Duration::from_millis(1))
        .jitter_fraction(0.0)
        .build()
        .expect("build");

    client
        .identify(User {
            user_id: "u1".into(),
            ..Default::default()
        })
        .await
        .expect("identify");
    client.flush().await.expect("flush");
    let _ = client.close().await;

    assert_eq!(cloud_recorder.count(), 1, "expected cloud POST");
    assert_eq!(local_recorder.count(), 1, "expected local mirror POST");
}

#[tokio::test]
async fn no_key_plus_local_ships_to_local_only() {
    let cloud = MockServer::start().await;
    let local = MockServer::start().await;
    let cloud_recorder = mount_any_post(&cloud).await;
    let local_recorder = mount_path(&local, "POST", "/v1/users/identify").await;

    let client = Client::builder()
        .endpoint(format!("{}/v1/", cloud.uri()))
        .local_workshop_url(local_url(&local))
        .partial_flush_interval(Duration::ZERO)
        .trace_flush_interval(Duration::ZERO)
        .build()
        .expect("build");

    assert!(client.is_enabled(), "client with local URL must be enabled");

    client
        .identify(User {
            user_id: "u1".into(),
            ..Default::default()
        })
        .await
        .expect("identify");
    client.flush().await.expect("flush");
    let _ = client.close().await;

    assert_eq!(cloud_recorder.count(), 0, "no cloud POST when key absent");
    assert_eq!(local_recorder.count(), 1, "expected local mirror POST");
}

#[tokio::test]
async fn local_post_failure_does_not_break_cloud() {
    let cloud = MockServer::start().await;
    let cloud_recorder = mount_path(&cloud, "POST", "/v1/users/identify").await;

    // Intentionally point local at a bound-then-dropped port so connect fails
    // immediately. The mirror should swallow the error; cloud must still ship.
    let dead_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let dead_port = dead_listener.local_addr().unwrap().port();
    drop(dead_listener);

    let client = Client::builder()
        .write_key("rk_test")
        .endpoint(format!("{}/v1/", cloud.uri()))
        .local_workshop_url(format!("http://127.0.0.1:{}/v1/", dead_port))
        .partial_flush_interval(Duration::ZERO)
        .trace_flush_interval(Duration::ZERO)
        .base_delay(Duration::from_millis(1))
        .jitter_fraction(0.0)
        .build()
        .expect("build");

    client
        .identify(User {
            user_id: "u1".into(),
            ..Default::default()
        })
        .await
        .expect("identify (cloud must succeed even if local fails)");
    let _ = client.close().await;

    assert_eq!(cloud_recorder.count(), 1, "cloud POST must still land");
}
