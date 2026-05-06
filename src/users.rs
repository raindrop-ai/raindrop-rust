use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;

use crate::client::ClientInner;
use crate::error::Result;

/// User identification payload.
#[derive(Debug, Default, Clone)]
pub struct User {
    /// User id.
    pub user_id: String,
    /// Free-form traits.
    pub traits: BTreeMap<String, Value>,
}

#[derive(Debug, Default, Clone, Serialize)]
struct IdentifyPayload {
    user_id: String,
    traits: BTreeMap<String, Value>,
}

pub(crate) async fn identify(client: &ClientInner, user: User) -> Result<()> {
    if !client.enabled {
        return Ok(());
    }
    if user.user_id.is_empty() {
        return Ok(());
    }
    let payload = IdentifyPayload {
        user_id: user.user_id,
        traits: user.traits,
    };
    client.transport.post_json("users/identify", &payload).await
}
