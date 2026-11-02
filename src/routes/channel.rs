use {
    super::auth::Claims,
    crate::{
        model::{ChannelContent, EmptyChannelContent},
        routes::internal_error,
        utils::{
            address_migration::migrate_addresses,
            keys::{address_key, channel_key},
            stream_helpers::get_channel_lifetime_views,
        },
        AppState,
    },
    alloy::primitives::Address,
    anyhow::{anyhow, Result},
    axum::{
        extract::{Path, State},
        http::StatusCode,
        response::{
            sse::{Event, Sse},
            IntoResponse,
        },
        Json,
    },
    bb8_redis::redis::AsyncCommands,
    futures::{stream::Stream, StreamExt},
    serde_json::json,
    std::{
        collections::{HashMap, HashSet},
        convert::Infallible,
        time::Duration,
    },
    tokio_stream::wrappers::BroadcastStream,
};

pub async fn get_channel(
    state: State<AppState>,
    Path(channel): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let channel = channel.to_ascii_lowercase();

    tracing::info!("get content for channel {}", channel);

    let key = channel_key(&channel);

    // get content from DB if set
    let initial_content: Option<ChannelContent> = {
        match state.pool.get().await {
            Ok(mut conn) => match conn.get(&key).await {
                Ok(content) => Some(content),
                Err(err) => {
                    tracing::error!("Error getting content for channel {}: {:?}", channel, err);
                    None
                }
            },
            Err(err) => {
                tracing::error!("Error getting connection from pool: {:?}", err);
                None
            }
        }
    };

    // subscribe to changes
    let (tx, rx) = {
        let mut changes = state.changes.clone();
        changes.subscribe(channel.clone()).await
    };

    // map changes of channel content to SSE events
    let stream = BroadcastStream::new(rx).map(|content| {
        let event = match content {
            Ok(content) => match content {
                Some(content) => {
                    // map to v2
                    let content_v4 = content.v4();
                    Event::default()
                        .json_data(content_v4)
                        .unwrap()
                        .event("content")
                }
                None => Event::default()
                    .json_data(EmptyChannelContent::default())
                    .unwrap()
                    .event("content"),
            },
            Err(err) => {
                tracing::error!("Error getting content: {:?}", err);
                Event::default()
            }
        };

        Ok::<Event, Infallible>(event)
    });

    // map channel content to v3
    let initial_content_v4 = initial_content.map(|content| content.v4());

    // send initial content to subscribers
    match tx.send(initial_content_v4) {
        Ok(len) => {
            tracing::debug!("sent {} to {} receivers", channel, len);
        }
        Err(err) => {
            tracing::error!("Error sending initial content: {:?}", err);
        }
    }

    // return SSE stream
    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(1))
            .text("keep-alive-text"),
    )
}

pub async fn is_taken(state: State<AppState>, Path(channel): Path<String>) -> impl IntoResponse {
    let channel = channel.to_ascii_lowercase();

    tracing::info!("check if channel {} is taken", channel);

    let key = channel_key(&channel);

    let exists: bool = match state.pool.get().await {
        Ok(mut conn) => match conn.exists::<&str, bool>(&key).await {
            Ok(exists) => exists,
            Err(err) => {
                tracing::error!("Error checking if channel exists: {:?}", err);
                return internal_error(anyhow!(err));
            }
        },
        Err(err) => {
            tracing::error!("Error getting connection from pool: {:?}", err);
            return internal_error(anyhow!(err));
        }
    };

    (StatusCode::OK, json!({ "taken": exists }).to_string())
}

pub async fn get_summary(state: State<AppState>, Path(channel): Path<String>) -> impl IntoResponse {
    let channel = channel.to_ascii_lowercase();

    tracing::info!("get summary for channel {}", channel);

    let key = channel_key(&channel);

    match state.pool.get().await {
        Ok(mut conn) => {
            let channel_views = match get_channel_lifetime_views(&mut conn, &channel).await {
                Ok(views) => views,
                Err(err) => {
                    tracing::error!(
                        "Error getting total views for channel {}: {:?}",
                        channel,
                        err
                    );
                    0
                }
            };

            // Fetch the channel content
            let initial_content: Option<ChannelContent> = match conn.get(&key).await {
                Ok(content) => Some(content),
                Err(err) => {
                    tracing::error!("Error getting content for channel {}: {:?}", channel, err);
                    None
                }
            };

            match initial_content {
                Some(content) => {
                    let mut summary = json!({});
                    let items = content.items();

                    // Add items to summary
                    summary["items"] = json!(items.len());

                    // Add thumbnail to summary
                    if let Some(thumbnail) = items.first().map(|item| item.thumbnail_url.clone()) {
                        summary["thumbnail"] = json!(thumbnail);
                    }

                    // Add view count to summary
                    summary["lifetimeViews"] = json!(channel_views);

                    (StatusCode::OK, json!(summary).to_string())
                }
                None => (
                    StatusCode::NOT_FOUND,
                    json!({ "status": false, "error": "channel not found" }).to_string(),
                ),
            }
        }
        Err(err) => {
            tracing::error!("Error getting connection from pool: {:?}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "status": false, "error": "Internal server error" }).to_string(),
            )
        }
    }
}

pub async fn set_channel(
    claims: Claims,
    state: State<AppState>,
    Path(channel): Path<String>,
    Json(channel_content): Json<ChannelContent>,
) -> impl IntoResponse {
    let channel = channel.to_ascii_lowercase();

    tracing::info!(
        "set channel content for {}, owned by {:?}",
        channel,
        claims.address
    );

    if !channel
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-')
    {
        return (
            StatusCode::BAD_REQUEST,
            json!({ "status": false, "error": "invalid channel name, must be ASCII alphabetic" })
                .to_string(),
        );
    }

    if let Err(err) = migrate_addresses(&claims.address, state.pool.get().await).await {
        tracing::error!("Error migrating address key: {:?}", err);
        return internal_error(anyhow!(err));
    }

    // Check if the user is an admin or the channel is owned by the user
    let claims_address_key = address_key(&claims.address);
    let owned: bool = if claims.address.is_zero() {
        true // Admins can set any channel
    } else {
        match state.pool.get().await {
            Ok(mut conn) => match conn
                .sismember::<&str, &str, bool>(&claims_address_key, &channel)
                .await
            {
                Ok(owned) => owned,
                Err(err) => {
                    tracing::error!("Error checking if channel is owned: {:?}", err);
                    return internal_error(anyhow!(err));
                }
            },
            Err(err) => {
                tracing::error!("Error getting connection from pool: {:?}", err);
                return internal_error(anyhow!(err));
            }
        }
    };

    // Check if channel already exists
    let channel_key = channel_key(&channel);
    let exists: bool = match state.pool.get().await {
        Ok(mut conn) => match conn.exists::<&str, bool>(&channel_key).await {
            Ok(exists) => exists,
            Err(err) => {
                tracing::error!("Error checking if channel exists: {:?}", err);
                return internal_error(anyhow!(err));
            }
        },
        Err(err) => {
            tracing::error!("Error getting connection from pool: {:?}", err);
            return internal_error(anyhow!(err));
        }
    };

    // Check if the channel exists and is not owned
    if exists && !owned {
        return (
            StatusCode::BAD_REQUEST,
            json!({ "status": false, "error": "channel already exists" }).to_string(),
        );
    }

    // Validate the incoming channel content
    if let Err(err) = validate_channel_content(&channel_content) {
        tracing::debug!("Error validating channel content: {:?}", err);
        return (
            StatusCode::BAD_REQUEST,
            json!({ "status": false, "error": err.to_string() }).to_string(),
        );
    }

    // Fetch the existing channel content from Redis (if it exists)
    let existing_content: Option<ChannelContent> = match state.pool.get().await {
        Ok(mut conn) => match conn.get::<&str, String>(&channel_key).await {
            Ok(content_json) => serde_json::from_str(&content_json).ok(),
            Err(_) => None,
        },
        Err(err) => {
            tracing::error!("Error getting connection from pool: {:?}", err);
            return internal_error(anyhow!(err));
        }
    };

    // Extract `shared_with` from both the input `channel_content` and
    // `existing_content`
    let new_shared_with: HashSet<Address> = match &channel_content {
        ChannelContent::ChannelContentV4 { shared_with, .. } => {
            shared_with.iter().cloned().collect()
        }
        _ => HashSet::new(),
    };

    let existing_shared_with: HashSet<Address> = match existing_content {
        Some(ChannelContent::ChannelContentV4 { shared_with, .. }) => {
            shared_with.into_iter().collect()
        }
        _ => HashSet::new(),
    };

    // Determine addresses to add and remove
    let to_add = new_shared_with
        .difference(&existing_shared_with)
        .cloned()
        .collect::<Vec<_>>();
    let to_remove = existing_shared_with
        .difference(&new_shared_with)
        .cloned()
        .collect::<Vec<_>>();

    // Reuse a single connection to update Redis
    let mut conn = match state.pool.get().await {
        Ok(conn) => conn,
        Err(err) => {
            tracing::error!("Error getting connection from pool: {:?}", err);
            return internal_error(anyhow!(err));
        }
    };

    // Add new editors by updating `address_key` for each added address
    for editor_address in &to_add {
        let editor_key = address_key(editor_address);
        if let Err(err) = conn.sadd::<&str, &str, ()>(&editor_key, &channel).await {
            tracing::error!(
                "Error adding channel to address set {}: {:?}",
                editor_address,
                err
            );
            return internal_error(anyhow!(err));
        }
    }

    // Remove old editors by updating `address_key` for each removed address
    for editor_address in &to_remove {
        let editor_key = address_key(editor_address);
        if let Err(err) = conn.srem::<&str, &str, ()>(&editor_key, &channel).await {
            tracing::error!(
                "Error removing channel from address set {}: {:?}",
                editor_address,
                err
            );
            return internal_error(anyhow!(err));
        }
    }

    tracing::debug!(
        "Updated channel content for channel {}",
        serde_json::to_string(&channel_content).unwrap()
    );

    // Finalize by updating the content in Redis
    match conn
        .sadd::<&str, &str, ()>(&claims_address_key, &channel)
        .await
    {
        Ok(_) => match conn
            .set::<&str, String, ()>(
                &channel_key,
                serde_json::to_string(&channel_content).unwrap(), // Always write new format
            )
            .await
        {
            Ok(_) => {
                state.changes.broadcast(&channel, channel_content).await;
                (StatusCode::OK, json!({ "status": true }).to_string())
            }
            Err(err) => {
                tracing::error!("Error setting content for channel {}: {:?}", channel, err);
                internal_error(anyhow!(err))
            }
        },
        Err(err) => {
            tracing::error!(
                "Error adding channel {} to owner set {}: {:?}",
                channel,
                claims.address,
                err
            );
            internal_error(anyhow!(err))
        }
    }
}

fn validate_channel_content(channel_content: &ChannelContent) -> Result<()> {
    // ensure all ids are unique in items
    let mut ids = HashSet::new();

    // Get items from content across all versions
    let items = channel_content.items();

    for item in items {
        if ids.contains(&item.id) {
            return Err(anyhow!("duplicate item id: {}", item.id));
        }
        ids.insert(item.id.clone());
    }

    for item in items {
        if item.title.is_empty() || item.title.len() > 100 {
            return Err(anyhow!(
                "item title must be between 1 and 100 characters, for id {}",
                item.id
            ));
        }

        if let Some(artist) = &item.artist {
            if artist.is_empty() || artist.len() > 100 {
                return Err(anyhow!(
                    "item artist must be omitted or between 1 and 100 characters, for id {}",
                    item.id
                ));
            }
        }
    }

    Ok(())
}

// Get the 30 channels with the most views in the last 7 days
pub async fn get_top_channels_weekly(State(state): State<AppState>) -> impl IntoResponse {
    let seven_days_ago = chrono::Utc::now()
        .checked_sub_signed(chrono::Duration::days(7))
        .unwrap()
        .timestamp_millis();

    let now = chrono::Utc::now().timestamp_millis();

    match state.pool.get().await {
        Ok(mut conn) => {
            let mut cursor = 0;
            let mut channels_with_counts: HashMap<String, usize> = HashMap::new();

            loop {
                let scan_result: Result<(u64, Vec<String>), _> = redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg("channel_views:*")
                    .query_async(&mut *conn)
                    .await;

                match scan_result {
                    Ok((next_cursor, keys)) => {
                        for key in keys {
                            let range_result: Result<Vec<(String, String)>, _> =
                                redis::cmd("TS.RANGE")
                                    .arg(&key)
                                    .arg(seven_days_ago)
                                    .arg(now)
                                    .query_async(&mut *conn)
                                    .await;

                            if let Ok(data_points) = range_result {
                                // Count each entry
                                let count = data_points.len();
                                if count > 0 {
                                    let channel_name =
                                        key.trim_start_matches("channel_views:").to_string();
                                    channels_with_counts.insert(channel_name, count);
                                }
                            }
                        }

                        cursor = next_cursor;
                        if cursor == 0 {
                            break;
                        }
                    }
                    Err(err) => {
                        tracing::error!("Failed to scan Redis keys: {:?}", err);
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(json!({ "error": "Failed to fetch channel data" })),
                        );
                    }
                }
            }

            // Sort and take the top 30
            let mut sorted_channels: Vec<(String, usize)> =
                channels_with_counts.into_iter().collect();
            sorted_channels.sort_by_key(|(_, views)| std::cmp::Reverse(*views));

            // Map sorted channels to the desired format
            let formatted_channels: Vec<_> = sorted_channels
                .into_iter()
                .take(30)
                .map(|(name, views)| json!({ "name": name, "views": views }))
                .collect();

            (StatusCode::OK, Json(json!(formatted_channels)))
        }
        Err(err) => {
            tracing::error!("Failed to get Redis connection: {:?}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to connect to Redis" })),
            )
        }
    }
}
