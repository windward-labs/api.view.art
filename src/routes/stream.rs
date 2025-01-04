use {
    crate::{
        routes::internal_error,
        utils::{
            keys::{
                all_top_channel_key,
                channel_key,
                channel_view_key,
                item_stream_key,
                top_channel_key,
                user_stream_key,
                user_view_key,
            },
            redis::{add_time_series_data, delete_key, increment_ts_key, TimeSeriesCommands},
            stream_helpers::get_channel_lifetime_views,
        },
        AppState,
    },
    anyhow::anyhow,
    axum::{
        extract::{ConnectInfo, Path, State},
        http::StatusCode,
        response::IntoResponse,
        Json,
    },
    bb8::PooledConnection,
    bb8_redis::{
        redis::{AsyncCommands, AsyncIter, RedisError},
        RedisConnectionManager,
    },
    serde_json::json,
    std::{collections::HashMap, net::SocketAddr},
};

pub async fn get_channel_view_metrics(
    state: State<AppState>,
    Path(channel): Path<String>,
) -> impl IntoResponse {
    let channel = channel.to_ascii_lowercase();
    tracing::info!("Fetching all-time views for channel {}", channel);

    match state.pool.get().await {
        Ok(mut conn) => match get_channel_lifetime_views(&mut conn, &channel).await {
            Ok(total_views) => (
                StatusCode::OK,
                Json(json!({ "channel": channel, "total_views": total_views })),
            ),
            Err(err) => {
                tracing::error!(
                    "Error getting total views for channel {}: {:?}",
                    channel,
                    err
                );
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "Failed to fetch total views" })),
                )
            }
        },
        Err(err) => {
            tracing::error!("Error getting connection from pool: {:?}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to connect to Redis" })),
            )
        }
    }
}

pub async fn log_channel_view(
    state: State<AppState>,
    Path(channel): Path<String>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Extract user IP and normalize item ID
    let user = addr.ip().to_string();
    let channel = channel.to_ascii_lowercase();
    tracing::info!("Log channel view for channel {}", channel);

    // Generate Redis keys for item stream tracking
    let channel_view_key = channel_view_key(&channel);
    let user_view_key = user_view_key(&user, &channel);

    // Get current timestamp
    let now = chrono::Utc::now().timestamp_millis();

    // Get Redis connection
    let mut conn = state.pool.get().await.map_err(|err| {
        tracing::error!("Error getting Redis connection: {:?}", err);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": "Failed to connect to Redis" }).to_string(),
        )
    })?;

    // Define time ranges for top channels
    let time_ranges: Vec<(&str, i64)> = vec![
        ("daily", 24 * 60 * 60),        // 24 hours
        ("weekly", 7 * 24 * 60 * 60),   // 7 days
        ("monthly", 30 * 24 * 60 * 60), // 30 days
    ];

    // Define the number of top channels to track
    let top_channels_count = 30;

    // Check if the channel exists
    check_channel_exists(&mut conn, &channel).await?;

    // Check rate limit for the user and add rate limit key if not already set
    check_rate_limit(&mut conn, &user_view_key, &channel).await?;

    // Increment the view count at the current timestamp
    increment_ts_key(&mut conn, &channel_view_key, 1, Some(now)).await?;

    for (time_range_key, retention) in time_ranges.iter() {
        process_time_range(
            &mut conn,
            &channel,
            time_range_key,
            *retention,
            now,
            top_channels_count,
        )
        .await?;
    }

    tracing::info!("Logged channel view");
    Ok((StatusCode::OK, json!({ "status": true }).to_string()))
}

pub async fn log_item_stream(
    state: State<AppState>,
    Path(item_id): Path<String>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    let user = addr.ip().to_string();
    let item_id = item_id.to_ascii_lowercase();
    tracing::info!("Log item stream for item with id {}", item_id);

    let item_stream_key = item_stream_key(&item_id);
    let user_stream_key = user_stream_key(&user, &item_id);

    match state.pool.get().await {
        Ok(mut conn) => {
            let ttl_seconds = 600; // 10 minutes in seconds

            let set_result: Result<bool, RedisError> = redis::cmd("SET")
                .arg(&user_stream_key)
                .arg(&item_id)
                .arg("NX")
                .arg("EX")
                .arg(ttl_seconds)
                .query_async(&mut *conn)
                .await;

            match set_result {
                Ok(true) => {
                    tracing::info!("Set view and rate limit for user");
                    // Increment the count at the current timestamp by 1
                    if let Err(err) = conn
                        .ts_incrby(
                            &item_stream_key,
                            1,
                            Some(chrono::Utc::now().timestamp_millis()),
                        )
                        .await
                    {
                        tracing::error!(
                            "Error logging item stream for {}: {:?}",
                            item_stream_key,
                            err
                        );
                        return internal_error(anyhow!(err));
                    }
                }
                Ok(false) => {
                    tracing::info!("User already viewed item within the last 10 minutes");
                }
                Err(err) => {
                    tracing::error!("Error applying rate limit for {}: {:?}", user, err);
                    return internal_error(anyhow!(err));
                }
            }
        }
        Err(err) => {
            tracing::error!("Error getting connection from pool: {:?}", err);
            return internal_error(anyhow!(err));
        }
    }

    (StatusCode::OK, json!({ "status": true }).to_string())
}

async fn process_time_range(
    conn: &mut PooledConnection<'_, RedisConnectionManager>,
    channel: &str,
    time_range_key: &str,
    retention: i64,
    now: i64,
    top_channels_count: usize,
) -> Result<(), (StatusCode, String)> {
    let channel_view_key = channel_view_key(channel);
    let channel_in_time_range_key = top_channel_key(time_range_key, channel);
    let start_time = now - (retention * 1000);

    let data_points = conn
        .ts_range(&channel_view_key, start_time, now)
        .await
        .map_err(|err| {
            tracing::error!("Error getting view count: {:?}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "error": "Failed to get view count" }).to_string(),
            )
        })?;

    let sorted_channels = get_sorted_top_channels(conn, time_range_key, top_channels_count).await?;

    let min_channel = sorted_channels
        .last()
        .map(|(name, count)| (name.clone(), *count))
        .unwrap_or(("".to_string(), 0));

    let channel_exists = sorted_channels
        .iter()
        .any(|(name, _)| name == &channel_in_time_range_key);

    if channel_exists {
        delete_key(conn, &channel_in_time_range_key).await?;
    }

    let should_add_channel = channel_exists
        || sorted_channels.len() < top_channels_count
        || data_points.len() > min_channel.1;

    if should_add_channel {
        for data_point in &data_points {
            let data_timestamp = data_point.0;
            let data_retention = data_timestamp + (retention * 1000) - now;
            let data_view_count = &data_point.1;

            add_time_series_data(
                conn,
                &channel_in_time_range_key,
                data_timestamp,
                data_view_count,
                data_retention,
            )
            .await?;
        }
    }

    let should_delete =
        (!channel_exists && should_add_channel) && sorted_channels.len() >= top_channels_count;

    if should_delete {
        delete_key(conn, &min_channel.0).await?;
    }

    Ok(())
}

async fn check_channel_exists(
    conn: &mut PooledConnection<'_, RedisConnectionManager>,
    channel: &str,
) -> Result<(), (StatusCode, String)> {
    let channel_key = channel_key(channel);
    let exists = conn.exists::<_, bool>(&channel_key).await.map_err(|err| {
        tracing::error!("Error checking if channel exists: {:?}", err);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": "Redis error while checking existence" }).to_string(),
        )
    })?;

    if !exists {
        tracing::warn!("Channel {} does not exist", channel);
        return Err((
            StatusCode::NOT_FOUND,
            json!({ "error": "Channel does not exist" }).to_string(),
        ));
    }
    Ok(())
}

async fn check_rate_limit(
    conn: &mut PooledConnection<'_, RedisConnectionManager>,
    user_view_key: &str,
    channel: &str,
) -> Result<(), (StatusCode, String)> {
    if cfg!(test) {
        return Ok(()); // Skip rate limiting in tests
    }

    let ttl_seconds = 600; // 10 minutes in seconds
    let set_result: bool = conn.set_nx(user_view_key, channel).await.map_err(|err| {
        tracing::error!("Error applying rate limit: {:?}", err);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": "Redis error while applying rate limit" }).to_string(),
        )
    })?;

    if !set_result {
        tracing::info!("User already viewed channel within the last 10 minutes");
        return Err((
            StatusCode::BAD_REQUEST,
            json!({ "error": "User already viewed channel within the last 10 minutes" })
                .to_string(),
        ));
    }

    // Add explicit type annotation for expire command
    let _: () = conn.expire(user_view_key, ttl_seconds).await.map_err(|err| {
        tracing::error!("Error setting expiration: {:?}", err);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": "Redis error while setting expiration" }).to_string(),
        )
    })?;

    Ok(())
}

async fn get_sorted_top_channels(
    conn: &mut PooledConnection<'_, RedisConnectionManager>,
    time_range_key: &str,
    _top_channels_count: usize,
) -> Result<Vec<(String, usize)>, (StatusCode, String)> {
    let mut channels_with_counts: HashMap<String, (usize, i64)> = HashMap::new();

    // Get all top channels for the time range
    let all_top_channel_key = all_top_channel_key(time_range_key);

    // Collect all keys
    let mut keys: Vec<String> = Vec::new();
    {
        let mut scan: AsyncIter<'_, String> =
            conn.scan_match(&all_top_channel_key).await.map_err(|err| {
                tracing::error!("Failed to create Redis scan: {:?}", err);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    json!({ "error": "Failed to scan Redis keys" }).to_string(),
                )
            })?;

        while let Some(key) = scan.next_item().await {
            keys.push(key);
        }
    }

    // Process the collected keys
    for key in keys {
        let value: Option<(i64, String)> = conn.ts_get(&key).await.map_err(|err| {
            tracing::error!("Failed to get value for key {}: {:?}", key, err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "error": "Failed to get channel data" }).to_string(),
            )
        })?;

        if let Some((timestamp, count_str)) = value {
            let count = count_str.parse::<i64>().unwrap_or(0);
            let channel_name = key.trim_start_matches("channel_views:").to_string();
            channels_with_counts.insert(channel_name, (count as usize, timestamp));
        }
    }

    // Sort by view count (descending), then by timestamp (descending)
    let mut sorted_channels: Vec<(String, usize, i64)> = channels_with_counts
        .into_iter()
        .map(|(name, (views, timestamp))| (name, views, timestamp))
        .collect();
    sorted_channels.sort_by(|a, b| b.1.cmp(&a.1).then(b.2.cmp(&a.2)));

    Ok(sorted_channels
        .into_iter()
        .map(|(name, views, _)| (name, views))
        .collect())
}
