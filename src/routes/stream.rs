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

    // Generate Redis keys
    let channel_key = channel_key(&channel);
    let target_key_prefix = "channel_views:";
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
    check_target_exists(&mut conn, &channel_key).await?;

    // Check rate limit for the user and add rate limit key if not already set
    check_rate_limit(&mut conn, &user_view_key, &channel).await?;

    // Increment the view count at the current timestamp
    increment_ts_key(&mut conn, &channel_view_key, 1, Some(now)).await?;

    for (time_range_key, retention) in time_ranges.iter() {
        // Generate the Redis keys for the time range
        let channel_in_time_range_key = top_channel_key(time_range_key, &channel);
        let all_top_channels_key = all_top_channel_key(time_range_key);

        process_time_range(TimeRangeParams {
            conn: &mut conn,
            target_count_key: channel_view_key.to_string(),
            target_key_prefix: target_key_prefix.to_string(),
            target_in_time_range_key: channel_in_time_range_key.to_string(),
            all_top_targets_key: all_top_channels_key.to_string(),
            retention: *retention,
            now,
            top_targets_count: top_channels_count,
        })
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
    // Extract user IP and normalize item ID
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

struct TimeRangeParams<'a> {
    conn: &'a mut PooledConnection<'a, RedisConnectionManager>,
    target_count_key: String,
    target_key_prefix: String,
    target_in_time_range_key: String,
    all_top_targets_key: String,
    retention: i64,
    now: i64,
    top_targets_count: usize,
}

async fn process_time_range(params: TimeRangeParams<'_>) -> Result<(), (StatusCode, String)> {
    let start_time = params.now - (params.retention * 1000);

    let data_points = params
        .conn
        .ts_range(&params.target_count_key, start_time, params.now)
        .await
        .map_err(|err| {
            tracing::error!("Error getting count for {}: {:?}", params.target_count_key, err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "error": "Failed to get count" }).to_string(),
            )
        })?;

    let sorted_targets = get_sorted_top_targets(
        params.conn,
        &params.target_key_prefix,
        &params.all_top_targets_key,
    )
    .await?;

    let min_target = sorted_targets
        .last()
        .map(|(name, count)| (name.clone(), *count))
        .unwrap_or(("".to_string(), 0));

    let target_exists = sorted_targets
        .iter()
        .any(|(name, _)| *name == params.target_in_time_range_key);

    if target_exists {
        delete_key(params.conn, &params.target_in_time_range_key).await?;
    }

    let should_add_target = target_exists
        || sorted_targets.len() < params.top_targets_count
        || data_points.len() > min_target.1;

    if should_add_target {
        for data_point in &data_points {
            let data_timestamp = data_point.0;
            let data_retention = data_timestamp + (params.retention * 1000) - params.now;
            let data_view_count = &data_point.1;

            add_time_series_data(
                params.conn,
                &params.target_in_time_range_key,
                data_timestamp,
                data_view_count,
                data_retention,
            )
            .await?;
        }
    }

    let should_delete =
        (!target_exists && should_add_target) && sorted_targets.len() >= params.top_targets_count;

    if should_delete {
        delete_key(params.conn, &min_target.0).await?;
    }

    Ok(())
}

async fn check_target_exists(
    conn: &mut PooledConnection<'_, RedisConnectionManager>,
    key: &str,
) -> Result<(), (StatusCode, String)> {
    let exists = conn.exists::<_, bool>(&key).await.map_err(|err| {
        tracing::error!("Error checking if target exists: {:?}", err);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": "Redis error while checking existence" }).to_string(),
        )
    })?;

    if !exists {
        tracing::warn!("Target {} does not exist", key);
        return Err((
            StatusCode::NOT_FOUND,
            json!({ "error": "Target does not exist" }).to_string(),
        ));
    }
    Ok(())
}

async fn check_rate_limit(
    conn: &mut PooledConnection<'_, RedisConnectionManager>,
    rate_limit_key: &str,
    target: &str,
) -> Result<(), (StatusCode, String)> {
    if cfg!(test) {
        return Ok(()); // Skip rate limiting in tests
    }

    let ttl_seconds = 600; // 10 minutes in seconds
    let set_result: bool = conn.set_nx(rate_limit_key, target).await.map_err(|err| {
        tracing::error!("Error applying rate limit: {:?}", err);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": "Redis error while applying rate limit" }).to_string(),
        )
    })?;

    if !set_result {
        tracing::info!("User already viewed target within the last 10 minutes");
        return Err((
            StatusCode::BAD_REQUEST,
            json!({ "error": "User already viewed target within the last 10 minutes" }).to_string(),
        ));
    }

    let _: () = conn
        .expire(rate_limit_key, ttl_seconds)
        .await
        .map_err(|err| {
            tracing::error!("Error setting expiration: {:?}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "error": "Redis error while setting expiration" }).to_string(),
            )
        })?;

    Ok(())
}

async fn get_sorted_top_targets(
    conn: &mut PooledConnection<'_, RedisConnectionManager>,
    target_key_prefix: &str,
    all_top_targets_key: &str,
) -> Result<Vec<(String, usize)>, (StatusCode, String)> {
    let mut targets_with_counts: HashMap<String, (usize, i64)> = HashMap::new();

    // Get all top targets for the time range
    let mut keys: Vec<String> = Vec::new();
    {
        let mut scan: AsyncIter<'_, String> = conn
            .scan_match(all_top_targets_key)
            .await
            .map_err(|err| {
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
                json!({ "error": "Failed to get target data" }).to_string(),
            )
        })?;

        if let Some((timestamp, count_str)) = value {
            let count = count_str.parse::<i64>().unwrap_or(0);
            let target_name = key.trim_start_matches(target_key_prefix).to_string();
            targets_with_counts.insert(target_name, (count as usize, timestamp));
        }
    }

    // Sort by count (descending), then by timestamp (descending)
    let mut sorted_targets: Vec<(String, usize, i64)> = targets_with_counts
        .into_iter()
        .map(|(name, (count, timestamp))| (name, count, timestamp))
        .collect();
    sorted_targets.sort_by(|a, b| b.1.cmp(&a.1).then(b.2.cmp(&a.2)));

    Ok(sorted_targets
        .into_iter()
        .map(|(name, count, _)| (name, count))
        .collect())
}
