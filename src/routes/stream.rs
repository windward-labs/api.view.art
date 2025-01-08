use {
    crate::{
        utils::{
            keys::{
                all_top_channels_key,
                all_top_items_key,
                channel_key,
                channel_view_key,
                item_stream_key,
                top_channel_key,
                top_item_key,
                user_stream_key,
                user_view_key,
            },
            redis::{delete, ts_add, ts_incrby, TimeSeriesCommands},
            stream_helpers::get_channel_lifetime_views,
        },
        AppState,
    },
    axum::{
        extract::{ConnectInfo, Path, State},
        http::StatusCode,
        response::IntoResponse,
        Json,
    },
    bb8::PooledConnection,
    bb8_redis::{
        redis::{AsyncCommands, AsyncIter},
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
    tracing::info!("Fetching all-time views for channel {channel}");

    match state.pool.get().await {
        Ok(mut conn) => match get_channel_lifetime_views(&mut conn, &channel).await {
            Ok(total_views) => (
                StatusCode::OK,
                Json(json!({
                    "channel": channel,
                    "total_views": total_views
                })),
            ),
            Err(err) => {
                tracing::error!("Error getting total views for channel {channel}: {err:?}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "Failed to fetch total views" })),
                )
            }
        },
        Err(err) => {
            tracing::error!("Error getting connection from pool: {err:?}");
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
    tracing::info!("Log channel view for channel {channel}");

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

    // Define the number of top channels to track
    // Define time ranges for top channels
    let time_ranges: Vec<(&str, i64)> = vec![
        // ("daily", 24 * 60 * 60),        // 24 hours
        ("weekly", 7 * 24 * 60 * 60),   // 7 days
        // ("monthly", 30 * 24 * 60 * 60), // 30 days
    ];

    let top_channels_count = if cfg!(test) { 5 } else { 15 };

    // Check if the channel exists
    check_target_exists(&mut conn, &channel_key).await?;

    // Check rate limit for the user and add rate limit key if not already set
    check_rate_limit(&mut conn, &user_view_key, &channel).await?;

    // Increment the view count at the current timestamp
    ts_incrby(&mut conn, &channel_view_key, 1, Some(now)).await?;

    for (time_range_key, retention) in time_ranges.iter() {
        // Generate the Redis keys for the time range
        let channel_in_time_range_key = top_channel_key(time_range_key, &channel);
        let all_top_channels_key = all_top_channels_key(time_range_key);

        let params = TimeRangeParams {
            target_count_key: channel_view_key.to_string(),
            target_key_prefix: target_key_prefix.to_string(),
            target_in_time_range_key: channel_in_time_range_key.to_string(),
            all_top_targets_key: all_top_channels_key.to_string(),
            retention: *retention,
            now,
            top_targets_count: top_channels_count,
        };

        process_time_range(&mut conn, params).await?;
    }

    tracing::info!("Logged channel view");
    Ok((StatusCode::OK, json!({ "status": true }).to_string()))
}

pub async fn log_item_stream(
    state: State<AppState>,
    Path(item_id): Path<String>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Extract user IP and normalize item ID
    let user = addr.ip().to_string();
    let item_id = item_id.to_ascii_lowercase();
    tracing::info!("Log item stream for item with id {item_id}");

    // Generate Redis keys
    let item_stream_key = item_stream_key(&item_id);
    let target_key_prefix = "item_streams:";
    let user_stream_key = user_stream_key(&user, &item_id);

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

    // Define time ranges for top items
    let time_ranges: Vec<(&str, i64)> = vec![
        // ("daily", 24 * 60 * 60),        // 24 hours
        ("weekly", 7 * 24 * 60 * 60),   // 7 days
        // ("monthly", 30 * 24 * 60 * 60), // 30 days
    ];

    // Define the number of top items to track
    let top_items_count = if cfg!(test) { 5 } else { 30 };

    // Check rate limit for the user and add rate limit key if not already set
    check_rate_limit(&mut conn, &user_stream_key, &item_id).await?;

    // Increment the stream count at the current timestamp
    ts_incrby(&mut conn, &item_stream_key, 1, Some(now)).await?;

    for (time_range_key, retention) in time_ranges.iter() {
        // Generate the Redis keys for the time range
        let item_in_time_range_key = top_item_key(time_range_key, &item_id);
        let all_top_items_key = all_top_items_key(time_range_key);

        let params = TimeRangeParams {
            target_count_key: item_stream_key.to_string(),
            target_key_prefix: target_key_prefix.to_string(),
            target_in_time_range_key: item_in_time_range_key.to_string(),
            all_top_targets_key: all_top_items_key.to_string(),
            retention: *retention,
            now,
            top_targets_count: top_items_count,
        };

        process_time_range(&mut conn, params).await?;
    }

    tracing::info!("Logged item stream");
    Ok((StatusCode::OK, json!({ "status": true }).to_string()))
}

struct TimeRangeParams {
    target_count_key: String,
    target_key_prefix: String,
    target_in_time_range_key: String,
    all_top_targets_key: String,
    retention: i64,
    now: i64,
    top_targets_count: usize,
}

async fn process_time_range(
    conn: &mut PooledConnection<'_, RedisConnectionManager>,
    params: TimeRangeParams,
) -> Result<(), (StatusCode, String)> {
    let start_time = params.now - (params.retention * 1000);

    let data_points = conn
        .ts_range(&params.target_count_key, start_time, params.now)
        .await
        .map_err(|err| {
            tracing::error!(
                "Error getting count for {}: {:?}",
                params.target_count_key,
                err
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "error": "Failed to get count" }).to_string(),
            )
        })?;

    let sorted_targets =
        get_sorted_top_targets(conn, &params.target_key_prefix, &params.all_top_targets_key)
            .await?;

    let min_target = sorted_targets
        .last()
        .map(|(name, count)| (name.clone(), *count))
        .unwrap_or(("".to_string(), 0));

    let target_exists = sorted_targets
        .iter()
        .any(|(name, _)| *name == params.target_in_time_range_key);

    if target_exists {
        delete(conn, &params.target_in_time_range_key).await?;
    }

    let should_add_target = target_exists
        || sorted_targets.len() < params.top_targets_count
        || data_points.len() > min_target.1;

    if should_add_target {
        for data_point in &data_points {
            let data_timestamp = data_point.0;
            let data_retention = data_timestamp + (params.retention * 1000) - params.now;
            let data_view_count = &data_point.1;

            ts_add(
                conn,
                &params.target_in_time_range_key,
                data_timestamp,
                data_view_count,
                Some(data_retention),
            )
            .await?;
        }
    }

    let should_delete =
        (!target_exists && should_add_target) && sorted_targets.len() >= params.top_targets_count;

    if should_delete {
        delete(conn, &min_target.0).await?;
    }

    Ok(())
}

async fn check_target_exists<T>(
    conn: &mut PooledConnection<'_, RedisConnectionManager>,
    key: T,
) -> Result<(), (StatusCode, String)>
where
    T: AsRef<str>,
{
    let key_ref = key.as_ref();
    let exists = conn.exists::<_, bool>(key_ref).await.map_err(|err| {
        tracing::error!("Error checking if target exists: {err:?}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Redis error while checking existence".to_string(),
        )
    })?;

    if !exists {
        tracing::warn!("Target {key_ref} does not exist");
        return Err((
            StatusCode::NOT_FOUND,
            format!("Target {key_ref} does not exist"),
        ));
    }
    Ok(())
}

async fn check_rate_limit<T, U>(
    conn: &mut PooledConnection<'_, RedisConnectionManager>,
    rate_limit_key: T,
    target: U,
) -> Result<(), (StatusCode, String)>
where
    T: AsRef<str>,
    U: AsRef<str>,
{
    if cfg!(test) {
        return Ok(()); // Skip rate limiting in tests
    }

    let ttl_seconds = 600; // 10 minutes in seconds
    let set_result: bool = conn
        .set_nx(rate_limit_key.as_ref(), target.as_ref())
        .await
        .map_err(|err| {
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
        .expire(rate_limit_key.as_ref(), ttl_seconds)
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

pub async fn get_sorted_top_targets<T, U>(
    conn: &mut PooledConnection<'_, RedisConnectionManager>,
    target_key_prefix: T,
    all_top_targets_key: U,
) -> Result<Vec<(String, usize)>, (StatusCode, String)>
where
    T: AsRef<str>,
    U: AsRef<str>,
{
    let mut targets_with_counts: HashMap<String, (usize, i64)> = HashMap::new();
    let mut keys: Vec<String> = Vec::new();

    {
        // Get all top targets for the time range
        let mut scan: AsyncIter<'_, String> = conn
            .scan_match(all_top_targets_key.as_ref())
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
            let target_name = key
                .trim_start_matches(target_key_prefix.as_ref())
                .to_string();
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

#[cfg(test)]
mod tests {

    #[cfg(feature = "integration")]
    mod integration {
        use {
            crate::{
                routes::stream::{get_sorted_top_targets, log_channel_view},
                utils::{
                    keys::{all_top_channels_key, channel_view_key, top_channel_key},
                    redis::{ts_add, ts_incrby, TimeSeriesCommands},
                },
                AppState,
                Args,
                Changes,
                Keys,
            },
            alloy::providers::ProviderBuilder,
            axum::{
                extract::{ConnectInfo, Path, State},
                http::StatusCode,
            },
            bb8_redis::{redis::AsyncCommands, RedisConnectionManager},
            serial_test::serial,
            std::ops::DerefMut,
        };

        struct TestContext {
            pool: bb8::Pool<RedisConnectionManager>,
        }

        impl TestContext {
            async fn new() -> Self {
                let manager =
                    RedisConnectionManager::new(format!("redis://localhost:6379/{}", 0)).unwrap();
                let pool = bb8::Pool::builder()
                    .max_size(2)
                    .connection_timeout(std::time::Duration::from_secs(5))
                    .build(manager)
                    .await
                    .unwrap();

                Self { pool }
            }

            async fn cleanup(&self) -> Result<(), anyhow::Error> {
                let mut conn = self.pool.get().await?;
                let _: () = redis::cmd("FLUSHDB").query_async(conn.deref_mut()).await?;
                Ok(())
            }

            async fn populate_channels(&self, count: u32) -> Result<(), (StatusCode, String)> {
                let mut conn = self.pool.get().await.map_err(|err| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Redis connection error: {}", err),
                    )
                })?;

                let now = chrono::Utc::now().timestamp_millis();

                for i in 1..=count {
                    let channel = format!("channel{}", i);
                    let key = top_channel_key("weekly", &channel);
                    let value = format!("{}", i);
                    ts_add(&mut conn, &key, now, &value, None).await?;
                }

                Ok(())
            }

            async fn create_channel_key(&self, channel: &str) -> Result<(), (StatusCode, String)> {
                let mut conn = self.pool.get().await.map_err(|err| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Redis connection error: {}", err),
                    )
                })?;

                let key = format!("channel:{}", channel);
                let _: () = conn.set(key, "test_channel").await.map_err(|err| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Redis error: {}", err),
                    )
                })?;
                Ok(())
            }

            async fn ts_incrby(&self, key: &str, value: i64) -> Result<(), (StatusCode, String)> {
                let mut conn = self.pool.get().await.map_err(|err| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Redis connection error: {}", err),
                    )
                })?;

                ts_incrby(&mut conn, key, value, None).await?;
                Ok(())
            }

            async fn ts_get(
                &self,
                key: &str,
            ) -> Result<Option<(i64, String)>, (StatusCode, String)> {
                let mut conn = self.pool.get().await.map_err(|err| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Redis connection error: {}", err),
                    )
                })?;

                conn.ts_get(key).await.map_err(|err| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Redis error: {}", err),
                    )
                })
            }

            async fn get_sorted_targets(
                &self,
                prefix: &str,
                pattern: &str,
            ) -> Result<Vec<(String, usize)>, (StatusCode, String)> {
                let mut conn = self.pool.get().await.map_err(|err| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Redis connection error: {}", err),
                    )
                })?;

                get_sorted_top_targets(&mut conn, prefix, pattern).await
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
        #[serial]
        async fn test_log_channel_view_new_channel() -> Result<(), anyhow::Error> {
            // Create test context
            let ctx = TestContext::new().await;

            // Create test function arguments
            let args = Args::load().await?;
            let state = AppState {
                pool: ctx.pool.clone(),
                changes: Changes::new(),
                keys: Keys::new(String::from(args.jwt_secret).as_bytes()),
                provider: ProviderBuilder::new()
                    .with_recommended_fillers()
                    .on_http(String::from(args.base_rpc_url).parse()?),
            };

            let socket_addr: std::net::SocketAddr = "127.0.0.1:8000".parse()?;

            // Populate channels
            ctx.populate_channels(5).await.map_err(|err| {
                anyhow::anyhow!("Failed to populate channels: {} - {}", err.0, err.1)
            })?;

            // Define new channel name
            let channel = "test_new_channel";

            // Create channel key
            ctx.create_channel_key(channel).await.map_err(|err| {
                anyhow::anyhow!("Failed to create channel key: {} - {}", err.0, err.1)
            })?;

            // Log channel view
            let _ = log_channel_view(
                State(state.clone()),
                Path(channel.to_string()),
                ConnectInfo(socket_addr),
            )
            .await;

            // Check channel view count was incremented
            let channel_view_key = channel_view_key(channel);
            let channel_view_result = ctx.ts_get(&channel_view_key).await.unwrap();
            let channel_view_count = channel_view_result.unwrap().1;
            assert_eq!(channel_view_count, "1");

            // Assert new channel is not in sorted targets since it was just created
            let target_key_prefix = "channel_views:";
            let all_top_channels_key = all_top_channels_key("weekly");
            let sorted_targets = ctx
                .get_sorted_targets(target_key_prefix, &all_top_channels_key)
                .await
                .unwrap();
            assert!(
                !sorted_targets.iter().any(|(name, _)| name == channel),
                "Newly created channel should not be in sorted targets yet"
            );

            let _ = log_channel_view(
                State(state),
                Path(channel.to_string()),
                ConnectInfo(socket_addr),
            )
            .await;

            // Check channel view count was incremented
            let channel_view_result = ctx.ts_get(&channel_view_key).await.unwrap();
            let channel_view_count = channel_view_result.unwrap().1;
            assert_eq!(channel_view_count, "2");

            // Assert new channel is in sorted targets since it has enough views
            let top_channel_key_added_channel = top_channel_key("weekly", channel);
            let sorted_targets = ctx
                .get_sorted_targets(target_key_prefix, &all_top_channels_key)
                .await
                .unwrap();
            assert!(
                sorted_targets
                    .iter()
                    .any(|(name, _)| name == &top_channel_key_added_channel),
                "Newly created channel should be in sorted targets"
            );

            // Assert channel1 is not in sorted targets since it has been removed
            // from the top channels
            let top_channel_key_deleted_channel = top_channel_key("weekly", "channel1");
            let sorted_targets = ctx
                .get_sorted_targets(target_key_prefix, &all_top_channels_key)
                .await
                .unwrap();
            assert!(
                !sorted_targets
                    .iter()
                    .any(|(name, _)| name == &top_channel_key_deleted_channel),
                "Channel1 should not be in sorted targets"
            );

            ctx.cleanup().await?;
            Ok(())
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
        #[serial]
        async fn test_log_channel_view_existing_channel() -> Result<(), anyhow::Error> {
            // Create test context
            let ctx = TestContext::new().await;

            // Create test function arguments
            let args = Args::load().await?;
            let state = AppState {
                pool: ctx.pool.clone(),
                changes: Changes::new(),
                keys: Keys::new(String::from(args.jwt_secret).as_bytes()),
                provider: ProviderBuilder::new()
                    .with_recommended_fillers()
                    .on_http(String::from(args.base_rpc_url).parse()?),
            };

            let socket_addr: std::net::SocketAddr = "127.0.0.1:8000".parse()?;

            // Populate channels
            ctx.populate_channels(5).await.map_err(|err| {
                anyhow::anyhow!("Failed to populate channels: {} - {}", err.0, err.1)
            })?;

            // Define existing channel name
            let channel = "channel1";

            // Define channel view key
            let channel_view_key = channel_view_key(channel);

            // Create channel key
            ctx.create_channel_key(channel).await.map_err(|err| {
                anyhow::anyhow!("Failed to create channel key: {} - {}", err.0, err.1)
            })?;

            // Set channel view count to 1
            ctx.ts_incrby(&channel_view_key, 1).await.map_err(|err| {
                anyhow::anyhow!(
                    "Failed to increment channel view count: {} - {}",
                    err.0,
                    err.1
                )
            })?;

            let target_key_prefix = "channel_views:";
            let all_top_channels_key = all_top_channels_key("weekly");

            let original_sorted_targets = ctx
                .get_sorted_targets(target_key_prefix, &all_top_channels_key)
                .await
                .unwrap();
            assert_eq!(original_sorted_targets.len(), 5);
            assert_eq!(
                original_sorted_targets,
                vec![
                    ("top_channels:weekly:channel5".to_string(), 5),
                    ("top_channels:weekly:channel4".to_string(), 4),
                    ("top_channels:weekly:channel3".to_string(), 3),
                    ("top_channels:weekly:channel2".to_string(), 2),
                    ("top_channels:weekly:channel1".to_string(), 1),
                ]
            );

            // Log channel view
            let _ = log_channel_view(
                State(state.clone()),
                Path(channel.to_string()),
                ConnectInfo(socket_addr),
            )
            .await;

            // Check channel view count was incremented
            let channel_view_result = ctx.ts_get(&channel_view_key).await.unwrap();
            let channel_view_count = channel_view_result.unwrap().1;

            // Assert channel view count was incremented
            assert_eq!(channel_view_count, "2");

            // Assert channel is in correct position in sorted targets
            let updated_sorted_targets = ctx
                .get_sorted_targets(target_key_prefix, &all_top_channels_key)
                .await
                .unwrap();

            assert_eq!(updated_sorted_targets.len(), 5);
            assert_eq!(
                updated_sorted_targets,
                vec![
                    ("top_channels:weekly:channel5".to_string(), 5),
                    ("top_channels:weekly:channel4".to_string(), 4),
                    ("top_channels:weekly:channel3".to_string(), 3),
                    ("top_channels:weekly:channel1".to_string(), 2),
                    ("top_channels:weekly:channel2".to_string(), 2),
                ]
            );

            ctx.cleanup().await?;
            Ok(())
        }
    }
}
