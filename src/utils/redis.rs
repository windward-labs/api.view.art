use {
    axum::http::StatusCode,
    bb8::PooledConnection,
    bb8_redis::{
        redis::{aio::ConnectionLike, AsyncCommands, Cmd, RedisResult},
        RedisConnectionManager,
    },
    serde_json::json,
    std::{future::Future, pin::Pin},
};

type TSRangeResult<'a> = Pin<Box<dyn Future<Output = RedisResult<Vec<(i64, String)>>> + Send + 'a>>;
type TSGetResult<'a> =
    Pin<Box<dyn Future<Output = RedisResult<Option<(i64, String)>>> + Send + 'a>>;

pub async fn increment_ts_key(
    conn: &mut PooledConnection<'_, RedisConnectionManager>,
    key: &str,
    increment: i64,
    timestamp: Option<i64>,
) -> Result<(), (StatusCode, String)> {
    conn.ts_incrby(key, increment, timestamp)
        .await
        .map_err(|err| {
            tracing::error!("Error incrementing ts key for {}: {:?}", key, err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "error": "Redis error while incrementing ts key" }).to_string(),
            )
        })
}

pub async fn delete_key(
    conn: &mut PooledConnection<'_, RedisConnectionManager>,
    key: &str,
) -> Result<(), (StatusCode, String)> {
    conn.del(key).await.map_err(|err| {
        tracing::error!("Error deleting key {}: {:?}", key, err);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": "Redis error while deleting key" }).to_string(),
        )
    })
}

pub async fn add_time_series_data(
    conn: &mut PooledConnection<'_, RedisConnectionManager>,
    key: &str,
    timestamp: i64,
    value: &str,
    retention: i64,
) -> Result<(), (StatusCode, String)> {
    conn.ts_add(key, timestamp, value, retention)
        .await
        .map_err(|err| {
            tracing::error!("Error adding time series data for {}: {:?}", key, err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "error": "Redis error while adding time series data" }).to_string(),
            )
        })
}

pub trait TimeSeriesCommands: Send {
    fn ts_incrby<'a>(
        &'a mut self,
        key: &'a str,
        increment: i64,
        timestamp: Option<i64>,
    ) -> Pin<Box<dyn Future<Output = RedisResult<()>> + Send + 'a>>;

    fn ts_add<'a>(
        &'a mut self,
        key: &'a str,
        timestamp: i64,
        value: &'a str,
        retention: i64,
    ) -> Pin<Box<dyn Future<Output = RedisResult<()>> + Send + 'a>>;

    fn ts_range<'a>(
        &'a mut self,
        key: &'a str,
        start_time: i64,
        end_time: i64,
    ) -> TSRangeResult<'a>;

    fn ts_get<'a>(&'a mut self, key: &'a str) -> TSGetResult<'a>;
}

impl<T: ConnectionLike + Send> TimeSeriesCommands for T {
    fn ts_incrby<'a>(
        &'a mut self,
        key: &'a str,
        increment: i64,
        timestamp: Option<i64>,
    ) -> Pin<Box<dyn Future<Output = RedisResult<()>> + Send + 'a>> {
        Box::pin(async move {
            let mut cmd = Cmd::new();
            cmd.arg("TS.INCRBY").arg(key).arg(increment);

            if let Some(ts) = timestamp {
                cmd.arg("TIMESTAMP").arg(ts);
            }

            cmd.query_async(self).await
        })
    }

    fn ts_add<'a>(
        &'a mut self,
        key: &'a str,
        timestamp: i64,
        value: &'a str,
        retention: i64,
    ) -> Pin<Box<dyn Future<Output = RedisResult<()>> + Send + 'a>> {
        Box::pin(async move {
            let mut cmd = Cmd::new();
            cmd.arg("TS.ADD")
                .arg(key)
                .arg(timestamp)
                .arg(value)
                .arg("RETENTION")
                .arg(retention)
                .arg("ON_DUPLICATE")
                .arg("LAST");

            cmd.query_async(self).await
        })
    }

    fn ts_range<'a>(
        &'a mut self,
        key: &'a str,
        start_time: i64,
        end_time: i64,
    ) -> TSRangeResult<'a> {
        Box::pin(async move {
            redis::cmd("TS.RANGE")
                .arg(key)
                .arg(start_time)
                .arg(end_time)
                .query_async(self)
                .await
        })
    }

    fn ts_get<'a>(&'a mut self, key: &'a str) -> TSGetResult<'a> {
        Box::pin(async move { redis::cmd("TS.GET").arg(key).query_async(self).await })
    }
}
