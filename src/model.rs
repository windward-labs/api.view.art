use {
    crate::caip::asset_id::AssetId,
    alloy::primitives::{Address, Bytes},
    bb8_redis::redis::{FromRedisValue, RedisError, RedisResult, Value},
    chrono::{DateTime, Utc},
    serde::{Deserialize, Serialize},
    siwe::Message,
    url::Url,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetAuth {
    pub address: Address,
    pub chain_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyAuth {
    pub message: Message,
    pub signature: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Item {
    pub id: AssetId,
    pub title: String,
    pub artist: Option<String>,
    pub url: Url,
    pub thumbnail_url: Url,
    #[serde(default)]
    pub external_url: Option<Url>,
    #[serde(default = "default_item_rotation_angle")]
    pub rotation_angle: u32,
    pub apply_matte: bool,
    pub activate_by: String,
    pub predominant_color: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[serde(rename_all_fields = "camelCase")]
pub enum ChannelContent {
    ChannelContentV4 {
        items: Vec<Item>,
        #[serde(default)]
        description: String,
        #[serde(default = "default_display")]
        display: Display,
        #[serde(default = "default_shared_with")]
        shared_with: Vec<Address>,
        status: Status,
    },
    ChannelContentV3 {
        items: Vec<Item>,
        #[serde(default = "default_display")]
        display: Display,
        status: Status,
    },
    ChannelContentV2 {
        items: Vec<Item>,
        #[serde(default = "default_item_duration")]
        item_duration: u32,
        status: Status,
    },
    ChannelContentV1 {
        items: Vec<Item>,
        played: Played,
    },
}

impl ChannelContent {
    pub fn items(&self) -> &Vec<Item> {
        match self {
            ChannelContent::ChannelContentV1 { items, .. } => items,
            ChannelContent::ChannelContentV2 { items, .. } => items,
            ChannelContent::ChannelContentV3 { items, .. } => items,
            ChannelContent::ChannelContentV4 { items, .. } => items,
        }
    }

    pub fn v4(self) -> ChannelContent {
        match self {
            ChannelContent::ChannelContentV1 { items, played } => {
                ChannelContent::ChannelContentV3 {
                    items,
                    display: default_display(),
                    status: Status {
                        item: played.item,
                        at: played.at,
                        action: Action::Played,
                    },
                }
            }
            ChannelContent::ChannelContentV2 {
                items,
                item_duration,
                status,
            } => {
                let mut default_display = default_display();
                default_display.item_duration = item_duration;
                ChannelContent::ChannelContentV3 {
                    items,
                    display: default_display,
                    status,
                }
            }
            ChannelContent::ChannelContentV3 {
                items,
                display,
                status,
            } => ChannelContent::ChannelContentV4 {
                items,
                description: String::new(),
                display,
                shared_with: default_shared_with(),
                status,
            },
            content @ ChannelContent::ChannelContentV4 { .. } => content,
        }
    }
}

// ChannelContentV4

fn default_shared_with() -> Vec<Address> {
    vec![]
}

// ChannelContentV3

fn default_display() -> Display {
    Display {
        item_duration: 60,
        show_attribution: false,
        background_color: String::from("#000000"),
        show_border: false,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Display {
    pub item_duration: u32,
    pub show_attribution: bool,
    pub background_color: String,
    pub show_border: bool,
}

// ChannelContentV2

// Function to provide the default value
fn default_item_duration() -> u32 {
    60
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Status {
    pub item: u32,
    pub at: DateTime<Utc>,
    pub action: Action,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    Played,
    Paused,
}

// ChannelContentV1

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Played {
    pub item: u32,
    pub at: DateTime<Utc>,
}

// Item

// Function to provide the default value for item rotation angle
fn default_item_rotation_angle() -> u32 {
    0
}

// EmptyChannelContent

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmptyChannelContent {
    pub empty: bool,
}

impl Default for EmptyChannelContent {
    fn default() -> Self {
        Self { empty: true }
    }
}

impl FromRedisValue for ChannelContent {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let s = match v {
            Value::BulkString(data) => std::str::from_utf8(data).map_err(|err| {
                RedisError::from((
                    bb8_redis::redis::ErrorKind::TypeError,
                    "Error parsing string from utf8",
                    err.to_string(),
                ))
            })?,
            _ => {
                return Err(RedisError::from((
                    bb8_redis::redis::ErrorKind::TypeError,
                    "Unexpected Redis value type",
                )))
            }
        };
        serde_json::from_str(s).map_err(|err| {
            RedisError::from((
                bb8_redis::redis::ErrorKind::TypeError,
                "Error parsing Redis value",
                err.to_string(),
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ChannelContentV4Test {
        items: Vec<Item>,
        description: String,
        display: Display,
        shared_with: Vec<Address>,
        status: Status,
    }

    #[ignore]
    #[test]
    /// This test can be used to test the channel content v3 serialization
    /// You need to populate the test_channel_content.json file with the
    /// channel content you want to test and then run:
    /// cargo test test_channel_content_v3_serialization -- --ignored
    /// --nocapture
    fn test_channel_content_v4_serialization() {
        let channel_content = include_str!("../test/test_channel_content.json");
        let _: ChannelContentV4Test = serde_json::from_str(channel_content).unwrap();
    }
}
