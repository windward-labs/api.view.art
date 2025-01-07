use alloy::primitives::Address;

pub const ADDRESS_KEY: &str = "address";
pub const CHANNEL_KEY: &str = "channel";
pub const CHANNEL_VIEW_KEY: &str = "channel_views";
pub const ITEM_STREAM_KEY: &str = "item_streams";
pub const USER_VIEW_KEY: &str = "user_views";
pub const USER_STREAM_KEY: &str = "user_streams";
pub const NONCE_KEY: &str = "nonce";
pub const TOP_CHANNEL_KEY: &str = "top_channels";
pub const TOP_ITEM_KEY: &str = "top_items";
/// Returns an ethers style address key, no longer used in the DB
pub fn old_address_key(address: &Address) -> String {
    format!("{}:{:#}", ADDRESS_KEY, address).to_lowercase()
}

pub fn address_key(address: &Address) -> String {
    format!("{}:{}", ADDRESS_KEY, address)
}

pub fn channel_key(channel: &str) -> String {
    format!("{}:{}", CHANNEL_KEY, channel).to_ascii_lowercase()
}

pub fn channel_view_key(channel: &str) -> String {
    format!("{}:{}", CHANNEL_VIEW_KEY, channel).to_ascii_lowercase()
}

pub fn item_stream_key(item_caid: &str) -> String {
    format!("{}:{}", ITEM_STREAM_KEY, item_caid).to_ascii_lowercase()
}

pub fn user_view_key(user: &str, channel: &str) -> String {
    format!("{}:{}:{}", USER_VIEW_KEY, user, channel).to_ascii_lowercase()
}

pub fn user_stream_key(user: &str, item_caid: &str) -> String {
    format!("{}:{}:{}", USER_STREAM_KEY, user, item_caid).to_ascii_lowercase()
}

pub fn top_channel_key(range: &str, channel: &str) -> String {
    format!("{}:{}:{}", TOP_CHANNEL_KEY, range, channel).to_ascii_lowercase()
}

pub fn all_top_channels_key(range: &str) -> String {
    format!("{}:{}:*", TOP_CHANNEL_KEY, range).to_ascii_lowercase()
}

pub fn top_item_key(range: &str, item_caid: &str) -> String {
    format!("{}:{}:{}", TOP_ITEM_KEY, range, item_caid).to_ascii_lowercase()
}

pub fn all_top_items_key(range: &str) -> String {
    format!("{}:{}:*", TOP_ITEM_KEY, range).to_ascii_lowercase()
}

pub fn nonce_key(address: &Address, chain_id: u64) -> String {
    format!("{}:{}:{}", NONCE_KEY, address, chain_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_old_address_key() {
        let key = old_address_key(
            &"0x3635a25d6c9b69c517aaeb17a9a30468202563fe"
                .parse()
                .unwrap(),
        );
        assert_eq!(key, "address:0x3635…63fe");

        let key = old_address_key(
            &"0x3635a25d6c9b69C517AAeB17A9a30468202563fE"
                .parse()
                .unwrap(),
        );
        assert_eq!(key, "address:0x3635…63fe");
    }

    #[test]
    fn test_address_key() {
        let key = address_key(
            &"0x3635a25d6c9b69c517aaeb17a9a30468202563fe"
                .parse()
                .unwrap(),
        );
        assert_eq!(key, "address:0x3635a25d6c9b69C517AAeB17A9a30468202563fE");

        let key = address_key(
            &"0x3635a25d6c9b69C517AAeB17A9a30468202563fE"
                .parse()
                .unwrap(),
        );
        assert_eq!(key, "address:0x3635a25d6c9b69C517AAeB17A9a30468202563fE");
    }

    #[test]
    fn test_channel_key() {
        let key = channel_key("test");
        assert_eq!(key, "channel:test");

        let key = channel_key("TEST");
        assert_eq!(key, "channel:test");
    }

    #[test]
    fn test_channel_view_key() {
        let key = channel_view_key("test");
        assert_eq!(key, "channel_views:test");

        let key = channel_view_key("TEST");
        assert_eq!(key, "channel_views:test");
    }

    #[test]
    fn test_item_stream_key() {
        let key = item_stream_key("test");
        assert_eq!(key, "item_streams:test");

        let key = item_stream_key("TEST");
        assert_eq!(key, "item_streams:test");
    }

    #[test]
    fn test_user_view_key() {
        let key = user_view_key("test", "testchannel");
        assert_eq!(key, "user_views:test:testchannel");

        let key = user_view_key("TEST", "TESTCHANNEL");
        assert_eq!(key, "user_views:test:testchannel");
    }

    #[test]
    fn test_user_stream_key() {
        let key = user_stream_key("test", "testitem");
        assert_eq!(key, "user_streams:test:testitem");

        let key = user_stream_key("TEST", "TESTITEM");
        assert_eq!(key, "user_streams:test:testitem");
    }

    #[test]
    fn test_top_channel_key() {
        let key = top_channel_key("daily", "testchannel");
        assert_eq!(key, "top_channels:daily:testchannel");

        let key = top_channel_key("DAILY", "TESTCHANNEL");
        assert_eq!(key, "top_channels:daily:testchannel");
    }

    #[test]
    fn test_all_top_channels_key() {
        let key = all_top_channels_key("daily");
        assert_eq!(key, "top_channels:daily:*");

        let key = all_top_channels_key("DAILY");
        assert_eq!(key, "top_channels:daily:*");
    }

    #[test]
    fn test_top_item_key() {
        let key = top_item_key("daily", "testitem");
        assert_eq!(key, "top_items:daily:testitem");

        let key = top_item_key("DAILY", "TESTITEM");
        assert_eq!(key, "top_items:daily:testitem");
    }

    #[test]
    fn test_all_top_items_key() {
        let key = all_top_items_key("daily");
        assert_eq!(key, "top_items:daily:*");

        let key = all_top_items_key("DAILY");
        assert_eq!(key, "top_items:daily:*");
    }

    #[test]
    fn test_nonce_key() {
        let key = nonce_key(
            &"0x3635a25d6c9b69c517aaeb17a9a30468202563fe"
                .parse()
                .unwrap(),
            8453,
        );
        assert_eq!(key, "nonce:0x3635a25d6c9b69C517AAeB17A9a30468202563fE:8453");
    }
}
