use {
    crate::args::Args,
    alloy::{
        network::Ethereum,
        providers::{
            fillers::{FillProvider, RecommendedFiller},
            Provider,
            ProviderBuilder,
            ReqwestProvider,
        },
        transports::http::Http,
    },
    anyhow::{Context, Result},
    axum::{
        self,
        http::{header, HeaderValue, Method},
        routing::{get, post},
        Extension,
        Router,
    },
    bb8::Pool,
    bb8_redis::{redis::AsyncCommands, RedisConnectionManager},
    changes::Changes,
    routes::auth::Keys,
    std::net::{Ipv4Addr, SocketAddr},
    tower_http::{
        cors::{AllowOrigin, CorsLayer},
        trace::TraceLayer,
    },
    tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt},
};

mod args;
mod caip;
mod changes;
mod model;
mod routes;
mod utils;

const ALLOWED_ORIGINS: &[&str] = &[
    "http://localhost:5173",                          // local development
    "https://macaw-resolved-arguably.ngrok-free.app", // ngrok
    "https://gentle-flea-officially.ngrok-free.app",  // ngrok
    "https://view.art",                               // production
];

#[derive(Clone)]
struct AppState {
    pool: Pool<RedisConnectionManager>,
    changes: Changes,
    provider: FillProvider<RecommendedFiller, ReqwestProvider, Http<reqwest::Client>, Ethereum>,
    keys: Keys,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "api_view_art=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::load().await?;
    tracing::info!("Running with args: {args:?}");

    // needed because Heroku Redis uses self-signed certificates
    let insecure_rediscloud_url = format!("{}/#insecure", String::from(args.rediscloud_url));

    let manager = RedisConnectionManager::new::<String>(insecure_rediscloud_url).unwrap();
    let pool = Pool::builder().build(manager).await.unwrap();
    {
        // ping the database before starting
        let mut conn = pool.get().await.unwrap();
        conn.set::<&str, &str, ()>("foo", "bar").await.unwrap();
        let result: String = conn.get("foo").await.unwrap();
        assert_eq!(result, "bar");
    }
    tracing::debug!("successfully connected to redis and pinged it");

    let rpc_url = String::from(args.base_rpc_url).parse()?;
    let provider = ProviderBuilder::new()
        .with_recommended_fillers()
        .on_http(rpc_url);
    let chain_id = provider.get_chain_id().await.unwrap();
    tracing::debug!("connected to chain id {:?}", chain_id);

    let keys = Keys::new(String::from(args.jwt_secret).as_bytes());

    let changes = Changes::new();

    let allowed_origins = AllowOrigin::list(
        ALLOWED_ORIGINS
            .iter()
            .map(|&origin| origin.parse::<HeaderValue>().unwrap())
            .chain(args.allowed_origins.unwrap_or_default()),
    );

    // build our application
    let app = app(
        allowed_origins,
        AppState {
            pool,
            changes,
            provider,
            keys,
        },
    )
    .into_make_service_with_connect_info::<SocketAddr>();

    // run it
    let listener =
        tokio::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, args.port)))
            .await
            .context("Failed to bind to port")?;
    tracing::debug!("listening on {}", listener.local_addr()?);
    axum::serve(listener, app)
        .await
        .context("Failed to serve")?;

    Ok(())
}

fn app(allowed_origins: AllowOrigin, state: AppState) -> Router {
    let keys = state.keys.clone();
    // build our application with a route
    Router::new().nest(
        "/v1",
        Router::new()
            .route("/nonce", post(routes::auth::get_nonce))
            .route("/auth", post(routes::auth::verify_auth))
            .route("/auth_privy", post(routes::auth::verify_privy_auth))
            .route(
                "/channel/:channel",
                get(routes::channel::get_channel).post(routes::channel::set_channel),
            )
            .route("/channel/:channel/taken", get(routes::channel::is_taken))
            .route(
                "/channel/:channel/summary",
                get(routes::channel::get_summary),
            )
            .route(
                "/wallet/:address/channels",
                get(routes::wallet::get_channels),
            )
            .route(
                "/view/:channel",
                get(routes::stream::get_channel_view_metrics)
                    .post(routes::stream::log_channel_view),
            )
            .route("/stream/:item_id", post(routes::stream::log_item_stream))
            .route("/top/weekly", get(routes::channel::get_top_channels_weekly))
            .layer(TraceLayer::new_for_http())
            .layer(Extension(keys))
            .layer(
                CorsLayer::new()
                    .allow_credentials(true)
                    .allow_origin(allowed_origins)
                    .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
                    .allow_headers([header::ACCEPT, header::CONTENT_TYPE, header::AUTHORIZATION]),
            )
            .with_state(state),
    )
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        alloy::{
            primitives::Bytes,
            signers::{local::PrivateKeySigner, Signer},
        },
        caip::asset_id::AssetId,
        chrono::{SecondsFormat, Utc},
        eventsource_stream::Eventsource,
        futures::StreamExt,
        model::{
            Action,
            ChannelContent,
            Display,
            EmptyChannelContent,
            GetAuth,
            Item,
            Status,
            VerifyAuth,
        },
        routes::auth::tests::get_team_api_key,
        serde_json::{json, Value},
        siwe::Message,
        tokio::net::TcpListener,
        url::Url,
        utils::keys::{nonce_key, old_address_key},
    };

    const REDISCLOUD_URL: &str = "redis://localhost:6379";
    const BASE_RPC_URL: &str = "https://mainnet.base.org";
    const JWT_SECRET: &str = "secret";

    /// A helper function that spawns our application in the background
    async fn spawn_app(host: impl Into<String>) -> (String, Pool<RedisConnectionManager>) {
        let host = host.into();

        let listener = TcpListener::bind(format!("{}:0", host)).await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let manager = RedisConnectionManager::new(REDISCLOUD_URL).unwrap();
        let pool = Pool::builder().build(manager).await.unwrap();
        {
            // flush redis before starting
            let mut conn = pool.get().await.unwrap();
            let _: () = bb8_redis::redis::cmd("FLUSHALL")
                .query_async(&mut *conn)
                .await
                .unwrap();
        }
        tracing::debug!("successfully connected to redis and pinged it");

        let provider = ProviderBuilder::new()
            .with_recommended_fillers()
            .on_http(BASE_RPC_URL.parse().unwrap());
        let chain_id = provider.get_chain_id().await.unwrap();
        tracing::debug!("connected to chain id {:?}", chain_id);

        let changes = Changes::new();

        let cloned_pool = pool.clone();
        tokio::spawn(async {
            axum::serve(
                listener,
                app(
                    vec![].into(),
                    AppState {
                        pool,
                        changes,
                        provider,
                        keys: Keys::new(String::from(JWT_SECRET).as_bytes()),
                    },
                ),
            )
            .await
            .unwrap();
        });
        // Returns address (e.g. http://127.0.0.1{random_port})
        (format!("http://{}:{}", host, port), cloned_pool)
    }

    #[test_log::test(tokio::test)]
    async fn integration_test() {
        let (listening_url, pool) = spawn_app("127.0.0.1").await;

        let mut event_stream = reqwest::Client::new()
            .get(format!("{}/v1/channel/test", listening_url))
            .header("User-Agent", "integration_test")
            .send()
            .await
            .unwrap()
            .bytes_stream()
            .eventsource()
            .take(3);

        let authorization = get_team_api_key(JWT_SECRET.to_string());

        let mut i = 0;
        while let Some(event) = event_stream.next().await {
            match event {
                Ok(event) => {
                    assert!(event.event == "content");

                    match i {
                        0 => {
                            let content =
                                serde_json::from_str::<EmptyChannelContent>(&event.data).unwrap();
                            assert!(content.empty);

                            // check if channel is taken
                            let result = reqwest::Client::new()
                                .get(format!("{}/v1/channel/test/taken", listening_url))
                                .header("User-Agent", "integration_test")
                                .send()
                                .await
                                .unwrap();
                            assert!(result.status().is_success());
                            let content =
                                serde_json::from_str::<Value>(&result.text().await.unwrap())
                                    .unwrap();
                            assert!(!content["taken"].as_bool().unwrap());

                            // setting the channel without the team API key should fail
                            let result = reqwest::Client::new()
                                .post(format!("{}/v1/channel/test", listening_url))
                                .header("User-Agent", "integration_test")
                                .json(&ChannelContent::ChannelContentV4 {
                                    items: vec![Item {
                                        id: "eip155:1/erc721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/771769".parse::<AssetId>().unwrap(),
                                        title: "test".to_string(),
                                        artist: Some("test".to_string()),
                                        url: Url::parse("https://test.com").unwrap(),
                                        thumbnail_url: Url::parse("https://test.com").unwrap(),
                                        external_url: Some(Url::parse("https://test.com").unwrap()),
                                        rotation_angle: 0,
                                        apply_matte: false,
                                        activate_by: "".to_string(),
                                        predominant_color: None,
                                    }],
                                    description: "test description".to_string(),
                                    display: Display {
                                        item_duration: 60,
                                        background_color: "#ffffff".to_string(),
                                        show_attribution: false,
                                        show_border: false,
                                    },
                                    shared_with: vec![],
                                    status: Status {
                                        item: 0,
                                        at: Utc::now(),
                                        action: Action::Played,
                                    },
                                })
                                .send()
                                .await
                                .unwrap();
                            assert!(result.status().is_client_error());

                            // set channel to 1 using the team API key
                            reqwest::Client::new()
                                .post(format!("{}/v1/channel/test", listening_url))
                                .header("User-Agent", "integration_test")
                                .header("Authorization", format!("Bearer {}", authorization))
                                .json(&ChannelContent::ChannelContentV4 {
                                    items: vec![Item {
                                        id: "eip155:1/erc721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/771769".parse::<AssetId>().unwrap(),
                                        title: "test".to_string(),
                                        artist: Some("test".to_string()),
                                        url: Url::parse("https://test.com").unwrap(),
                                        thumbnail_url: Url::parse("https://test.com").unwrap(),
                                        external_url: Some(Url::parse("https://test.com").unwrap()),
                                        rotation_angle: 0,
                                        apply_matte: false,
                                        activate_by: "".to_string(),
                                        predominant_color: None,
                                    }],
                                    description: "test description".to_string(),
                                    display: Display {
                                        item_duration: 60,
                                        background_color: "#ffffff".to_string(),
                                        show_attribution: false,
                                        show_border: false,
                                    },
                                    shared_with: vec![],
                                    status: Status {
                                        item: 0,
                                        at: Utc::now(),
                                        action: Action::Played,
                                    },
                                })
                                .send()
                                .await
                                .unwrap();
                        }
                        1 => {
                            let content =
                                serde_json::from_str::<ChannelContent>(&event.data).unwrap();

                            if let ChannelContent::ChannelContentV4 {
                                items,
                                display,
                                description,
                                shared_with,
                                status,
                            } = content
                            {
                                assert!(items.len() == 1);
                                assert!(items[0].id == "eip155:1/erc721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/771769"
                                    .parse::<AssetId>()
                                    .unwrap());
                                assert!(description == "test description");
                                assert!(display.item_duration == 60);
                                assert!(display.background_color == *"#ffffff".to_string());
                                assert!(!display.show_attribution);
                                assert!(!display.show_border);
                                assert!(status.item == 0);
                                assert!(shared_with.is_empty());
                                assert!(status.action == Action::Played);
                            } else {
                                panic!("Expected ChannelContentV4 variant");
                            }

                            // check if channel is taken
                            let result = reqwest::Client::new()
                                .get(format!("{}/v1/channel/test/taken", listening_url))
                                .header("User-Agent", "integration_test")
                                .send()
                                .await
                                .unwrap();
                            assert!(result.status().is_success());
                            let content =
                                serde_json::from_str::<Value>(&result.text().await.unwrap())
                                    .unwrap();
                            assert!(content["taken"].as_bool().unwrap());

                            // check if channel is in list of channels for address
                            let result = reqwest::Client::new()
                                .get(format!(
                                    "{}/v1/wallet/{:#?}/channels",
                                    listening_url,
                                    alloy::primitives::Address::ZERO
                                ))
                                .header("User-Agent", "integration_test")
                                .send()
                                .await
                                .unwrap();

                            assert!(result.status().is_success());
                            let content =
                                serde_json::from_str::<Value>(&result.text().await.unwrap())
                                    .unwrap();
                            assert!(content["channels"].as_array().unwrap().len() == 1);
                            assert!(content["channels"][0].as_str().unwrap() == "test");

                            // set played item to 1
                            reqwest::Client::new()
                                .post(format!("{}/v1/channel/test", listening_url))
                                .header("User-Agent", "integration_test")
                                .header("Authorization", format!("Bearer {}", authorization))
                                .json(&ChannelContent::ChannelContentV4 {
                                    items: vec![Item {
                                        id: "eip155:1/erc721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/771769".parse::<AssetId>().unwrap(),
                                        title: "test".to_string(),
                                        artist: Some("test".to_string()),
                                        url: Url::parse("https://test.com").unwrap(),
                                        thumbnail_url: Url::parse("https://test.com").unwrap(),
                                        external_url: Some(Url::parse("https://test.com").unwrap()),
                                        rotation_angle: 0,
                                        apply_matte: false,
                                        activate_by: "".to_string(),
                                        predominant_color: None,
                                    }],
                                    description: "test description".to_string(),
                                    display: Display {
                                        item_duration: 60,
                                        background_color: "#ffffff".to_string(),
                                        show_attribution: false,
                                        show_border: false,
                                    },
                                    shared_with: vec![],
                                    status: Status {
                                        item: 1,
                                        at: Utc::now(),
                                        action: Action::Played,
                                    },
                                })
                                .send()
                                .await
                                .unwrap();
                        }
                        2 => {
                            let content =
                                serde_json::from_str::<ChannelContent>(&event.data).unwrap();

                            if let ChannelContent::ChannelContentV4 {
                                items,
                                display,
                                description,
                                shared_with,
                                status,
                            } = content
                            {
                                assert!(items.len() == 1);
                                assert!(items[0].id == "eip155:1/erc721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/771769".parse::<AssetId>().unwrap());
                                assert!(description == "test description");
                                assert!(display.item_duration == 60);
                                assert!(display.background_color == *"#ffffff".to_string());
                                assert!(!display.show_attribution);
                                assert!(!display.show_border);
                                assert!(status.item == 1);
                                assert!(shared_with.is_empty());
                                assert!(status.action == Action::Played);
                            } else {
                                panic!("Expected ChannelContentV4 variant");
                            }
                        }
                        _ => {
                            panic!("Unexpected event");
                        }
                    }

                    i += 1;
                }
                Err(_) => {
                    panic!("Error in event stream");
                }
            }
        }

        assert!(i == 3);

        // Now we test with a wallet / user rather than the team API key
        let signer = PrivateKeySigner::random();

        let nonce = reqwest::Client::new()
            .post(format!("{}/v1/nonce", listening_url))
            .header("User-Agent", "integration_test")
            .json(&GetAuth {
                address: signer.address(),
                chain_id: 8453,
            })
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()["nonce"]
            .as_str()
            .unwrap()
            .to_string();

        let msg = format!(
            r#"view.art wants you to sign in with your Ethereum account:
{}

To finish connecting, you must sign a message in your wallet to verify that you are the owner of this account.

URI: https://view.art
Version: 1
Chain ID: 8453
Nonce: {}
Issued At: {}"#,
            signer.address().to_checksum(None),
            nonce,
            Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
        );
        let message: Message = msg.parse().unwrap();

        let signature: Bytes = signer
            .sign_message(message.to_string().as_bytes())
            .await
            .unwrap()
            .as_bytes()
            .into();

        let authorization = reqwest::Client::new()
            .post(format!("{}/v1/auth", listening_url))
            .header("User-Agent", "integration_test")
            .json(&VerifyAuth { message, signature })
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()["token"]
            .as_str()
            .unwrap()
            .to_string();

        let result = reqwest::Client::new()
            .post(format!("{}/v1/channel/test", listening_url))
            .header("User-Agent", "integration_test")
            .header("Authorization", format!("Bearer {}", authorization))
            .json(&ChannelContent::ChannelContentV4 {
                items: vec![Item {
                    id: "eip155:1/erc721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/771769"
                        .parse::<AssetId>()
                        .unwrap(),
                    title: "test".to_string(),
                    artist: Some("test".to_string()),
                    url: Url::parse("https://test.com").unwrap(),
                    thumbnail_url: Url::parse("https://test.com").unwrap(),
                    external_url: Some(Url::parse("https://test.com").unwrap()),
                    rotation_angle: 0,
                    apply_matte: false,
                    activate_by: "".to_string(),
                    predominant_color: None,
                }],
                description: "test description".to_string(),
                display: Display {
                    item_duration: 60,
                    background_color: "#ffffff".to_string(),
                    show_attribution: false,
                    show_border: false,
                },
                shared_with: vec![],
                status: Status {
                    item: 0,
                    at: Utc::now(),
                    action: Action::Played,
                },
            })
            .send()
            .await
            .unwrap();

        assert!(result.status().is_client_error());

        let result = reqwest::Client::new()
            .post(format!("{}/v1/channel/test-user", listening_url))
            .header("User-Agent", "integration_test")
            .header("Authorization", format!("Bearer {}", authorization))
            .json(&ChannelContent::ChannelContentV4 {
                items: vec![Item {
                    id: "eip155:1/erc721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/771769"
                        .parse::<AssetId>()
                        .unwrap(),
                    title: "test".to_string(),
                    artist: Some("test".to_string()),
                    url: Url::parse("https://test.com").unwrap(),
                    thumbnail_url: Url::parse("https://test.com").unwrap(),
                    external_url: Some(Url::parse("https://test.com").unwrap()),
                    rotation_angle: 0,
                    apply_matte: false,
                    activate_by: "".to_string(),
                    predominant_color: None,
                }],
                description: "test description".to_string(),
                display: Display {
                    item_duration: 60,
                    background_color: "#ffffff".to_string(),
                    show_attribution: false,
                    show_border: false,
                },
                shared_with: vec![],
                status: Status {
                    item: 0,
                    at: Utc::now(),
                    action: Action::Played,
                },
            })
            .send()
            .await
            .unwrap();

        assert!(result.status().is_success());

        // Now we test with a smart contract wallet
        {
            // Set the nonce for the wallet so that the stored signature is valid
            let nonce_key = nonce_key(
                &"0x3635a25d6c9b69c517aaeb17a9a30468202563fe"
                    .parse()
                    .unwrap(),
                8453,
            );
            let mut conn = pool.get().await.unwrap();
            conn.set::<&str, String, ()>(&nonce_key, "EbyKsNBvyN3Kg6sMR".to_string())
                .await
                .unwrap();

            // Set an old format address key for the wallet so that the migration flow
            // occurs
            let address_key = old_address_key(
                &"0x3635a25d6c9b69c517aaeb17a9a30468202563fe"
                    .parse()
                    .unwrap(),
            );
            conn.sadd::<&str, &str, ()>(&address_key, "test-migration")
                .await
                .unwrap();
        }

        let body = r#"{
            "message": "view.art wants you to sign in with your Ethereum account:\n0x3635a25d6c9b69C517AAeB17A9a30468202563fE\n\nTo finish connecting, you must sign a message in your wallet to verify that you are the owner of this account.\n\nURI: https://view.art\nVersion: 1\nChain ID: 8453\nNonce: EbyKsNBvyN3Kg6sMR\nIssued At: 2024-09-04T10:48:12.397Z",
            "signature": "0x000000000000000000000000ca11bde05977b3631167028862be2a173976ca110000000000000000000000000000000000000000000000000000000000000060000000000000000000000000000000000000000000000000000000000000028000000000000000000000000000000000000000000000000000000000000001e482ad56cb0000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000200000000000000000000000000ba5ed0c6aa8c49038f819e587e2633c4a9f428a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000e43ffba36f00000000000000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000040719803d80885ac58a268822f7069b4944800653b33c4187182cc1dfb7ec46760b63a53353d81c02a45eb70880c05c3f3d1b724d6a024c262fc779c4d033977ab0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000026000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000001e0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000c0000000000000000000000000000000000000000000000000000000000000012000000000000000000000000000000000000000000000000000000000000000170000000000000000000000000000000000000000000000000000000000000001c0271e6593325e5a24da85a977ab405b462f2e802ef0eaea719fef665be124bf49f2a4066f485308b73fcf3a3bdba9e03dd680f473218c575e45f7266b8aa77e0000000000000000000000000000000000000000000000000000000000000025f198086b2db17256731bc456673b96bcef23f51d1fbacdd7c4379ef65465572f1d0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000767b2274797065223a22776562617574686e2e676574222c226368616c6c656e6765223a22616e7549595a775631625071754c4f4378334d6570533455333044526975745f3169613837723466575159222c226f726967696e223a2268747470733a2f2f6b6579732e636f696e626173652e636f6d227d000000000000000000006492649264926492649264926492649264926492649264926492649264926492"
        }"#;

        let verify_auth: VerifyAuth = serde_json::from_str(body).unwrap();

        let authorization = reqwest::Client::new()
            .post(format!("{}/v1/auth", listening_url))
            .header("User-Agent", "integration_test")
            .json(&verify_auth)
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()["token"]
            .as_str()
            .unwrap()
            .to_string();

        let result = reqwest::Client::new()
            .post(format!("{}/v1/channel/test-wallet", listening_url))
            .header("User-Agent", "integration_test")
            .header("Authorization", format!("Bearer {}", authorization))
            .json(&ChannelContent::ChannelContentV3 {
                items: vec![Item {
                    id: "eip155:1/erc721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/771769"
                        .parse::<AssetId>()
                        .unwrap(),
                    title: "test".to_string(),
                    artist: Some("test".to_string()),
                    url: Url::parse("https://test.com").unwrap(),
                    thumbnail_url: Url::parse("https://test.com").unwrap(),
                    external_url: Some(Url::parse("https://test.com").unwrap()),
                    rotation_angle: 0,
                    apply_matte: false,
                    activate_by: "".to_string(),
                    predominant_color: None,
                }],
                display: Display {
                    item_duration: 60,
                    background_color: "#ffffff".to_string(),
                    show_attribution: false,
                    show_border: false,
                },
                status: Status {
                    item: 0,
                    at: Utc::now(),
                    action: Action::Played,
                },
            })
            .send()
            .await
            .unwrap();

        assert!(result.status().is_success());

        // Now we check that the address key was migrated
        let channels = reqwest::Client::new()
            .get(format!(
                "{}/v1/wallet/0x3635a25d6c9b69c517aaeb17a9a30468202563fe/channels",
                listening_url
            ))
            .header("User-Agent", "integration_test")
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();

        // test-wallet and test-migration should be merged
        assert_eq!(channels["channels"].as_array().unwrap().len(), 2);
        assert!(channels["channels"]
            .as_array()
            .unwrap()
            .contains(&json!("test-wallet")));
        assert!(channels["channels"]
            .as_array()
            .unwrap()
            .contains(&json!("test-migration")));
    }
}
