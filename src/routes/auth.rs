use {
    crate::{
        args::Args,
        model::{GetAuth, VerifyAuth},
        routes::internal_error,
        utils::keys::nonce_key,
        AppState,
    },
    alloy::primitives::Address,
    anyhow::anyhow,
    axum::{
        async_trait,
        extract::{FromRequestParts, State},
        http::{request::Parts, StatusCode},
        response::{IntoResponse, Response},
        Extension,
        Json,
        RequestPartsExt,
    },
    axum_extra::{
        headers::{authorization::Bearer, Authorization},
        TypedHeader,
    },
    bb8_redis::redis::{AsyncCommands, SetExpiry, SetOptions},
    chrono::Utc,
    erc6492::verify_signature,
    jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation},
    serde::{Deserialize, Deserializer, Serialize},
    serde_json::json,
    siwe::{generate_nonce, VerificationError},
    std::str::FromStr,
};

/// 1 hour
const NONCE_EXPIRY: u64 = 60 * 60;

#[derive(Debug, Deserialize)]
pub struct PrivyClaims {
    #[serde(
        rename = "linked_accounts",
        deserialize_with = "deserialize_linked_accounts"
    )]
    pub linked_accounts: Vec<LinkedAccount>,

    #[serde(rename = "iss")]
    pub issuer: String,

    #[serde(rename = "iat")]
    pub issued_at: usize,

    #[serde(rename = "aud")]
    pub app_id: String,

    #[serde(rename = "exp")]
    pub expiration: usize,
}

#[derive(Debug, Deserialize)]
pub struct LinkedAccount {
    #[serde(rename = "type")]
    pub account_type: String,

    /// Present for wallet and email accounts; absent for others such as
    /// passkeys.
    #[serde(default)]
    pub address: Option<String>,
}

// Custom deserializer for `linked_accounts`
fn deserialize_linked_accounts<'de, D>(deserializer: D) -> Result<Vec<LinkedAccount>, D::Error>
where
    D: Deserializer<'de>,
{
    // Deserialize the field as a string
    let linked_accounts_str = String::deserialize(deserializer)?;

    // Parse the string as JSON to get a Vec<LinkedAccount>
    serde_json::from_str::<Vec<LinkedAccount>>(&linked_accounts_str)
        .map_err(serde::de::Error::custom)
}

impl PrivyClaims {
    pub fn valid(&self, app_id: &str) -> Result<(), anyhow::Error> {
        if self.app_id != app_id {
            return Err(anyhow!("aud claim must be your Privy App ID."));
        }
        if self.issuer != "privy.io" {
            return Err(anyhow!("iss claim must be 'privy.io'"));
        }
        if self.expiration < Utc::now().timestamp() as usize {
            return Err(anyhow!("Token is expired."));
        }
        if self.issued_at > Utc::now().timestamp() as usize {
            return Err(anyhow!("Token is issued in the future."));
        }
        if self.linked_accounts.is_empty() {
            return Err(anyhow!("No linked accounts found."));
        }

        Ok(())
    }

    /// Whether `wallet` is one of the account's linked wallets.
    pub fn links_wallet(&self, wallet: &Address) -> bool {
        self.linked_accounts.iter().any(|account| {
            account.account_type == "wallet"
                && account
                    .address
                    .as_deref()
                    .and_then(|address| Address::from_str(address).ok())
                    .is_some_and(|wallet_address| wallet_address == *wallet)
        })
    }
}

pub async fn get_nonce(
    state: State<AppState>,
    Json(GetAuth { address, chain_id }): Json<GetAuth>,
) -> impl IntoResponse {
    tracing::info!("getting nonce for {:?}", address);

    let nonce_key = nonce_key(&address, chain_id);

    match state.pool.get().await {
        Ok(mut conn) => match conn
            .get_ex::<&str, String>(&nonce_key, redis::Expiry::EX(NONCE_EXPIRY))
            .await
        {
            Ok(nonce) => (
                StatusCode::OK,
                json!({ "status": true, "nonce": nonce }).to_string(),
            ),
            Err(_) => {
                tracing::info!(
                    "No nonce found for address {}, generating new nonce",
                    address
                );
                let random_nonce = generate_nonce();

                match conn
                    .set_options::<&str, String, ()>(
                        &nonce_key,
                        random_nonce.clone(),
                        SetOptions::default().with_expiration(SetExpiry::EX(NONCE_EXPIRY)),
                    )
                    .await
                {
                    Ok(_) => (
                        StatusCode::OK,
                        json!({ "status": true, "nonce": random_nonce }).to_string(),
                    ),
                    Err(err) => {
                        tracing::error!("Error setting nonce for address {}: {:?}", address, err);
                        internal_error(anyhow!(err))
                    }
                }
            }
        },
        Err(err) => {
            tracing::error!("Error getting connection from pool: {:?}", err);
            internal_error(anyhow!(err))
        }
    }
}

#[derive(Debug)]
pub struct PrivyAuthHeaders {
    pub bearer_token: String,
    pub connected_wallet: String,
}

#[async_trait]
impl<S> FromRequestParts<S> for PrivyAuthHeaders
where
    S: Send + Sync,
{
    type Rejection = PrivyAuthError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Extract the Authorization header
        let TypedHeader(auth_header) = parts
            .extract::<TypedHeader<Authorization<Bearer>>>()
            .await
            .map_err(|_| {
                PrivyAuthError::MissingHeader("Authorization header missing".to_string())
            })?;

        let full_value = auth_header.token();

        // Split the value using the pipe delimiter
        let mut parts = full_value.split('|');

        // Extract the token and connected wallet
        let bearer_token = parts
            .next()
            .ok_or_else(|| {
                PrivyAuthError::InvalidHeader("Authorization header format invalid".to_string())
            })?
            .to_string();

        let connected_wallet = parts
            .next()
            .ok_or_else(|| {
                PrivyAuthError::InvalidHeader(
                    "Authorization header missing connected-wallet".to_string(),
                )
            })?
            .to_string();

        // Ensure no unexpected extra parts are included
        if parts.next().is_some() {
            return Err(PrivyAuthError::InvalidHeader(
                "Authorization header contains unexpected extra parts".to_string(),
            ));
        }

        Ok(PrivyAuthHeaders {
            bearer_token,
            connected_wallet,
        })
    }
}

pub async fn verify_privy_auth(
    PrivyAuthHeaders {
        bearer_token,
        connected_wallet,
    }: PrivyAuthHeaders,
    state: State<AppState>,
) -> Result<impl IntoResponse, PrivyAuthError> {
    let connected_wallet_address = alloy::primitives::Address::from_str(&connected_wallet)
        .map_err(|e| {
            PrivyAuthError::InvalidAddress(format!("Invalid connected-wallet address: {}", e))
        })?;

    let args = Args::load().await.map_err(|err| {
        let msg = format!("Error loading args: {:?}", err);
        tracing::error!("{}", msg);
        PrivyAuthError::InternalError(msg)
    })?;

    let privy_app_id = String::from(args.privy_app_id);
    let privy_public_key = String::from(args.privy_public_key).replace("\\n", "\n");

    tracing::info!("Preparing to verify privy auth");

    // Create and configure the Validation instance
    let validation = Validation::new(jsonwebtoken::Algorithm::ES256);

    let decoded = jsonwebtoken::decode::<PrivyClaims>(
        &bearer_token,
        &DecodingKey::from_ec_pem(privy_public_key.as_bytes()).map_err(|err| {
            PrivyAuthError::InternalError(format!("Failed to parse EC key: {:?}", err))
        })?,
        &validation,
    )
    .map_err(|err| PrivyAuthError::JwtDecodeError(format!("JWT verification error: {:?}", err)))?;

    let privy_claims = decoded.claims;

    // Validate claims
    privy_claims.valid(&privy_app_id).map_err(|err| {
        PrivyAuthError::ClaimsValidationError(format!("Claims validation error: {:?}", err))
    })?;

    tracing::info!(
        "Verified JWT claims successfully for connected wallet {}",
        connected_wallet
    );

    // Ensure the connected-wallet matches one of the wallet addresses in
    // linked_accounts
    if !privy_claims.links_wallet(&connected_wallet_address) {
        let msg = "connected-wallet does not match any linked accounts".to_string();
        tracing::error!("{}", msg);
        return Err(PrivyAuthError::ClaimsValidationError(msg));
    }

    let claims = Claims {
        address: connected_wallet_address,
        exp: Utc::now().timestamp() + 30 * 24 * 60 * 60, // 30 days
    };

    let token = encode(&Header::default(), &claims, &state.keys.encoding)
        .map_err(|err| PrivyAuthError::InternalError(format!("Error encoding token: {:?}", err)))?;

    Ok((
        StatusCode::OK,
        Json(json!({ "status": true, "token": token })),
    ))
}

#[derive(Debug)]
pub enum PrivyAuthError {
    MissingHeader(String),
    InvalidHeader(String),
    InvalidAddress(String),
    JwtDecodeError(String),
    ClaimsValidationError(String),
    InternalError(String),
}

impl IntoResponse for PrivyAuthError {
    fn into_response(self) -> axum::response::Response {
        let (status, error_message) = match self {
            PrivyAuthError::MissingHeader(msg) => (StatusCode::UNAUTHORIZED, msg),
            PrivyAuthError::InvalidHeader(msg) => (StatusCode::UNAUTHORIZED, msg),
            PrivyAuthError::InvalidAddress(msg) => (StatusCode::UNAUTHORIZED, msg),
            PrivyAuthError::JwtDecodeError(msg) => (StatusCode::UNAUTHORIZED, msg),
            PrivyAuthError::ClaimsValidationError(msg) => (StatusCode::UNAUTHORIZED, msg),
            PrivyAuthError::InternalError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };

        tracing::error!("Error occurred: {}", error_message);

        // Return a JSON error response
        let body = Json(json!({
            "status": false,
            "error": error_message,
        }));
        (status, body).into_response()
    }
}

pub async fn verify_auth(
    state: State<AppState>,
    Json(VerifyAuth { message, signature }): Json<VerifyAuth>,
) -> impl IntoResponse {
    let address = Address::from(message.address);
    let chain_id = message.chain_id;

    tracing::info!("verifying auth for {:?}", address);

    let nonce_key = nonce_key(&address, chain_id);

    match state.pool.get().await {
        Ok(mut conn) => match conn.get::<&str, String>(&nonce_key).await {
            Ok(nonce) => {
                let verify_error = match (
                    message.valid_now(),
                    message.domain == "view.art",
                    message.nonce == nonce,
                ) {
                    (false, _, _) => Err(VerificationError::Time),
                    (_, false, _) => Err(VerificationError::DomainMismatch),
                    (_, _, false) => Err(VerificationError::NonceMismatch),
                    _ => Ok(()),
                };

                if let Err(e) = verify_error {
                    return (
                        StatusCode::BAD_REQUEST,
                        json!({ "status": false, "error": e.to_string() }).to_string(),
                    );
                }

                match verify_signature(
                    signature,
                    address,
                    message.to_string().as_bytes(),
                    state.provider.clone(),
                )
                .await
                {
                    Ok(verification) => {
                        if !verification.is_valid() {
                            tracing::debug!("signature invalid");
                            return (
                                StatusCode::BAD_REQUEST,
                                json!({ "status": false, "error": "signature invalid" })
                                    .to_string(),
                            );
                        }

                        tracing::debug!("signature valid");

                        if let Err(err) = conn.del::<&str, ()>(&nonce_key).await {
                            tracing::error!(
                                "Error deleting nonce for address {}: {:?}",
                                address,
                                err
                            );
                            return internal_error(anyhow!(err));
                        }

                        let claims = Claims {
                            address: Address::from(message.address),
                            // 30 days from now
                            exp: Utc::now().timestamp() + 30 * 24 * 60 * 60,
                        };

                        match encode(&Header::default(), &claims, &state.keys.encoding) {
                            Ok(token) => (
                                StatusCode::OK,
                                json!({ "status": true, "token": token }).to_string(),
                            ),
                            Err(err) => {
                                tracing::error!("Error encoding token: {:?}", err);
                                internal_error(anyhow!(err))
                            }
                        }
                    }
                    Err(err) => {
                        tracing::error!("Error verifying signature: {:#?}", err);
                        internal_error(anyhow!(err))
                    }
                }
            }
            Err(err) => {
                // if the nonce doesn't exist, return an error
                tracing::error!("Error getting nonce for address {}: {:?}", address, err);
                (
                    StatusCode::BAD_REQUEST,
                    json!({ "status": false, "error": "invalid nonce" }).to_string(),
                )
            }
        },
        Err(err) => {
            tracing::error!("Error getting connection from pool: {:?}", err);
            internal_error(anyhow!(err))
        }
    }
}

#[derive(Clone)]
pub struct Keys {
    encoding: EncodingKey,
    decoding: DecodingKey,
}

impl Keys {
    pub fn new(secret: &[u8]) -> Self {
        Self {
            encoding: EncodingKey::from_secret(secret),
            decoding: DecodingKey::from_secret(secret),
        }
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for Claims
where
    S: Send + Sync,
{
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Extract the token from the authorization header
        let TypedHeader(Authorization(bearer)) = parts
            .extract::<TypedHeader<Authorization<Bearer>>>()
            .await
            .map_err(|_| AuthError::InvalidToken)?;

        let Extension(keys) = parts
            .extract::<Extension<Keys>>()
            .await
            .map_err(|_| AuthError::CouldNotAccessState)?;

        // Decode the user data
        let token_data = decode::<Claims>(bearer.token(), &keys.decoding, &Validation::default())
            .map_err(|_| AuthError::InvalidToken)?;

        Ok(token_data.claims)
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            AuthError::InvalidToken => (StatusCode::BAD_REQUEST, "Invalid token"),
            AuthError::CouldNotAccessState => {
                (StatusCode::INTERNAL_SERVER_ERROR, "Could not access state")
            }
        };
        let body = Json(json!({
            "error": error_message,
        }));
        (status, body).into_response()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub address: Address,
    exp: i64,
}

#[derive(Debug)]
pub enum AuthError {
    InvalidToken,
    CouldNotAccessState,
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn get_team_api_key(secret: String) -> String {
        let keys = Keys::new(secret.as_bytes());
        let claims = Claims {
            address: Address::from([0; 20]),
            // 90 days from now
            exp: Utc::now().timestamp() + 90 * 24 * 60 * 60,
        };
        encode(&Header::default(), &claims, &keys.encoding).unwrap()
    }

    /// Privy claims as they appear in an identity token: `linked_accounts` is a
    /// JSON-encoded string.
    fn privy_claims_with_linked_accounts(linked_accounts: serde_json::Value) -> PrivyClaims {
        serde_json::from_value(json!({
            "linked_accounts": linked_accounts.to_string(),
            "iss": "privy.io",
            "iat": 1_700_000_000,
            "aud": "test_app_id",
            "exp": 1_700_003_600,
        }))
        .expect("claims should deserialize")
    }

    #[test]
    /// Passkey (and other non-address) linked accounts have no `address`
    /// field. They must not break auth for the account's wallets.
    fn test_privy_claims_with_passkey_linked_account() {
        let wallet: Address = "0x04e4024eDD54d9870F97DE4040198b4254A7C1f0"
            .parse()
            .unwrap();
        let claims = privy_claims_with_linked_accounts(json!([
            { "type": "email", "address": "someone@example.com" },
            {
                "type": "wallet",
                "address": "0x04e4024eDD54d9870F97DE4040198b4254A7C1f0",
                "chain_type": "ethereum",
                "wallet_client_type": "privy"
            },
            {
                "type": "passkey",
                "credential_id": "abc123",
                "authenticator_name": "iCloud Keychain",
                "enrolled_in_mfa": false
            }
        ]));

        assert_eq!(claims.linked_accounts.len(), 3);
        assert!(claims.links_wallet(&wallet));
    }

    #[test]
    /// Only `wallet` accounts count: an email or passkey never matches, and a
    /// wallet that isn't linked doesn't either.
    fn test_privy_claims_links_wallet_only_for_linked_wallets() {
        let other: Address = "0x0000000000000000000000000000000000000001"
            .parse()
            .unwrap();
        let claims = privy_claims_with_linked_accounts(json!([
            { "type": "email", "address": "0x0000000000000000000000000000000000000001" },
            { "type": "passkey", "credential_id": "abc123" },
            { "type": "wallet", "address": "0x04e4024eDD54d9870F97DE4040198b4254A7C1f0" }
        ]));

        assert!(!claims.links_wallet(&other));
    }

    #[ignore]
    #[test]
    /// This test can be used to generate a token for the team API key
    /// You must set JWT_SECRET to the production secret and then run:
    /// cargo test test_generate_claim_for_0x0 -- --ignored --nocapture
    fn test_generate_claim_for_0x0() {
        let key = std::env::var("JWT_SECRET").expect("JWT_SECRET must be set");
        let token = get_team_api_key(key);

        dbg!(token);
    }
}
