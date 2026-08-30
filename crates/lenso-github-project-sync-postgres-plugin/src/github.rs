use std::collections::BTreeMap;

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use lenso::prelude::Ctx;
use lenso_capability_http_client as http;
use lenso_capability_secrets as secrets;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use time::OffsetDateTime;
use zeroize::Zeroizing;

use crate::storage::{Mapping, Settings};

const ACCEPT: &str = "application/vnd.github+json";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GithubFailure {
    pub code: &'static str,
    pub retryable: bool,
}

impl GithubFailure {
    fn fatal(code: &'static str) -> Self {
        Self {
            code,
            retryable: false,
        }
    }
    fn retry(code: &'static str) -> Self {
        Self {
            code,
            retryable: true,
        }
    }
}

#[derive(Debug, Serialize)]
struct AppClaims<'a> {
    iss: &'a str,
    iat: i64,
    exp: i64,
}

#[derive(Debug, Deserialize)]
struct InstallationTokenResponse {
    token: String,
}

pub(crate) struct GithubApi<'a> {
    http: &'a http::ClientClient,
    secrets: &'a secrets::SecretsClient,
    context: &'a Ctx,
    api_origin: &'a str,
    api_version: &'a str,
}

impl<'a> GithubApi<'a> {
    pub fn new(
        http: &'a http::ClientClient,
        secrets: &'a secrets::SecretsClient,
        context: &'a Ctx,
        api_origin: &'a str,
        api_version: &'a str,
    ) -> Self {
        Self {
            http,
            secrets,
            context,
            api_origin,
            api_version,
        }
    }

    async fn resolve(&self, reference: &str) -> Result<Zeroizing<String>, GithubFailure> {
        self.secrets
            .resolve_with_context(
                self.context.clone(),
                secrets::ResolveRequest {
                    reference: reference.to_owned(),
                },
            )
            .await
            .map(|response| Zeroizing::new(response.value))
            .map_err(|error| match error {
                secrets::SecretsInvocationError::Domain(_) => {
                    GithubFailure::fatal("secret_rejected")
                }
                secrets::SecretsInvocationError::Runtime(_) => {
                    GithubFailure::retry("secrets_unavailable")
                }
            })
    }

    async fn installation_token(
        &self,
        settings: &Settings,
        mapping: &Mapping,
    ) -> Result<Zeroizing<String>, GithubFailure> {
        let private_key = self.resolve(&settings.private_key_secret_ref).await?;
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let key = EncodingKey::from_rsa_pem(private_key.as_bytes())
            .map_err(|_| GithubFailure::fatal("invalid_app_private_key"))?;
        let jwt = Zeroizing::new(
            encode(
                &Header::new(Algorithm::RS256),
                &AppClaims {
                    iss: &settings.app_id,
                    iat: now - 60,
                    exp: now + 540,
                },
                &key,
            )
            .map_err(|_| GithubFailure::fatal("app_jwt_failed"))?,
        );
        let repository_id = mapping
            .github_repository_id
            .parse::<u64>()
            .map_err(|_| GithubFailure::fatal("invalid_repository_id"))?;
        let mut permissions = json!({"issues":"write","metadata":"read"});
        if mapping.github_project_id.is_some() {
            permissions["organization_projects"] = Value::String("write".to_owned());
        }
        let body = json!({"repository_ids":[repository_id],"permissions":permissions});
        let response = self
            .send_raw(
                "POST",
                &format!(
                    "/app/installations/{}/access_tokens",
                    mapping.installation_id
                ),
                Some(jwt.as_str()),
                Some(&body),
            )
            .await?;
        if response.status != 201 {
            return Err(classify_status(response.status, &response.headers));
        }
        let parsed: InstallationTokenResponse = serde_json::from_slice(&response.body)
            .map_err(|_| GithubFailure::retry("invalid_token_response"))?;
        Ok(Zeroizing::new(parsed.token))
    }

    pub async fn rest(
        &self,
        settings: &Settings,
        mapping: &Mapping,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<ApiResponse, GithubFailure> {
        let token = self.installation_token(settings, mapping).await?;
        self.send_raw(method, path, Some(token.as_str()), body)
            .await
    }

    pub async fn graphql(
        &self,
        settings: &Settings,
        mapping: &Mapping,
        query: &str,
        variables: Value,
    ) -> Result<Value, GithubFailure> {
        let response = self
            .rest(
                settings,
                mapping,
                "POST",
                "/graphql",
                Some(&json!({"query":query,"variables":variables})),
            )
            .await?;
        if !(200..300).contains(&response.status) {
            return Err(classify_status(response.status, &response.headers));
        }
        let value: Value = serde_json::from_slice(&response.body)
            .map_err(|_| GithubFailure::retry("invalid_graphql_response"))?;
        if value
            .get("errors")
            .is_some_and(|errors| errors.as_array().is_some_and(|items| !items.is_empty()))
        {
            return Err(GithubFailure::fatal("github_graphql_error"));
        }
        Ok(value)
    }

    async fn send_raw(
        &self,
        method: &str,
        path: &str,
        bearer: Option<&str>,
        body: Option<&Value>,
    ) -> Result<ApiResponse, GithubFailure> {
        if !path.starts_with('/') || path.starts_with("//") || path.contains("..") {
            return Err(GithubFailure::fatal("invalid_github_path"));
        }
        let mut headers = vec![
            http::SendRequestHeadersItem {
                name: "accept".to_owned(),
                value: ACCEPT.to_owned(),
            },
            http::SendRequestHeadersItem {
                name: "x-github-api-version".to_owned(),
                value: self.api_version.to_owned(),
            },
            http::SendRequestHeadersItem {
                name: "user-agent".to_owned(),
                value: "lenso-github-project-sync/0.1".to_owned(),
            },
        ];
        if let Some(value) = bearer {
            headers.push(http::SendRequestHeadersItem {
                name: "authorization".to_owned(),
                value: format!("Bearer {value}"),
            });
        }
        let bytes = if let Some(value) = body {
            headers.push(http::SendRequestHeadersItem {
                name: "content-type".to_owned(),
                value: "application/json".to_owned(),
            });
            serde_json::to_vec(value)
                .map_err(|_| GithubFailure::fatal("serialize_github_request"))?
        } else {
            Vec::new()
        };
        let response = self
            .http
            .send_with_context(
                self.context.clone(),
                http::SendRequest {
                    body: bytes.into(),
                    headers,
                    method: method.to_owned(),
                    url: format!("{}{path}", self.api_origin),
                },
            )
            .await
            .map_err(|error| match error {
                http::ClientInvocationError::Domain(
                    http::SendError::DestinationNotAllowed | http::SendError::InvalidRequest,
                ) => GithubFailure::fatal("github_destination_rejected"),
                http::ClientInvocationError::Domain(_)
                | http::ClientInvocationError::Runtime(_) => {
                    GithubFailure::retry("github_transport_failure")
                }
            })?;
        let headers = response
            .headers
            .into_iter()
            .map(|item| (item.name.to_ascii_lowercase(), item.value))
            .collect();
        Ok(ApiResponse {
            status: response.status,
            headers,
            body: response.body.into_vec(),
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ApiResponse {
    pub status: i64,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

pub(crate) fn classify_status(status: i64, headers: &BTreeMap<String, String>) -> GithubFailure {
    if status == 429
        || status >= 500
        || (status == 403
            && (headers.contains_key("retry-after")
                || headers
                    .get("x-ratelimit-remaining")
                    .is_some_and(|value| value == "0")))
    {
        GithubFailure::retry("github_retryable_status")
    } else if status == 401 {
        GithubFailure::retry("github_token_rejected")
    } else {
        GithubFailure::fatal("github_request_rejected")
    }
}

pub(crate) fn response_json(
    response: &ApiResponse,
    expected: &[i64],
) -> Result<Value, GithubFailure> {
    if !expected.contains(&response.status) {
        return Err(classify_status(response.status, &response.headers));
    }
    if response.body.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice(&response.body)
        .map_err(|_| GithubFailure::retry("invalid_github_response"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limit_and_server_failures_are_retryable() {
        assert!(classify_status(429, &BTreeMap::new()).retryable);
        assert!(classify_status(503, &BTreeMap::new()).retryable);
        assert!(!classify_status(422, &BTreeMap::new()).retryable);
        assert!(
            classify_status(
                403,
                &BTreeMap::from([("x-ratelimit-remaining".to_owned(), "0".to_owned())])
            )
            .retryable
        );
    }
}
