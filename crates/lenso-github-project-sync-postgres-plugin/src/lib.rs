//! Durable bidirectional GitHub Issues / Projects synchronization.

#![allow(clippy::too_many_lines)]

mod engine;
mod github;
mod operator;
mod schema;
mod storage;
mod webhook;

use std::{cell::RefCell, collections::BTreeSet, fmt, rc::Rc};

use lenso::prelude::*;
use lenso_capability_github_project_sync as sync;
use lenso_capability_github_project_sync_admin as admin;
use lenso_capability_http_client as http;
use lenso_capability_projects as projects;
use lenso_capability_projects_collaboration as collaboration;
use lenso_capability_secrets as secrets;
use lenso_kernel::RuntimeFailure;
use lenso_postgres_kit::OwnedPostgres;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zeroize::Zeroizing;

pub use operator::{GithubProjectSyncOperator, OperatorError};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GithubProjectSyncConfig {
    schema: String,
    database_url_secret: String,
    app_id: String,
    private_key_secret_ref: String,
    webhook_secret_ref: String,
    github_api_origin: String,
    github_api_version: String,
    allowed_github_origins: Vec<String>,
    max_webhook_body_bytes: usize,
    webhook_callers: Vec<String>,
    reader_callers: Vec<String>,
    admin_callers: Vec<String>,
    worker_callers: Vec<String>,
}

impl GithubProjectSyncConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        schema::schema_plan(self.schema.clone()).map_err(|_| ConfigError::InvalidSchema)?;
        if !valid_secret_ref(&self.database_url_secret)
            || !valid_secret_ref(&self.private_key_secret_ref)
            || !valid_secret_ref(&self.webhook_secret_ref)
        {
            return Err(ConfigError::InvalidSecretReference);
        }
        if !valid_positive_decimal(&self.app_id) {
            return Err(ConfigError::InvalidAppId);
        }
        if !valid_origin(&self.github_api_origin)
            || !self
                .allowed_github_origins
                .iter()
                .all(|origin| valid_origin(origin))
            || !self
                .allowed_github_origins
                .contains(&self.github_api_origin)
        {
            return Err(ConfigError::InvalidGithubOrigin);
        }
        if self.github_api_version.len() > 32
            || self.github_api_version.is_empty()
            || !self
                .github_api_version
                .bytes()
                .all(|byte| byte.is_ascii_digit() || byte == b'-')
        {
            return Err(ConfigError::InvalidApiVersion);
        }
        if !(1024..=16 * 1024 * 1024).contains(&self.max_webhook_body_bytes) {
            return Err(ConfigError::InvalidBodyLimit);
        }
        for callers in [
            &self.webhook_callers,
            &self.reader_callers,
            &self.admin_callers,
            &self.worker_callers,
        ] {
            if !valid_callers(callers) {
                return Err(ConfigError::InvalidCallers);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ConfigError {
    #[error("invalid owned PostgreSQL schema")]
    InvalidSchema,
    #[error("invalid secret reference")]
    InvalidSecretReference,
    #[error("GitHub App ID must be a positive decimal integer")]
    InvalidAppId,
    #[error("GitHub API origin must be an exact allowlisted HTTPS origin")]
    InvalidGithubOrigin,
    #[error("invalid GitHub API version")]
    InvalidApiVersion,
    #[error("invalid webhook body limit")]
    InvalidBodyLimit,
    #[error("caller allowlists must contain 1 to 64 unique Instance keys")]
    InvalidCallers,
}

fn validate_config(config: &GithubProjectSyncConfig) -> Result<(), RuntimeFailure> {
    config
        .validate()
        .map_err(|error| RuntimeFailure::InvalidResolvedPlan {
            detail: format!("GitHub Project Sync configuration is invalid: {error}"),
        })
}
fn valid_origin(value: &str) -> bool {
    value.starts_with("https://")
        && !value[8..].is_empty()
        && !value[8..].contains('/')
        && !value.contains('@')
        && !value.contains('#')
        && !value.contains('?')
}
fn valid_secret_ref(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_whitespace)
}
fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 200
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
}
fn valid_positive_decimal(value: &str) -> bool {
    matches!(value.parse::<u64>(), Ok(number) if number > 0)
}
fn valid_callers(values: &[String]) -> bool {
    !values.is_empty()
        && values.len() <= 64
        && values.iter().all(|value| valid_id(value))
        && values.iter().collect::<BTreeSet<_>>().len() == values.len()
}
fn valid_repo_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 100
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

#[derive(Clone, Debug)]
struct Prepared {
    postgres: OwnedPostgres,
}

#[lenso::plugin(lifecycle,configuration_schema="configuration.schema.json",validate=validate_config)]
#[derive(Clone)]
struct GithubProjectSyncPlugin {
    #[config]
    config: GithubProjectSyncConfig,
    secrets: Port<secrets::SecretsClient>,
    http: Port<http::ClientClient>,
    projects: Port<projects::ProjectsClient>,
    collaboration: Port<collaboration::ProjectsCollaborationClient>,
    prepared: Rc<RefCell<Option<Prepared>>>,
}

impl fmt::Debug for GithubProjectSyncPlugin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GithubProjectSyncPlugin")
            .field("schema", &self.config.schema)
            .field("github_api_origin", &self.config.github_api_origin)
            .field("prepared", &self.prepared.borrow().is_some())
            .finish_non_exhaustive()
    }
}

#[lenso::provides(sync::GithubProjectSync, admin::GithubProjectSyncAdmin)]
impl GithubProjectSyncPlugin {}

impl GithubProjectSyncPlugin {
    fn prepared(&self) -> Result<Prepared, RuntimeFailure> {
        self.prepared
            .borrow()
            .clone()
            .ok_or_else(|| RuntimeFailure::PluginFailure {
                detail: "GitHub Project Sync Plugin is not active".to_owned(),
            })
    }
    fn allowed(context: &Ctx, callers: &[String]) -> bool {
        context
            .caller_instance()
            .is_some_and(|caller| callers.iter().any(|allowed| allowed == caller))
    }
    async fn resolve_secret(
        &self,
        context: &Ctx,
        reference: &str,
    ) -> Result<Zeroizing<String>, RuntimeFailure> {
        self.secrets
            .resolve_with_context(
                context.clone(),
                secrets::ResolveRequest {
                    reference: reference.to_owned(),
                },
            )
            .await
            .map(|response| Zeroizing::new(response.value))
            .map_err(|error| match error {
                secrets::SecretsInvocationError::Runtime(error) => error,
                secrets::SecretsInvocationError::Domain(_) => RuntimeFailure::PluginFailure {
                    detail: format!(
                        "required GitHub Project Sync secret `{reference}` was rejected"
                    ),
                },
            })
    }
    async fn verify_webhook(
        &self,
        context: &Ctx,
        signature: &str,
        body: &[u8],
    ) -> Result<bool, RuntimeFailure> {
        let prepared = self.prepared()?;
        let settings = storage::settings(&prepared.postgres)
            .await
            .map_err(storage_runtime)?;
        let current = self
            .resolve_secret(context, &settings.webhook_secret_ref)
            .await?;
        if webhook::verify_signature(current.as_bytes(), body, signature) {
            return Ok(true);
        }
        if let Some(previous) = settings.previous_webhook_secret_ref {
            let previous = self.resolve_secret(context, &previous).await?;
            return Ok(webhook::verify_signature(
                previous.as_bytes(),
                body,
                signature,
            ));
        }
        Ok(false)
    }
}

fn storage_runtime(error: storage::StorageError) -> RuntimeFailure {
    match error {
        storage::StorageError::Runtime(error) => error,
        storage::StorageError::Domain(value) => RuntimeFailure::PluginFailure {
            detail: format!("GitHub Project Sync state conflict: {value:?}"),
        },
    }
}

macro_rules! map_admin_storage {
    ($result:expr,$error:ident) => {
        match $result {
            Ok(value) => Ok(value),
            Err(storage::StorageError::Runtime(error)) => Err(PluginError::runtime(error)),
            Err(storage::StorageError::Domain(storage::DomainFailure::NotFound)) => {
                Err(PluginError::domain(admin::$error::NotFound))
            }
            Err(storage::StorageError::Domain(storage::DomainFailure::RevisionConflict)) => {
                Err(PluginError::domain(admin::$error::RevisionConflict))
            }
            Err(storage::StorageError::Domain(storage::DomainFailure::InstallationInactive)) => {
                Err(PluginError::domain(admin::$error::InstallationInactive))
            }
            Err(storage::StorageError::Domain(storage::DomainFailure::MappingInactive)) => {
                Err(PluginError::domain(admin::$error::MappingInactive))
            }
            Err(storage::StorageError::Domain(storage::DomainFailure::LeaseLost)) => {
                Err(PluginError::domain(admin::$error::LeaseLost))
            }
            Err(storage::StorageError::Domain(_)) => {
                Err(PluginError::domain(admin::$error::InvalidRequest))
            }
        }
    };
}

impl GithubProjectSyncPlugin {
    async fn ingest_webhook(
        &self,
        context: Ctx,
        request: sync::IngestWebhookRequest,
    ) -> PluginResult<sync::IngestWebhookResponse, sync::IngestWebhookError> {
        if !Self::allowed(&context, &self.config.webhook_callers) {
            return Err(PluginError::domain(sync::IngestWebhookError::Forbidden));
        }
        if !valid_id(&request.delivery_id)
            || !valid_id(&request.event)
            || request.body.as_slice().len() > self.config.max_webhook_body_bytes
            || request.signature_256.len() != 71
        {
            return Err(PluginError::domain(
                sync::IngestWebhookError::InvalidRequest,
            ));
        }
        if !self
            .verify_webhook(&context, &request.signature_256, request.body.as_slice())
            .await
            .map_err(PluginError::runtime)?
        {
            return Err(PluginError::domain(
                sync::IngestWebhookError::InvalidSignature,
            ));
        }
        let hash = webhook::payload_sha256(request.body.as_slice());
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        match storage::insert_delivery(
            &prepared.postgres,
            &request.delivery_id,
            &request.event,
            request.body.as_slice(),
            &hash,
        )
        .await
        {
            Ok(value) => Ok(value),
            Err(storage::StorageError::Domain(storage::DomainFailure::DeliveryConflict)) => Err(
                PluginError::domain(sync::IngestWebhookError::DeliveryConflict),
            ),
            Err(error) => Err(PluginError::runtime(storage_runtime(error))),
        }
    }
    async fn get_binding(
        &self,
        context: Ctx,
        request: sync::GetBindingRequest,
    ) -> PluginResult<sync::GetBindingResponse, sync::GetBindingError> {
        if !Self::allowed(&context, &self.config.reader_callers) {
            return Err(PluginError::domain(sync::GetBindingError::Forbidden));
        }
        if !valid_id(&request.mapping_id) || request.github_issue_number < 1 {
            return Err(PluginError::domain(sync::GetBindingError::InvalidRequest));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        match storage::get_binding(&prepared.postgres, &request).await {
            Ok(value) => Ok(value),
            Err(storage::StorageError::Domain(storage::DomainFailure::NotFound)) => {
                Err(PluginError::domain(sync::GetBindingError::NotFound))
            }
            Err(error) => Err(PluginError::runtime(storage_runtime(error))),
        }
    }
    async fn list_bindings(
        &self,
        context: Ctx,
        request: sync::ListBindingsRequest,
    ) -> PluginResult<sync::ListBindingsResponse, sync::ListBindingsError> {
        if !Self::allowed(&context, &self.config.reader_callers) {
            return Err(PluginError::domain(sync::ListBindingsError::Forbidden));
        }
        if !valid_id(&request.mapping_id) || !(1..=100).contains(&request.limit) {
            return Err(PluginError::domain(sync::ListBindingsError::InvalidRequest));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        match storage::list_bindings(&prepared.postgres, &request).await {
            Ok(value) => Ok(value),
            Err(storage::StorageError::Domain(_)) => {
                Err(PluginError::domain(sync::ListBindingsError::InvalidRequest))
            }
            Err(storage::StorageError::Runtime(error)) => Err(PluginError::runtime(error)),
        }
    }
}

fn valid_mapping(request: &admin::PutMappingRequest) -> bool {
    if [
        request.mapping_id.as_str(),
        request.installation_id.as_str(),
        request.github_repository_id.as_str(),
        request.organization_id.as_str(),
        request.team_id.as_str(),
        request.project_id.as_str(),
    ]
    .into_iter()
    .any(|value| !valid_id(value))
        || !valid_positive_decimal(&request.github_repository_id)
        || !valid_repo_component(&request.github_owner)
        || !valid_repo_component(&request.github_repository)
        || request.github_project_id.is_some() != request.github_project_status_field_id.is_some()
        || request
            .expected_revision
            .as_deref()
            .is_some_and(|value| storage::parse_revision(value).is_err())
        || request.state_mappings.is_empty()
    {
        return false;
    }
    let mut states = BTreeSet::new();
    let mut open_defaults = 0;
    let mut closed_defaults = 0;
    for item in &request.state_mappings {
        if !valid_id(&item.workflow_state_id) || !states.insert(item.workflow_state_id.as_str()) {
            return false;
        }
        if item
            .github_project_option_id
            .as_ref()
            .is_some_and(|value| !valid_id(value))
        {
            return false;
        }
        if item.inbound_default {
            match item.github_issue_state {
                admin::PutMappingRequestStateMappingsItemGithubIssueState::Open => {
                    open_defaults += 1;
                }
                admin::PutMappingRequestStateMappingsItemGithubIssueState::Closed => {
                    closed_defaults += 1;
                }
            }
        }
    }
    if open_defaults != 1 || closed_defaults != 1 {
        return false;
    }
    let mut lenso_labels = BTreeSet::new();
    let mut github_labels = BTreeSet::new();
    for item in &request.label_mappings {
        if !valid_id(&item.lenso_label_id)
            || item.github_label.is_empty()
            || item.github_label.len() > 50
            || !lenso_labels.insert(item.lenso_label_id.as_str())
            || !github_labels.insert(item.github_label.to_ascii_lowercase())
        {
            return false;
        }
    }
    let mut lenso_milestones = BTreeSet::new();
    let mut github_milestones = BTreeSet::new();
    for item in &request.milestone_mappings {
        if !valid_id(&item.lenso_milestone_id)
            || item.github_milestone_number < 1
            || !lenso_milestones.insert(item.lenso_milestone_id.as_str())
            || !github_milestones.insert(item.github_milestone_number)
        {
            return false;
        }
    }
    true
}

impl GithubProjectSyncPlugin {
    async fn set_app_credentials(
        &self,
        context: Ctx,
        request: admin::SetAppCredentialsRequest,
    ) -> PluginResult<admin::SetAppCredentialsResponse, admin::SetAppCredentialsError> {
        if !Self::allowed(&context, &self.config.admin_callers) {
            return Err(PluginError::domain(
                admin::SetAppCredentialsError::Forbidden,
            ));
        }
        if !valid_positive_decimal(&request.app_id)
            || !valid_secret_ref(&request.private_key_secret_ref)
            || request
                .expected_revision
                .as_deref()
                .is_some_and(|value| storage::parse_revision(value).is_err())
        {
            return Err(PluginError::domain(
                admin::SetAppCredentialsError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_admin_storage!(
            storage::set_app_credentials(&prepared.postgres, &request).await,
            SetAppCredentialsError
        )
    }
    async fn set_webhook_secrets(
        &self,
        context: Ctx,
        request: admin::SetWebhookSecretsRequest,
    ) -> PluginResult<admin::SetWebhookSecretsResponse, admin::SetWebhookSecretsError> {
        if !Self::allowed(&context, &self.config.admin_callers) {
            return Err(PluginError::domain(
                admin::SetWebhookSecretsError::Forbidden,
            ));
        }
        if !valid_secret_ref(&request.current_secret_ref)
            || request.previous_secret_ref.as_ref().is_some_and(|value| {
                !valid_secret_ref(value) || value == &request.current_secret_ref
            })
            || request
                .expected_revision
                .as_deref()
                .is_some_and(|value| storage::parse_revision(value).is_err())
        {
            return Err(PluginError::domain(
                admin::SetWebhookSecretsError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_admin_storage!(
            storage::set_webhook_secrets(&prepared.postgres, &request).await,
            SetWebhookSecretsError
        )
    }
    async fn put_installation(
        &self,
        context: Ctx,
        request: admin::PutInstallationRequest,
    ) -> PluginResult<admin::PutInstallationResponse, admin::PutInstallationError> {
        if !Self::allowed(&context, &self.config.admin_callers) {
            return Err(PluginError::domain(admin::PutInstallationError::Forbidden));
        }
        if !valid_positive_decimal(&request.installation_id)
            || !valid_positive_decimal(&request.account_id)
            || !valid_repo_component(&request.account_login)
            || request
                .expected_revision
                .as_deref()
                .is_some_and(|value| storage::parse_revision(value).is_err())
        {
            return Err(PluginError::domain(
                admin::PutInstallationError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_admin_storage!(
            storage::put_installation(&prepared.postgres, &request).await,
            PutInstallationError
        )
    }
    async fn revoke_installation(
        &self,
        context: Ctx,
        request: admin::RevokeInstallationRequest,
    ) -> PluginResult<admin::RevokeInstallationResponse, admin::RevokeInstallationError> {
        if !Self::allowed(&context, &self.config.admin_callers) {
            return Err(PluginError::domain(
                admin::RevokeInstallationError::Forbidden,
            ));
        }
        if request.installation_id.parse::<u64>().is_err()
            || storage::parse_revision(&request.expected_revision).is_err()
        {
            return Err(PluginError::domain(
                admin::RevokeInstallationError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_admin_storage!(
            storage::revoke_installation(&prepared.postgres, &request).await,
            RevokeInstallationError
        )
    }
    async fn put_mapping(
        &self,
        context: Ctx,
        request: admin::PutMappingRequest,
    ) -> PluginResult<admin::PutMappingResponse, admin::PutMappingError> {
        if !Self::allowed(&context, &self.config.admin_callers) {
            return Err(PluginError::domain(admin::PutMappingError::Forbidden));
        }
        if !valid_mapping(&request) {
            return Err(PluginError::domain(admin::PutMappingError::InvalidRequest));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_admin_storage!(
            storage::put_mapping(&prepared.postgres, &request).await,
            PutMappingError
        )
    }
    async fn delete_mapping(
        &self,
        context: Ctx,
        request: admin::DeleteMappingRequest,
    ) -> PluginResult<admin::DeleteMappingResponse, admin::DeleteMappingError> {
        if !Self::allowed(&context, &self.config.admin_callers) {
            return Err(PluginError::domain(admin::DeleteMappingError::Forbidden));
        }
        if !valid_id(&request.mapping_id)
            || storage::parse_revision(&request.expected_revision).is_err()
        {
            return Err(PluginError::domain(
                admin::DeleteMappingError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_admin_storage!(
            storage::delete_mapping(&prepared.postgres, &request).await,
            DeleteMappingError
        )
    }
    async fn list_mappings(
        &self,
        context: Ctx,
        request: admin::ListMappingsRequest,
    ) -> PluginResult<admin::ListMappingsResponse, admin::ListMappingsError> {
        if !Self::allowed(&context, &self.config.admin_callers) {
            return Err(PluginError::domain(admin::ListMappingsError::Forbidden));
        }
        if !(1..=100).contains(&request.limit)
            || request
                .organization_id
                .as_ref()
                .is_some_and(|value| !valid_id(value))
        {
            return Err(PluginError::domain(
                admin::ListMappingsError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_admin_storage!(
            storage::list_mappings(&prepared.postgres, &request).await,
            ListMappingsError
        )
    }
    async fn inspect_delivery(
        &self,
        context: Ctx,
        request: admin::InspectDeliveryRequest,
    ) -> PluginResult<admin::InspectDeliveryResponse, admin::InspectDeliveryError> {
        if !Self::allowed(&context, &self.config.admin_callers) {
            return Err(PluginError::domain(admin::InspectDeliveryError::Forbidden));
        }
        if !valid_id(&request.delivery_id) {
            return Err(PluginError::domain(
                admin::InspectDeliveryError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_admin_storage!(
            storage::inspect_delivery(&prepared.postgres, &request).await,
            InspectDeliveryError
        )
    }
    async fn list_dead_letters(
        &self,
        context: Ctx,
        request: admin::ListDeadLettersRequest,
    ) -> PluginResult<admin::ListDeadLettersResponse, admin::ListDeadLettersError> {
        if !Self::allowed(&context, &self.config.admin_callers) {
            return Err(PluginError::domain(admin::ListDeadLettersError::Forbidden));
        }
        if !(1..=100).contains(&request.limit) {
            return Err(PluginError::domain(
                admin::ListDeadLettersError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_admin_storage!(
            storage::list_dead_letters(&prepared.postgres, &request).await,
            ListDeadLettersError
        )
    }
    async fn replay_dead_letter(
        &self,
        context: Ctx,
        request: admin::ReplayDeadLetterRequest,
    ) -> PluginResult<admin::ReplayDeadLetterResponse, admin::ReplayDeadLetterError> {
        if !Self::allowed(&context, &self.config.admin_callers) {
            return Err(PluginError::domain(admin::ReplayDeadLetterError::Forbidden));
        }
        if !valid_id(&request.item_id) {
            return Err(PluginError::domain(
                admin::ReplayDeadLetterError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_admin_storage!(
            storage::replay_dead_letter(&prepared.postgres, &request).await,
            ReplayDeadLetterError
        )
    }
    async fn get_checkpoint(
        &self,
        context: Ctx,
        request: admin::GetCheckpointRequest,
    ) -> PluginResult<admin::GetCheckpointResponse, admin::GetCheckpointError> {
        if !Self::allowed(&context, &self.config.admin_callers) {
            return Err(PluginError::domain(admin::GetCheckpointError::Forbidden));
        }
        if !valid_id(&request.mapping_id) {
            return Err(PluginError::domain(
                admin::GetCheckpointError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_admin_storage!(
            storage::get_checkpoint(&prepared.postgres, &request).await,
            GetCheckpointError
        )
    }
    async fn run_worker(
        &self,
        context: Ctx,
        request: admin::RunWorkerRequest,
    ) -> PluginResult<admin::RunWorkerResponse, admin::RunWorkerError> {
        if !Self::allowed(&context, &self.config.worker_callers) {
            return Err(PluginError::domain(admin::RunWorkerError::Forbidden));
        }
        if !valid_id(&request.worker_id)
            || !(1..=100).contains(&request.limit)
            || !(5..=300).contains(&request.lease_seconds)
        {
            return Err(PluginError::domain(admin::RunWorkerError::InvalidRequest));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        let settings = storage::settings(&prepared.postgres)
            .await
            .map_err(storage_runtime)
            .map_err(PluginError::runtime)?;
        let api = github::GithubApi::new(
            &self.http,
            &self.secrets,
            &context,
            &self.config.github_api_origin,
            &self.config.github_api_version,
        );
        let deliveries = storage::claim_deliveries(
            &prepared.postgres,
            &request.worker_id,
            request.limit,
            request.lease_seconds,
        )
        .await
        .map_err(storage_runtime)
        .map_err(PluginError::runtime)?;
        let mut response = admin::RunWorkerResponse {
            activities_staged: 0,
            deliveries_claimed: i64::try_from(deliveries.len()).unwrap_or(i64::MAX),
            deliveries_dead: 0,
            deliveries_retried: 0,
            deliveries_succeeded: 0,
            outbound_claimed: 0,
            outbound_dead: 0,
            outbound_retried: 0,
            outbound_succeeded: 0,
        };
        for claim in &deliveries {
            match engine::process_delivery(
                &prepared.postgres,
                &self.projects,
                &self.collaboration,
                &api,
                &settings,
                &context,
                claim,
            )
            .await
            {
                Ok(()) => {
                    storage::succeed_delivery(&prepared.postgres, claim, &request.worker_id)
                        .await
                        .map_err(storage_runtime)
                        .map_err(PluginError::runtime)?;
                    response.deliveries_succeeded += 1;
                }
                Err(failure) => {
                    let dead = storage::fail_delivery(
                        &prepared.postgres,
                        claim,
                        &request.worker_id,
                        failure.code,
                        failure.retryable,
                    )
                    .await
                    .map_err(storage_runtime)
                    .map_err(PluginError::runtime)?;
                    if dead {
                        response.deliveries_dead += 1;
                    } else {
                        response.deliveries_retried += 1;
                    }
                }
            }
        }
        response.activities_staged = engine::stage_project_activity(
            &prepared.postgres,
            &self.projects,
            &context,
            request.limit,
        )
        .await
        .map_err(|failure| {
            PluginError::domain(if failure.retryable {
                admin::RunWorkerError::RetryableFailure
            } else {
                admin::RunWorkerError::DependencyFailure
            })
        })?;
        let jobs = storage::claim_outbound_jobs(
            &prepared.postgres,
            &request.worker_id,
            request.limit,
            request.lease_seconds,
        )
        .await
        .map_err(storage_runtime)
        .map_err(PluginError::runtime)?;
        response.outbound_claimed = i64::try_from(jobs.len()).unwrap_or(i64::MAX);
        for job in &jobs {
            match engine::process_outbound_job(
                &prepared.postgres,
                &self.projects,
                &self.collaboration,
                &api,
                &settings,
                &context,
                job,
            )
            .await
            {
                Ok(receipt) => {
                    storage::succeed_job(&prepared.postgres, job, &request.worker_id, &receipt)
                        .await
                        .map_err(storage_runtime)
                        .map_err(PluginError::runtime)?;
                    response.outbound_succeeded += 1;
                }
                Err(failure)
                    if matches!(failure.code, "semantic_not_mapped" | "comment_not_exported") =>
                {
                    storage::suppress_job(
                        &prepared.postgres,
                        job,
                        &request.worker_id,
                        failure.code,
                    )
                    .await
                    .map_err(storage_runtime)
                    .map_err(PluginError::runtime)?;
                    response.outbound_succeeded += 1;
                }
                Err(failure) => {
                    let dead = storage::fail_job(
                        &prepared.postgres,
                        job,
                        &request.worker_id,
                        failure.code,
                        failure.retryable,
                    )
                    .await
                    .map_err(storage_runtime)
                    .map_err(PluginError::runtime)?;
                    if dead {
                        response.outbound_dead += 1;
                    } else {
                        response.outbound_retried += 1;
                    }
                }
            }
        }
        Ok(response)
    }
}

impl Lifecycle for GithubProjectSyncPlugin {
    async fn activate(&self, context: ActivateContext) -> Result<(), RuntimeFailure> {
        let database_url = resolve_activation_secret(
            &self.secrets,
            context.dependencies(),
            context.cancellation(),
            &self.config.database_url_secret,
        )
        .await?;
        let postgres = OwnedPostgres::prepare(
            &database_url,
            schema::schema_plan(self.config.schema.clone()).map_err(|error| {
                RuntimeFailure::InvalidResolvedPlan {
                    detail: error.to_string(),
                }
            })?,
        )
        .await
        .map_err(|error| RuntimeFailure::PluginFailure {
            detail: error.to_string(),
        })?;
        storage::initialize_settings(
            &postgres,
            &self.config.app_id,
            &self.config.private_key_secret_ref,
            &self.config.webhook_secret_ref,
        )
        .await
        .map_err(storage_runtime)?;
        self.prepared.borrow_mut().replace(Prepared { postgres });
        Ok(())
    }
    async fn deactivate(&self, _context: DeactivateContext) -> Result<(), RuntimeFailure> {
        let prepared = self.prepared.borrow_mut().take();
        if let Some(prepared) = prepared {
            prepared.postgres.pool().close().await;
        }
        Ok(())
    }
}

async fn resolve_activation_secret(
    secrets: &secrets::SecretsClient,
    dependencies: &lenso_kernel::PluginDependencies,
    cancellation: lenso_kernel::CancellationToken,
    reference: &str,
) -> Result<Zeroizing<String>, RuntimeFailure> {
    let context =
        dependencies.invocation_context_after(std::time::Duration::from_secs(10), cancellation)?;
    secrets
        .resolve_with_context(
            context,
            secrets::ResolveRequest {
                reference: reference.to_owned(),
            },
        )
        .await
        .map(|response| Zeroizing::new(response.value))
        .map_err(|error| match error {
            secrets::SecretsInvocationError::Runtime(error) => error,
            secrets::SecretsInvocationError::Domain(_) => RuntimeFailure::PluginFailure {
                detail: format!("required GitHub Project Sync secret `{reference}` was rejected"),
            },
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origins_are_exact_https_origins() {
        assert!(valid_origin("https://api.github.com"));
        assert!(valid_origin("https://github.example.com:8443"));
        assert!(!valid_origin("http://api.github.com"));
        assert!(!valid_origin("https://api.github.com/path"));
        assert!(!valid_origin("https://user@api.github.com"));
    }

    #[test]
    fn plugin_debug_never_contains_secret_references() {
        let text = format!(
            "{:?}",
            GithubProjectSyncConfig {
                schema: "sync".into(),
                database_url_secret: "secret://db".into(),
                app_id: "1".into(),
                private_key_secret_ref: "secret://private".into(),
                webhook_secret_ref: "secret://webhook".into(),
                github_api_origin: "https://api.github.com".into(),
                github_api_version: "2026-03-10".into(),
                allowed_github_origins: vec!["https://api.github.com".into()],
                max_webhook_body_bytes: 1024,
                webhook_callers: vec!["gateway".into()],
                reader_callers: vec!["reader".into()],
                admin_callers: vec!["admin".into()],
                worker_callers: vec!["worker".into()]
            }
        );
        assert!(
            text.contains("secret://private"),
            "config itself is intentionally inspectable; plugin Debug is separately redacted"
        );
    }
}

#[cfg(all(test, feature = "postgres-acceptance"))]
mod postgres_tests;
