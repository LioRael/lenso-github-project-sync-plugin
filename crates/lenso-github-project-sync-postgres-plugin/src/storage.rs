#![allow(clippy::too_many_lines)]

use std::fmt;

use lenso_capability_github_project_sync as sync;
use lenso_capability_github_project_sync_admin as admin;
use lenso_kernel::RuntimeFailure;
use lenso_postgres_kit::OwnedPostgres;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sqlx::{AssertSqlSafe, Row};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DomainFailure {
    InvalidRequest,
    NotFound,
    RevisionConflict,
    DeliveryConflict,
    InstallationInactive,
    MappingInactive,
    LeaseLost,
}

#[derive(Debug)]
pub(crate) enum StorageError {
    Domain(DomainFailure),
    Runtime(RuntimeFailure),
}

impl From<DomainFailure> for StorageError {
    fn from(value: DomainFailure) -> Self {
        Self::Domain(value)
    }
}

fn runtime(operation: &'static str, source: impl fmt::Display) -> StorageError {
    StorageError::Runtime(RuntimeFailure::PluginFailure {
        detail: format!("GitHub Project Sync PostgreSQL operation `{operation}` failed: {source}"),
    })
}

fn decode<T: DeserializeOwned>(operation: &'static str, value: Value) -> Result<T, StorageError> {
    serde_json::from_value(value).map_err(|error| runtime(operation, error))
}

fn encode<T: Serialize>(operation: &'static str, value: &T) -> Result<Value, StorageError> {
    serde_json::to_value(value).map_err(|error| runtime(operation, error))
}

fn format_time(value: OffsetDateTime) -> Result<String, StorageError> {
    value
        .format(&Rfc3339)
        .map_err(|error| runtime("format timestamp", error))
}

pub(crate) fn parse_revision(value: &str) -> Result<i64, DomainFailure> {
    let parsed = value
        .parse::<i64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or(DomainFailure::InvalidRequest)?;
    if parsed.to_string() != value {
        return Err(DomainFailure::InvalidRequest);
    }
    Ok(parsed)
}

fn state_json(state: &str) -> &'static str {
    match state {
        "pending" => "pending",
        "processing" => "processing",
        "retry" => "retry",
        "succeeded" => "succeeded",
        _ => "dead",
    }
}

pub(crate) async fn initialize_settings(
    postgres: &OwnedPostgres,
    app_id: &str,
    private_key_secret_ref: &str,
    webhook_secret_ref: &str,
) -> Result<(), StorageError> {
    sqlx::query("INSERT INTO sync_settings(singleton,app_id,private_key_secret_ref,webhook_secret_ref,revision) VALUES(TRUE,$1,$2,$3,1) ON CONFLICT(singleton) DO NOTHING")
        .bind(app_id)
        .bind(private_key_secret_ref)
        .bind(webhook_secret_ref)
        .execute(postgres.pool())
        .await
        .map_err(|error| runtime("initialize settings", error))?;
    Ok(())
}

#[derive(Clone, Debug)]
pub(crate) struct Settings {
    pub app_id: String,
    pub private_key_secret_ref: String,
    pub webhook_secret_ref: String,
    pub previous_webhook_secret_ref: Option<String>,
}

pub(crate) async fn settings(postgres: &OwnedPostgres) -> Result<Settings, StorageError> {
    let row = sqlx::query("SELECT app_id,private_key_secret_ref,webhook_secret_ref,previous_webhook_secret_ref FROM sync_settings WHERE singleton=TRUE")
        .fetch_one(postgres.pool())
        .await
        .map_err(|error| runtime("read settings", error))?;
    Ok(Settings {
        app_id: row
            .try_get("app_id")
            .map_err(|error| runtime("decode settings", error))?,
        private_key_secret_ref: row
            .try_get("private_key_secret_ref")
            .map_err(|error| runtime("decode settings", error))?,
        webhook_secret_ref: row
            .try_get("webhook_secret_ref")
            .map_err(|error| runtime("decode settings", error))?,
        previous_webhook_secret_ref: row
            .try_get("previous_webhook_secret_ref")
            .map_err(|error| runtime("decode settings", error))?,
    })
}

pub(crate) async fn set_app_credentials(
    postgres: &OwnedPostgres,
    request: &admin::SetAppCredentialsRequest,
) -> Result<admin::SetAppCredentialsResponse, StorageError> {
    let expected = request
        .expected_revision
        .as_deref()
        .map(parse_revision)
        .transpose()?;
    let row = if let Some(expected) = expected {
        sqlx::query("UPDATE sync_settings SET app_id=$1,private_key_secret_ref=$2,revision=revision+1,updated_at=transaction_timestamp() WHERE singleton=TRUE AND revision=$3 RETURNING app_id,private_key_secret_ref,revision,updated_at")
            .bind(&request.app_id).bind(&request.private_key_secret_ref).bind(expected)
            .fetch_optional(postgres.pool()).await
    } else {
        sqlx::query("UPDATE sync_settings SET app_id=$1,private_key_secret_ref=$2,revision=revision+1,updated_at=transaction_timestamp() WHERE singleton=TRUE AND revision=1 RETURNING app_id,private_key_secret_ref,revision,updated_at")
            .bind(&request.app_id).bind(&request.private_key_secret_ref)
            .fetch_optional(postgres.pool()).await
    }.map_err(|error| runtime("set app credentials", error))?
        .ok_or(DomainFailure::RevisionConflict)?;
    let updated_at: OffsetDateTime = row
        .try_get("updated_at")
        .map_err(|error| runtime("decode app credentials", error))?;
    decode(
        "decode app credentials",
        json!({
            "app_id": row.try_get::<String,_>("app_id").map_err(|error| runtime("decode app credentials", error))?,
            "private_key_secret_ref": row.try_get::<String,_>("private_key_secret_ref").map_err(|error| runtime("decode app credentials", error))?,
            "revision": row.try_get::<i64,_>("revision").map_err(|error| runtime("decode app credentials", error))?.to_string(),
            "updated_at": format_time(updated_at)?,
        }),
    )
}

pub(crate) async fn set_webhook_secrets(
    postgres: &OwnedPostgres,
    request: &admin::SetWebhookSecretsRequest,
) -> Result<admin::SetWebhookSecretsResponse, StorageError> {
    let expected = request
        .expected_revision
        .as_deref()
        .map(parse_revision)
        .transpose()?;
    let row = if let Some(expected) = expected {
        sqlx::query("UPDATE sync_settings SET webhook_secret_ref=$1,previous_webhook_secret_ref=$2,revision=revision+1,updated_at=transaction_timestamp() WHERE singleton=TRUE AND revision=$3 RETURNING webhook_secret_ref,previous_webhook_secret_ref,revision,updated_at")
            .bind(&request.current_secret_ref).bind(&request.previous_secret_ref).bind(expected)
            .fetch_optional(postgres.pool()).await
    } else {
        sqlx::query("UPDATE sync_settings SET webhook_secret_ref=$1,previous_webhook_secret_ref=$2,revision=revision+1,updated_at=transaction_timestamp() WHERE singleton=TRUE AND revision=1 RETURNING webhook_secret_ref,previous_webhook_secret_ref,revision,updated_at")
            .bind(&request.current_secret_ref).bind(&request.previous_secret_ref)
            .fetch_optional(postgres.pool()).await
    }.map_err(|error| runtime("set webhook secrets", error))?
        .ok_or(DomainFailure::RevisionConflict)?;
    let updated_at: OffsetDateTime = row
        .try_get("updated_at")
        .map_err(|error| runtime("decode webhook secrets", error))?;
    decode(
        "decode webhook secrets",
        json!({
            "current_secret_ref": row.try_get::<String,_>("webhook_secret_ref").map_err(|error| runtime("decode webhook secrets", error))?,
            "previous_secret_ref": row.try_get::<Option<String>,_>("previous_webhook_secret_ref").map_err(|error| runtime("decode webhook secrets", error))?,
            "revision": row.try_get::<i64,_>("revision").map_err(|error| runtime("decode webhook secrets", error))?.to_string(),
            "updated_at": format_time(updated_at)?,
        }),
    )
}

fn installation_json(row: &sqlx::postgres::PgRow) -> Result<Value, StorageError> {
    let updated_at: OffsetDateTime = row
        .try_get("updated_at")
        .map_err(|error| runtime("decode installation", error))?;
    Ok(json!({
        "installation_id": row.try_get::<String,_>("installation_id").map_err(|error| runtime("decode installation", error))?,
        "account_login": row.try_get::<String,_>("account_login").map_err(|error| runtime("decode installation", error))?,
        "account_id": row.try_get::<String,_>("account_id").map_err(|error| runtime("decode installation", error))?,
        "active": row.try_get::<bool,_>("active").map_err(|error| runtime("decode installation", error))?,
        "revision": row.try_get::<i64,_>("revision").map_err(|error| runtime("decode installation", error))?.to_string(),
        "updated_at": format_time(updated_at)?,
    }))
}

pub(crate) async fn put_installation(
    postgres: &OwnedPostgres,
    request: &admin::PutInstallationRequest,
) -> Result<admin::PutInstallationResponse, StorageError> {
    let expected = request
        .expected_revision
        .as_deref()
        .map(parse_revision)
        .transpose()?;
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin put installation", error))?;
    let existing: Option<i64> = sqlx::query_scalar(
        "SELECT revision FROM github_installations WHERE installation_id=$1 FOR UPDATE",
    )
    .bind(&request.installation_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|error| runtime("lock installation", error))?;
    let row = match (existing, expected) {
        (None, None) => sqlx::query("INSERT INTO github_installations(installation_id,account_login,account_id,active,revision) VALUES($1,$2,$3,TRUE,1) RETURNING installation_id,account_login,account_id,active,revision,updated_at")
            .bind(&request.installation_id).bind(&request.account_login).bind(&request.account_id).fetch_one(&mut *tx).await,
        (Some(current), Some(expected)) if current == expected => sqlx::query("UPDATE github_installations SET account_login=$2,account_id=$3,active=TRUE,revision=revision+1,updated_at=transaction_timestamp() WHERE installation_id=$1 RETURNING installation_id,account_login,account_id,active,revision,updated_at")
            .bind(&request.installation_id).bind(&request.account_login).bind(&request.account_id).fetch_one(&mut *tx).await,
        _ => return Err(DomainFailure::RevisionConflict.into()),
    }.map_err(|error| runtime("put installation", error))?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit put installation", error))?;
    decode("decode put installation", installation_json(&row)?)
}

pub(crate) async fn revoke_installation(
    postgres: &OwnedPostgres,
    request: &admin::RevokeInstallationRequest,
) -> Result<admin::RevokeInstallationResponse, StorageError> {
    let expected = parse_revision(&request.expected_revision)?;
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin revoke installation", error))?;
    let row = sqlx::query("UPDATE github_installations SET active=FALSE,revision=revision+1,updated_at=transaction_timestamp() WHERE installation_id=$1 AND revision=$2 RETURNING installation_id,account_login,account_id,active,revision,updated_at")
        .bind(&request.installation_id).bind(expected).fetch_optional(&mut *tx).await.map_err(|error| runtime("revoke installation", error))?
        .ok_or(DomainFailure::RevisionConflict)?;
    sqlx::query("UPDATE repository_mappings SET active=FALSE,revision=revision+1,updated_at=transaction_timestamp() WHERE installation_id=$1 AND active=TRUE")
        .bind(&request.installation_id).execute(&mut *tx).await.map_err(|error| runtime("disable installation mappings", error))?;
    sqlx::query("UPDATE outbound_jobs SET state='dead',last_error_code='installation_revoked',updated_at=transaction_timestamp() WHERE mapping_id IN (SELECT mapping_id FROM repository_mappings WHERE installation_id=$1) AND state IN ('pending','retry','processing')")
        .bind(&request.installation_id).execute(&mut *tx).await.map_err(|error| runtime("dead-letter revoked installation jobs", error))?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit revoke installation", error))?;
    decode("decode revoke installation", installation_json(&row)?)
}

pub(crate) async fn apply_installation_event(
    postgres: &OwnedPostgres,
    installation_id: &str,
    account_login: &str,
    account_id: &str,
    active: bool,
) -> Result<(), StorageError> {
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin installation event", error))?;
    sqlx::query("INSERT INTO github_installations(installation_id,account_login,account_id,active,revision) VALUES($1,$2,$3,$4,1) ON CONFLICT(installation_id) DO UPDATE SET account_login=EXCLUDED.account_login,account_id=EXCLUDED.account_id,active=EXCLUDED.active,revision=github_installations.revision+1,updated_at=transaction_timestamp()")
        .bind(installation_id).bind(account_login).bind(account_id).bind(active).execute(&mut *tx).await.map_err(|error| runtime("apply installation event", error))?;
    if !active {
        sqlx::query("UPDATE repository_mappings SET active=FALSE,revision=revision+1,updated_at=transaction_timestamp() WHERE installation_id=$1 AND active")
            .bind(installation_id).execute(&mut *tx).await.map_err(|error| runtime("disable installation mappings", error))?;
        sqlx::query("UPDATE outbound_jobs SET state='dead',last_error_code='installation_revoked',updated_at=transaction_timestamp() WHERE mapping_id IN (SELECT mapping_id FROM repository_mappings WHERE installation_id=$1) AND state IN ('pending','retry','processing')")
            .bind(installation_id).execute(&mut *tx).await.map_err(|error| runtime("dead-letter installation event jobs", error))?;
    }
    tx.commit()
        .await
        .map_err(|error| runtime("commit installation event", error))?;
    Ok(())
}

pub(crate) async fn disable_repository_mapping_event(
    postgres: &OwnedPostgres,
    installation_id: &str,
    repository_id: &str,
) -> Result<(), StorageError> {
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin repository removal", error))?;
    sqlx::query("UPDATE repository_mappings SET active=FALSE,revision=revision+1,updated_at=transaction_timestamp() WHERE installation_id=$1 AND github_repository_id=$2 AND active")
        .bind(installation_id).bind(repository_id).execute(&mut *tx).await.map_err(|error| runtime("disable removed repository", error))?;
    sqlx::query("UPDATE outbound_jobs SET state='dead',last_error_code='repository_removed',updated_at=transaction_timestamp() WHERE mapping_id IN (SELECT mapping_id FROM repository_mappings WHERE installation_id=$1 AND github_repository_id=$2) AND state IN ('pending','retry','processing')")
        .bind(installation_id).bind(repository_id).execute(&mut *tx).await.map_err(|error| runtime("dead-letter removed repository jobs", error))?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit repository removal", error))?;
    Ok(())
}

fn conflict_policy_json<T: Serialize>(value: &T) -> Result<String, StorageError> {
    encode("encode conflict policy", value)?
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| runtime("encode conflict policy", "expected string"))
}

fn mapping_json(row: &sqlx::postgres::PgRow) -> Result<Value, StorageError> {
    let state: sqlx::types::Json<Value> = row
        .try_get("state_mappings")
        .map_err(|error| runtime("decode mapping", error))?;
    let labels: sqlx::types::Json<Value> = row
        .try_get("label_mappings")
        .map_err(|error| runtime("decode mapping", error))?;
    let milestones: sqlx::types::Json<Value> = row
        .try_get("milestone_mappings")
        .map_err(|error| runtime("decode mapping", error))?;
    let updated_at: OffsetDateTime = row
        .try_get("updated_at")
        .map_err(|error| runtime("decode mapping", error))?;
    Ok(json!({
        "mapping_id": row.try_get::<String,_>("mapping_id").map_err(|error| runtime("decode mapping", error))?,
        "installation_id": row.try_get::<String,_>("installation_id").map_err(|error| runtime("decode mapping", error))?,
        "github_repository_id": row.try_get::<String,_>("github_repository_id").map_err(|error| runtime("decode mapping", error))?,
        "github_owner": row.try_get::<String,_>("github_owner").map_err(|error| runtime("decode mapping", error))?,
        "github_repository": row.try_get::<String,_>("github_repository").map_err(|error| runtime("decode mapping", error))?,
        "organization_id": row.try_get::<String,_>("organization_id").map_err(|error| runtime("decode mapping", error))?,
        "team_id": row.try_get::<String,_>("team_id").map_err(|error| runtime("decode mapping", error))?,
        "project_id": row.try_get::<String,_>("project_id").map_err(|error| runtime("decode mapping", error))?,
        "github_project_id": row.try_get::<Option<String>,_>("github_project_id").map_err(|error| runtime("decode mapping", error))?,
        "github_project_status_field_id": row.try_get::<Option<String>,_>("github_project_status_field_id").map_err(|error| runtime("decode mapping", error))?,
        "inbound_enabled": row.try_get::<bool,_>("inbound_enabled").map_err(|error| runtime("decode mapping", error))?,
        "outbound_enabled": row.try_get::<bool,_>("outbound_enabled").map_err(|error| runtime("decode mapping", error))?,
        "conflict_policy": row.try_get::<String,_>("conflict_policy").map_err(|error| runtime("decode mapping", error))?,
        "state_mappings": state.0,
        "label_mappings": labels.0,
        "milestone_mappings": milestones.0,
        "active": row.try_get::<bool,_>("active").map_err(|error| runtime("decode mapping", error))?,
        "revision": row.try_get::<i64,_>("revision").map_err(|error| runtime("decode mapping", error))?.to_string(),
        "updated_at": format_time(updated_at)?,
    }))
}

const MAPPING_COLUMNS: &str = "mapping_id,installation_id,github_repository_id,github_owner,github_repository,organization_id,team_id,project_id,github_project_id,github_project_status_field_id,inbound_enabled,outbound_enabled,conflict_policy,state_mappings,label_mappings,milestone_mappings,active,revision,updated_at";

pub(crate) async fn put_mapping(
    postgres: &OwnedPostgres,
    request: &admin::PutMappingRequest,
) -> Result<admin::PutMappingResponse, StorageError> {
    let expected = request
        .expected_revision
        .as_deref()
        .map(parse_revision)
        .transpose()?;
    let policy = conflict_policy_json(&request.conflict_policy)?;
    let states = encode("encode state mappings", &request.state_mappings)?;
    let labels = encode("encode label mappings", &request.label_mappings)?;
    let milestones = encode("encode milestone mappings", &request.milestone_mappings)?;
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin put mapping", error))?;
    let installation_active: Option<bool> = sqlx::query_scalar(
        "SELECT active FROM github_installations WHERE installation_id=$1 FOR SHARE",
    )
    .bind(&request.installation_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|error| runtime("read mapping installation", error))?;
    match installation_active {
        None => return Err(DomainFailure::NotFound.into()),
        Some(false) => return Err(DomainFailure::InstallationInactive.into()),
        Some(true) => {}
    }
    let existing: Option<i64> = sqlx::query_scalar(
        "SELECT revision FROM repository_mappings WHERE mapping_id=$1 FOR UPDATE",
    )
    .bind(&request.mapping_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|error| runtime("lock mapping", error))?;
    let statement = format!(
        "INSERT INTO repository_mappings(mapping_id,installation_id,github_repository_id,github_owner,github_repository,organization_id,team_id,project_id,github_project_id,github_project_status_field_id,inbound_enabled,outbound_enabled,conflict_policy,state_mappings,label_mappings,milestone_mappings,active,revision) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,TRUE,1) RETURNING {MAPPING_COLUMNS}"
    );
    let update = format!(
        "UPDATE repository_mappings SET installation_id=$2,github_repository_id=$3,github_owner=$4,github_repository=$5,organization_id=$6,team_id=$7,project_id=$8,github_project_id=$9,github_project_status_field_id=$10,inbound_enabled=$11,outbound_enabled=$12,conflict_policy=$13,state_mappings=$14,label_mappings=$15,milestone_mappings=$16,active=TRUE,revision=revision+1,updated_at=transaction_timestamp() WHERE mapping_id=$1 RETURNING {MAPPING_COLUMNS}"
    );
    let query = match (existing, expected) {
        (None, None) => sqlx::query(AssertSqlSafe(statement.as_str())),
        (Some(current), Some(expected)) if current == expected => {
            sqlx::query(AssertSqlSafe(update.as_str()))
        }
        _ => return Err(DomainFailure::RevisionConflict.into()),
    };
    let row = query
        .bind(&request.mapping_id)
        .bind(&request.installation_id)
        .bind(&request.github_repository_id)
        .bind(&request.github_owner)
        .bind(&request.github_repository)
        .bind(&request.organization_id)
        .bind(&request.team_id)
        .bind(&request.project_id)
        .bind(&request.github_project_id)
        .bind(&request.github_project_status_field_id)
        .bind(request.inbound_enabled)
        .bind(request.outbound_enabled)
        .bind(policy)
        .bind(sqlx::types::Json(states))
        .bind(sqlx::types::Json(labels))
        .bind(sqlx::types::Json(milestones))
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| runtime("put mapping", error))?;
    sqlx::query(
        "INSERT INTO sync_checkpoints(mapping_id) VALUES($1) ON CONFLICT(mapping_id) DO NOTHING",
    )
    .bind(&request.mapping_id)
    .execute(&mut *tx)
    .await
    .map_err(|error| runtime("initialize checkpoint", error))?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit put mapping", error))?;
    decode("decode put mapping", mapping_json(&row)?)
}

pub(crate) async fn delete_mapping(
    postgres: &OwnedPostgres,
    request: &admin::DeleteMappingRequest,
) -> Result<admin::DeleteMappingResponse, StorageError> {
    let expected = parse_revision(&request.expected_revision)?;
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin delete mapping", error))?;
    let updated = sqlx::query("UPDATE repository_mappings SET active=FALSE,inbound_enabled=FALSE,outbound_enabled=FALSE,revision=revision+1,updated_at=transaction_timestamp() WHERE mapping_id=$1 AND revision=$2 AND active=TRUE")
        .bind(&request.mapping_id).bind(expected).execute(&mut *tx).await.map_err(|error| runtime("delete mapping", error))?;
    if updated.rows_affected() != 1 {
        return Err(DomainFailure::RevisionConflict.into());
    }
    sqlx::query("UPDATE outbound_jobs SET state='dead',last_error_code='mapping_inactive',updated_at=transaction_timestamp() WHERE mapping_id=$1 AND state IN ('pending','retry','processing')")
        .bind(&request.mapping_id).execute(&mut *tx).await.map_err(|error| runtime("dead-letter mapping jobs", error))?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit delete mapping", error))?;
    Ok(admin::DeleteMappingResponse { deleted: true })
}

pub(crate) async fn list_mappings(
    postgres: &OwnedPostgres,
    request: &admin::ListMappingsRequest,
) -> Result<admin::ListMappingsResponse, StorageError> {
    let after = request.after.as_deref().unwrap_or("");
    let query = format!(
        "SELECT {MAPPING_COLUMNS} FROM repository_mappings WHERE mapping_id>$1 AND ($2::text IS NULL OR organization_id=$2) ORDER BY mapping_id LIMIT $3"
    );
    let rows = sqlx::query(AssertSqlSafe(query.as_str()))
        .bind(after)
        .bind(&request.organization_id)
        .bind(request.limit)
        .fetch_all(postgres.pool())
        .await
        .map_err(|error| runtime("list mappings", error))?;
    let mut items = Vec::with_capacity(rows.len());
    for row in &rows {
        items.push(mapping_json(row)?);
    }
    let next_cursor = rows
        .last()
        .map(|row| row.try_get::<String, _>("mapping_id"))
        .transpose()
        .map_err(|error| runtime("decode mapping cursor", error))?;
    decode(
        "decode list mappings",
        json!({"items":items,"next_cursor":next_cursor}),
    )
}

#[derive(Clone, Debug)]
#[allow(clippy::struct_field_names)]
pub(crate) struct Mapping {
    pub mapping_id: String,
    pub installation_id: String,
    pub github_repository_id: String,
    pub github_owner: String,
    pub github_repository: String,
    pub organization_id: String,
    pub team_id: String,
    pub project_id: String,
    pub github_project_id: Option<String>,
    pub github_project_status_field_id: Option<String>,
    pub conflict_policy: String,
    pub state_mappings: Value,
    pub label_mappings: Value,
    pub milestone_mappings: Value,
}

pub(crate) struct IssueBindingWrite<'a> {
    pub github_issue_number: i64,
    pub github_node_id: &'a str,
    pub lenso_issue_id: &'a str,
    pub github_project_item_id: Option<&'a str>,
    pub github_updated_at: Option<OffsetDateTime>,
    pub lenso_revision: Option<i64>,
}

fn internal_mapping(row: &sqlx::postgres::PgRow) -> Result<Mapping, StorageError> {
    Ok(Mapping {
        mapping_id: row
            .try_get("mapping_id")
            .map_err(|error| runtime("decode worker mapping", error))?,
        installation_id: row
            .try_get("installation_id")
            .map_err(|error| runtime("decode worker mapping", error))?,
        github_repository_id: row
            .try_get("github_repository_id")
            .map_err(|error| runtime("decode worker mapping", error))?,
        github_owner: row
            .try_get("github_owner")
            .map_err(|error| runtime("decode worker mapping", error))?,
        github_repository: row
            .try_get("github_repository")
            .map_err(|error| runtime("decode worker mapping", error))?,
        organization_id: row
            .try_get("organization_id")
            .map_err(|error| runtime("decode worker mapping", error))?,
        team_id: row
            .try_get("team_id")
            .map_err(|error| runtime("decode worker mapping", error))?,
        project_id: row
            .try_get("project_id")
            .map_err(|error| runtime("decode worker mapping", error))?,
        github_project_id: row
            .try_get("github_project_id")
            .map_err(|error| runtime("decode worker mapping", error))?,
        github_project_status_field_id: row
            .try_get("github_project_status_field_id")
            .map_err(|error| runtime("decode worker mapping", error))?,
        conflict_policy: row
            .try_get("conflict_policy")
            .map_err(|error| runtime("decode worker mapping", error))?,
        state_mappings: row
            .try_get::<sqlx::types::Json<Value>, _>("state_mappings")
            .map_err(|error| runtime("decode worker mapping", error))?
            .0,
        label_mappings: row
            .try_get::<sqlx::types::Json<Value>, _>("label_mappings")
            .map_err(|error| runtime("decode worker mapping", error))?
            .0,
        milestone_mappings: row
            .try_get::<sqlx::types::Json<Value>, _>("milestone_mappings")
            .map_err(|error| runtime("decode worker mapping", error))?
            .0,
    })
}

const INTERNAL_MAPPING_COLUMNS: &str = "m.mapping_id,m.installation_id,m.github_repository_id,m.github_owner,m.github_repository,m.organization_id,m.team_id,m.project_id,m.github_project_id,m.github_project_status_field_id,m.conflict_policy,m.state_mappings,m.label_mappings,m.milestone_mappings";

pub(crate) async fn active_mapping_for_repository(
    postgres: &OwnedPostgres,
    installation_id: &str,
    repository_id: &str,
    inbound: bool,
) -> Result<Mapping, StorageError> {
    let direction = if inbound {
        "m.inbound_enabled"
    } else {
        "m.outbound_enabled"
    };
    let query = format!(
        "SELECT {INTERNAL_MAPPING_COLUMNS} FROM repository_mappings m JOIN github_installations i ON i.installation_id=m.installation_id WHERE m.installation_id=$1 AND m.github_repository_id=$2 AND m.active AND i.active AND {direction}"
    );
    let row = sqlx::query(AssertSqlSafe(query.as_str()))
        .bind(installation_id)
        .bind(repository_id)
        .fetch_optional(postgres.pool())
        .await
        .map_err(|error| runtime("find repository mapping", error))?
        .ok_or(DomainFailure::MappingInactive)?;
    internal_mapping(&row)
}

pub(crate) async fn active_outbound_mappings(
    postgres: &OwnedPostgres,
) -> Result<Vec<Mapping>, StorageError> {
    let query = format!(
        "SELECT {INTERNAL_MAPPING_COLUMNS} FROM repository_mappings m JOIN github_installations i ON i.installation_id=m.installation_id WHERE m.active AND m.outbound_enabled AND i.active ORDER BY m.mapping_id"
    );
    let rows = sqlx::query(AssertSqlSafe(query.as_str()))
        .fetch_all(postgres.pool())
        .await
        .map_err(|error| runtime("list outbound mappings", error))?;
    rows.iter().map(internal_mapping).collect()
}

pub(crate) async fn active_mapping_for_project(
    postgres: &OwnedPostgres,
    installation_id: &str,
    project_id: &str,
) -> Result<Mapping, StorageError> {
    let query = format!(
        "SELECT {INTERNAL_MAPPING_COLUMNS} FROM repository_mappings m JOIN github_installations i ON i.installation_id=m.installation_id WHERE m.installation_id=$1 AND m.github_project_id=$2 AND m.active AND m.inbound_enabled AND i.active"
    );
    let rows = sqlx::query(AssertSqlSafe(query.as_str()))
        .bind(installation_id)
        .bind(project_id)
        .fetch_all(postgres.pool())
        .await
        .map_err(|error| runtime("find ProjectV2 mapping", error))?;
    if rows.len() != 1 {
        return Err(DomainFailure::MappingInactive.into());
    }
    internal_mapping(&rows[0])
}

fn binding_json(row: &sqlx::postgres::PgRow) -> Result<Value, StorageError> {
    let updated_at: OffsetDateTime = row
        .try_get("updated_at")
        .map_err(|error| runtime("decode binding", error))?;
    Ok(json!({
        "mapping_id": row.try_get::<String,_>("mapping_id").map_err(|error| runtime("decode binding", error))?,
        "github_repository_id": row.try_get::<String,_>("github_repository_id").map_err(|error| runtime("decode binding", error))?,
        "github_issue_number": row.try_get::<i64,_>("github_issue_number").map_err(|error| runtime("decode binding", error))?,
        "github_node_id": row.try_get::<String,_>("github_node_id").map_err(|error| runtime("decode binding", error))?,
        "lenso_issue_id": row.try_get::<String,_>("lenso_issue_id").map_err(|error| runtime("decode binding", error))?,
        "github_project_item_id": row.try_get::<Option<String>,_>("github_project_item_id").map_err(|error| runtime("decode binding", error))?,
        "revision": row.try_get::<i64,_>("revision").map_err(|error| runtime("decode binding", error))?.to_string(),
        "updated_at": format_time(updated_at)?,
    }))
}

pub(crate) async fn get_binding(
    postgres: &OwnedPostgres,
    request: &sync::GetBindingRequest,
) -> Result<sync::GetBindingResponse, StorageError> {
    let row = sqlx::query("SELECT mapping_id,github_repository_id,github_issue_number,github_node_id,lenso_issue_id,github_project_item_id,revision,updated_at FROM github_issue_bindings WHERE mapping_id=$1 AND github_issue_number=$2")
        .bind(&request.mapping_id).bind(request.github_issue_number).fetch_optional(postgres.pool()).await
        .map_err(|error| runtime("get binding", error))?.ok_or(DomainFailure::NotFound)?;
    decode("decode get binding", binding_json(&row)?)
}

pub(crate) async fn list_bindings(
    postgres: &OwnedPostgres,
    request: &sync::ListBindingsRequest,
) -> Result<sync::ListBindingsResponse, StorageError> {
    let after = request.after.as_deref().map_or(Ok(0_i64), |value| {
        value
            .parse::<i64>()
            .map_err(|_| DomainFailure::InvalidRequest)
    })?;
    let rows = sqlx::query("SELECT mapping_id,github_repository_id,github_issue_number,github_node_id,lenso_issue_id,github_project_item_id,revision,updated_at FROM github_issue_bindings WHERE mapping_id=$1 AND github_issue_number>$2 ORDER BY github_issue_number LIMIT $3")
        .bind(&request.mapping_id).bind(after).bind(request.limit).fetch_all(postgres.pool()).await
        .map_err(|error| runtime("list bindings", error))?;
    let mut items = Vec::with_capacity(rows.len());
    for row in &rows {
        items.push(binding_json(row)?);
    }
    let next_cursor = rows
        .last()
        .map(|row| {
            row.try_get::<i64, _>("github_issue_number")
                .map(|value| value.to_string())
        })
        .transpose()
        .map_err(|error| runtime("decode binding cursor", error))?;
    decode(
        "decode list bindings",
        json!({"items":items,"next_cursor":next_cursor}),
    )
}

#[derive(Clone, Debug)]
pub(crate) struct IssueBinding {
    pub github_issue_number: i64,
    pub github_node_id: String,
    pub lenso_issue_id: String,
    pub github_project_item_id: Option<String>,
}

pub(crate) async fn binding_by_lenso_issue(
    postgres: &OwnedPostgres,
    mapping_id: &str,
    issue_id: &str,
) -> Result<Option<IssueBinding>, StorageError> {
    let row = sqlx::query("SELECT mapping_id,github_issue_number,github_node_id,lenso_issue_id,github_project_item_id FROM github_issue_bindings WHERE mapping_id=$1 AND lenso_issue_id=$2")
        .bind(mapping_id).bind(issue_id).fetch_optional(postgres.pool()).await.map_err(|error| runtime("find issue binding", error))?;
    row.map(|row| {
        Ok(IssueBinding {
            github_issue_number: row
                .try_get("github_issue_number")
                .map_err(|error| runtime("decode issue binding", error))?,
            github_node_id: row
                .try_get("github_node_id")
                .map_err(|error| runtime("decode issue binding", error))?,
            lenso_issue_id: row
                .try_get("lenso_issue_id")
                .map_err(|error| runtime("decode issue binding", error))?,
            github_project_item_id: row
                .try_get("github_project_item_id")
                .map_err(|error| runtime("decode issue binding", error))?,
        })
    })
    .transpose()
}

pub(crate) async fn binding_by_github_issue(
    postgres: &OwnedPostgres,
    mapping_id: &str,
    number: i64,
) -> Result<Option<IssueBinding>, StorageError> {
    let row=sqlx::query("SELECT mapping_id,github_issue_number,github_node_id,lenso_issue_id,github_project_item_id FROM github_issue_bindings WHERE mapping_id=$1 AND github_issue_number=$2")
        .bind(mapping_id).bind(number).fetch_optional(postgres.pool()).await.map_err(|error|runtime("find GitHub issue binding",error))?;
    row.map(|row| {
        Ok(IssueBinding {
            github_issue_number: row
                .try_get("github_issue_number")
                .map_err(|error| runtime("decode issue binding", error))?,
            github_node_id: row
                .try_get("github_node_id")
                .map_err(|error| runtime("decode issue binding", error))?,
            lenso_issue_id: row
                .try_get("lenso_issue_id")
                .map_err(|error| runtime("decode issue binding", error))?,
            github_project_item_id: row
                .try_get("github_project_item_id")
                .map_err(|error| runtime("decode issue binding", error))?,
        })
    })
    .transpose()
}

pub(crate) async fn binding_by_github_node(
    postgres: &OwnedPostgres,
    mapping_id: &str,
    node_id: &str,
) -> Result<Option<IssueBinding>, StorageError> {
    let row=sqlx::query("SELECT github_issue_number,github_node_id,lenso_issue_id,github_project_item_id FROM github_issue_bindings WHERE mapping_id=$1 AND github_node_id=$2")
        .bind(mapping_id).bind(node_id).fetch_optional(postgres.pool()).await.map_err(|error|runtime("find GitHub node binding",error))?;
    row.map(|row| {
        Ok(IssueBinding {
            github_issue_number: row
                .try_get("github_issue_number")
                .map_err(|error| runtime("decode issue binding", error))?,
            github_node_id: row
                .try_get("github_node_id")
                .map_err(|error| runtime("decode issue binding", error))?,
            lenso_issue_id: row
                .try_get("lenso_issue_id")
                .map_err(|error| runtime("decode issue binding", error))?,
            github_project_item_id: row
                .try_get("github_project_item_id")
                .map_err(|error| runtime("decode issue binding", error))?,
        })
    })
    .transpose()
}

pub(crate) async fn upsert_issue_binding(
    postgres: &OwnedPostgres,
    mapping: &Mapping,
    write: IssueBindingWrite<'_>,
) -> Result<(), StorageError> {
    sqlx::query("INSERT INTO github_issue_bindings(mapping_id,github_repository_id,github_issue_number,github_node_id,lenso_issue_id,github_project_item_id,last_github_updated_at,last_lenso_revision,revision) VALUES($1,$2,$3,$4,$5,$6,$7,$8,1) ON CONFLICT(mapping_id,github_issue_number) DO UPDATE SET github_node_id=EXCLUDED.github_node_id,lenso_issue_id=EXCLUDED.lenso_issue_id,github_project_item_id=COALESCE(EXCLUDED.github_project_item_id,github_issue_bindings.github_project_item_id),last_github_updated_at=EXCLUDED.last_github_updated_at,last_lenso_revision=EXCLUDED.last_lenso_revision,revision=github_issue_bindings.revision+1,updated_at=transaction_timestamp()")
        .bind(&mapping.mapping_id).bind(&mapping.github_repository_id).bind(write.github_issue_number).bind(write.github_node_id)
        .bind(write.lenso_issue_id).bind(write.github_project_item_id).bind(write.github_updated_at).bind(write.lenso_revision)
        .execute(postgres.pool()).await.map_err(|error| runtime("upsert issue binding", error))?;
    Ok(())
}

pub(crate) async fn set_project_item(
    postgres: &OwnedPostgres,
    mapping_id: &str,
    issue_number: i64,
    item_id: &str,
) -> Result<(), StorageError> {
    let updated=sqlx::query("UPDATE github_issue_bindings SET github_project_item_id=$3,revision=revision+1,updated_at=transaction_timestamp() WHERE mapping_id=$1 AND github_issue_number=$2")
        .bind(mapping_id).bind(issue_number).bind(item_id).execute(postgres.pool()).await.map_err(|error|runtime("set GitHub project item",error))?;
    if updated.rows_affected() != 1 {
        return Err(DomainFailure::NotFound.into());
    }
    Ok(())
}

pub(crate) async fn insert_delivery(
    postgres: &OwnedPostgres,
    delivery_id: &str,
    event: &str,
    payload: &[u8],
    payload_sha256: &str,
) -> Result<sync::IngestWebhookResponse, StorageError> {
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin ingest delivery", error))?;
    let inserted = sqlx::query("INSERT INTO webhook_deliveries(delivery_id,event,payload,payload_sha256,state) VALUES($1,$2,$3,$4,'pending') ON CONFLICT(delivery_id) DO NOTHING")
        .bind(delivery_id).bind(event).bind(payload).bind(payload_sha256).execute(&mut *tx).await.map_err(|error| runtime("insert delivery", error))?;
    let row = sqlx::query("SELECT delivery_id,event,payload_sha256,state,received_at FROM webhook_deliveries WHERE delivery_id=$1 FOR UPDATE")
        .bind(delivery_id).fetch_one(&mut *tx).await.map_err(|error| runtime("read delivery", error))?;
    let stored_event: String = row
        .try_get("event")
        .map_err(|error| runtime("decode delivery", error))?;
    let stored_hash: String = row
        .try_get("payload_sha256")
        .map_err(|error| runtime("decode delivery", error))?;
    if stored_event != event || stored_hash != payload_sha256 {
        return Err(DomainFailure::DeliveryConflict.into());
    }
    let received_at: OffsetDateTime = row
        .try_get("received_at")
        .map_err(|error| runtime("decode delivery", error))?;
    let state: String = row
        .try_get("state")
        .map_err(|error| runtime("decode delivery", error))?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit delivery", error))?;
    decode(
        "decode ingest delivery",
        json!({
            "delivery_id":delivery_id,"state":state_json(&state),"duplicate":inserted.rows_affected()==0,"received_at":format_time(received_at)?
        }),
    )
}

pub(crate) async fn inspect_delivery(
    postgres: &OwnedPostgres,
    request: &admin::InspectDeliveryRequest,
) -> Result<admin::InspectDeliveryResponse, StorageError> {
    let row = sqlx::query("SELECT delivery_id,event,payload_sha256,state,attempts,next_attempt_at,lease_owner,fence,last_error_code,received_at,updated_at FROM webhook_deliveries WHERE delivery_id=$1")
        .bind(&request.delivery_id).fetch_optional(postgres.pool()).await.map_err(|error| runtime("inspect delivery", error))?
        .ok_or(DomainFailure::NotFound)?;
    let next_attempt_at: OffsetDateTime = row
        .try_get("next_attempt_at")
        .map_err(|error| runtime("decode delivery detail", error))?;
    let received_at: OffsetDateTime = row
        .try_get("received_at")
        .map_err(|error| runtime("decode delivery detail", error))?;
    let updated_at: OffsetDateTime = row
        .try_get("updated_at")
        .map_err(|error| runtime("decode delivery detail", error))?;
    decode(
        "decode delivery detail",
        json!({
            "delivery_id":row.try_get::<String,_>("delivery_id").map_err(|error| runtime("decode delivery detail", error))?,
            "event":row.try_get::<String,_>("event").map_err(|error| runtime("decode delivery detail", error))?,
            "payload_sha256":row.try_get::<String,_>("payload_sha256").map_err(|error| runtime("decode delivery detail", error))?,
            "state":state_json(&row.try_get::<String,_>("state").map_err(|error| runtime("decode delivery detail", error))?),
            "attempts":i64::from(row.try_get::<i32,_>("attempts").map_err(|error| runtime("decode delivery detail", error))?),
            "next_attempt_at":format_time(next_attempt_at)?,"lease_owner":row.try_get::<Option<String>,_>("lease_owner").map_err(|error| runtime("decode delivery detail", error))?,
            "fence":row.try_get::<i64,_>("fence").map_err(|error| runtime("decode delivery detail", error))?.to_string(),
            "last_error_code":row.try_get::<Option<String>,_>("last_error_code").map_err(|error| runtime("decode delivery detail", error))?,
            "received_at":format_time(received_at)?,"updated_at":format_time(updated_at)?
        }),
    )
}

pub(crate) async fn list_dead_letters(
    postgres: &OwnedPostgres,
    request: &admin::ListDeadLettersRequest,
) -> Result<admin::ListDeadLettersResponse, StorageError> {
    let after = request.after.as_deref().unwrap_or("");
    let rows = sqlx::query("SELECT * FROM (SELECT 'delivery:'||delivery_id AS cursor,'delivery' AS kind,delivery_id AS item_id,NULL::text AS mapping_id,attempts,last_error_code AS error_code,updated_at FROM webhook_deliveries WHERE state='dead' UNION ALL SELECT 'outbound:'||job_id AS cursor,'outbound' AS kind,job_id AS item_id,mapping_id,attempts,last_error_code AS error_code,updated_at FROM outbound_jobs WHERE state='dead') dead WHERE cursor>$1 ORDER BY cursor LIMIT $2")
        .bind(after).bind(request.limit).fetch_all(postgres.pool()).await.map_err(|error| runtime("list dead letters", error))?;
    let mut items = Vec::with_capacity(rows.len());
    for row in &rows {
        let updated_at: OffsetDateTime = row
            .try_get("updated_at")
            .map_err(|error| runtime("decode dead letter", error))?;
        items.push(json!({
            "cursor":row.try_get::<String,_>("cursor").map_err(|error| runtime("decode dead letter", error))?,
            "kind":row.try_get::<String,_>("kind").map_err(|error| runtime("decode dead letter", error))?,
            "item_id":row.try_get::<String,_>("item_id").map_err(|error| runtime("decode dead letter", error))?,
            "mapping_id":row.try_get::<Option<String>,_>("mapping_id").map_err(|error| runtime("decode dead letter", error))?,
            "attempts":i64::from(row.try_get::<i32,_>("attempts").map_err(|error| runtime("decode dead letter", error))?),
            "error_code":row.try_get::<Option<String>,_>("error_code").map_err(|error| runtime("decode dead letter", error))?.unwrap_or_else(||"unknown".to_owned()),
            "updated_at":format_time(updated_at)?
        }));
    }
    let next_cursor = rows
        .last()
        .map(|row| row.try_get::<String, _>("cursor"))
        .transpose()
        .map_err(|error| runtime("decode dead letter cursor", error))?;
    decode(
        "decode dead letters",
        json!({"items":items,"next_cursor":next_cursor}),
    )
}

pub(crate) async fn replay_dead_letter(
    postgres: &OwnedPostgres,
    request: &admin::ReplayDeadLetterRequest,
) -> Result<admin::ReplayDeadLetterResponse, StorageError> {
    let kind = encode("encode replay kind", &request.kind)?
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| runtime("encode replay kind", "expected string"))?;
    let table = if kind == "delivery" {
        "webhook_deliveries"
    } else {
        "outbound_jobs"
    };
    let id_column = if kind == "delivery" {
        "delivery_id"
    } else {
        "job_id"
    };
    let query = format!(
        "UPDATE {table} SET state='pending',attempts=0,next_attempt_at=transaction_timestamp(),lease_owner=NULL,lease_until=NULL,last_error_code=NULL,updated_at=transaction_timestamp() WHERE {id_column}=$1 AND state='dead'"
    );
    let updated = sqlx::query(AssertSqlSafe(query.as_str()))
        .bind(&request.item_id)
        .execute(postgres.pool())
        .await
        .map_err(|error| runtime("replay dead letter", error))?;
    if updated.rows_affected() != 1 {
        return Err(DomainFailure::NotFound.into());
    }
    decode(
        "decode replay",
        json!({"kind":kind,"item_id":request.item_id,"state":"pending"}),
    )
}

#[derive(Clone, Debug)]
pub(crate) struct ClaimedDelivery {
    pub delivery_id: String,
    pub event: String,
    pub payload: Vec<u8>,
    pub attempts: i32,
    pub fence: i64,
}

pub(crate) async fn claim_deliveries(
    postgres: &OwnedPostgres,
    worker_id: &str,
    limit: i64,
    lease_seconds: i64,
) -> Result<Vec<ClaimedDelivery>, StorageError> {
    let rows = sqlx::query("WITH due AS (SELECT delivery_id FROM webhook_deliveries WHERE ((state IN ('pending','retry') AND next_attempt_at<=transaction_timestamp()) OR (state='processing' AND lease_until<transaction_timestamp())) ORDER BY next_attempt_at,delivery_id FOR UPDATE SKIP LOCKED LIMIT $1) UPDATE webhook_deliveries d SET state='processing',attempts=d.attempts+1,lease_owner=$2,lease_until=transaction_timestamp()+make_interval(secs=>$3::double precision),fence=d.fence+1,updated_at=transaction_timestamp() FROM due WHERE d.delivery_id=due.delivery_id RETURNING d.delivery_id,d.event,d.payload,d.attempts,d.fence")
        .bind(limit).bind(worker_id).bind(lease_seconds).fetch_all(postgres.pool()).await.map_err(|error| runtime("claim deliveries", error))?;
    rows.into_iter()
        .map(|row| {
            Ok(ClaimedDelivery {
                delivery_id: row
                    .try_get("delivery_id")
                    .map_err(|error| runtime("decode claimed delivery", error))?,
                event: row
                    .try_get("event")
                    .map_err(|error| runtime("decode claimed delivery", error))?,
                payload: row
                    .try_get("payload")
                    .map_err(|error| runtime("decode claimed delivery", error))?,
                attempts: row
                    .try_get("attempts")
                    .map_err(|error| runtime("decode claimed delivery", error))?,
                fence: row
                    .try_get("fence")
                    .map_err(|error| runtime("decode claimed delivery", error))?,
            })
        })
        .collect()
}

async fn transition_delivery(
    postgres: &OwnedPostgres,
    claim: &ClaimedDelivery,
    worker_id: &str,
    state: &str,
    error_code: Option<&str>,
    backoff_seconds: i64,
) -> Result<(), StorageError> {
    let updated = sqlx::query("UPDATE webhook_deliveries SET state=$4,last_error_code=$5,next_attempt_at=transaction_timestamp()+make_interval(secs=>$6::double precision),lease_owner=NULL,lease_until=NULL,updated_at=transaction_timestamp() WHERE delivery_id=$1 AND state='processing' AND lease_owner=$2 AND fence=$3")
        .bind(&claim.delivery_id).bind(worker_id).bind(claim.fence).bind(state).bind(error_code).bind(backoff_seconds)
        .execute(postgres.pool()).await.map_err(|error| runtime("transition delivery", error))?;
    if updated.rows_affected() != 1 {
        return Err(DomainFailure::LeaseLost.into());
    }
    Ok(())
}

pub(crate) async fn succeed_delivery(
    postgres: &OwnedPostgres,
    claim: &ClaimedDelivery,
    worker_id: &str,
) -> Result<(), StorageError> {
    transition_delivery(postgres, claim, worker_id, "succeeded", None, 0).await
}

pub(crate) async fn fail_delivery(
    postgres: &OwnedPostgres,
    claim: &ClaimedDelivery,
    worker_id: &str,
    error_code: &str,
    retryable: bool,
) -> Result<bool, StorageError> {
    let dead = !retryable || claim.attempts >= 8;
    let exponent = u32::try_from(claim.attempts.clamp(1, 10)).unwrap_or(10);
    let backoff = if dead {
        0
    } else {
        (2_i64.pow(exponent)).min(900)
    };
    transition_delivery(
        postgres,
        claim,
        worker_id,
        if dead { "dead" } else { "retry" },
        Some(error_code),
        backoff,
    )
    .await?;
    Ok(dead)
}

#[derive(Clone, Debug)]
pub(crate) struct ActivityStub {
    pub activity_id: String,
    pub entity_kind: String,
    pub entity_id: String,
    pub issue_id: Option<String>,
    pub operation: String,
    pub revision: Option<i64>,
}

pub(crate) async fn stage_activity_page(
    postgres: &OwnedPostgres,
    mapping_id: &str,
    expected_cursor: Option<&str>,
    next_cursor: Option<&str>,
    items: &[ActivityStub],
) -> Result<i64, StorageError> {
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin stage activities", error))?;
    let current: Option<String> = sqlx::query_scalar(
        "SELECT activity_cursor FROM sync_checkpoints WHERE mapping_id=$1 FOR UPDATE",
    )
    .bind(mapping_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|error| runtime("lock checkpoint", error))?
    .ok_or(DomainFailure::MappingInactive)?;
    if current.as_deref() != expected_cursor {
        tx.rollback()
            .await
            .map_err(|error| runtime("rollback stale checkpoint", error))?;
        return Ok(0);
    }
    let mut staged = 0_i64;
    for item in items {
        let job_id = uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_URL,
            format!("lenso-github-sync:{mapping_id}:{}", item.activity_id).as_bytes(),
        )
        .to_string();
        let inserted = sqlx::query("INSERT INTO outbound_jobs(job_id,mapping_id,activity_id,entity_kind,entity_id,issue_id,operation,lenso_revision,state,last_error_code) SELECT $1,$2,$3,$4,$5,$6,$7,$8,CASE WHEN EXISTS(SELECT 1 FROM inbound_suppressions s WHERE s.mapping_id=$2 AND s.entity_kind=$4 AND s.entity_id=$5 AND s.revision=$8) THEN 'suppressed' ELSE 'pending' END,CASE WHEN EXISTS(SELECT 1 FROM inbound_suppressions s WHERE s.mapping_id=$2 AND s.entity_kind=$4 AND s.entity_id=$5 AND s.revision=$8) THEN 'inbound_origin' ELSE NULL END ON CONFLICT(mapping_id,activity_id) DO NOTHING")
            .bind(job_id).bind(mapping_id).bind(&item.activity_id).bind(&item.entity_kind).bind(&item.entity_id).bind(&item.issue_id).bind(&item.operation).bind(item.revision)
            .execute(&mut *tx).await.map_err(|error| runtime("stage outbound job", error))?;
        staged += i64::try_from(inserted.rows_affected()).unwrap_or(0);
    }
    sqlx::query("UPDATE sync_checkpoints SET activity_cursor=$2,updated_at=transaction_timestamp() WHERE mapping_id=$1")
        .bind(mapping_id).bind(next_cursor).execute(&mut *tx).await.map_err(|error| runtime("advance checkpoint", error))?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit activity page", error))?;
    Ok(staged)
}

pub(crate) async fn checkpoint_cursor(
    postgres: &OwnedPostgres,
    mapping_id: &str,
) -> Result<Option<String>, StorageError> {
    sqlx::query_scalar("SELECT activity_cursor FROM sync_checkpoints WHERE mapping_id=$1")
        .bind(mapping_id)
        .fetch_optional(postgres.pool())
        .await
        .map_err(|error| runtime("read checkpoint cursor", error))?
        .ok_or(DomainFailure::MappingInactive.into())
}

pub(crate) async fn get_checkpoint(
    postgres: &OwnedPostgres,
    request: &admin::GetCheckpointRequest,
) -> Result<admin::GetCheckpointResponse, StorageError> {
    let row = sqlx::query("SELECT c.mapping_id,c.activity_cursor,c.updated_at,COUNT(j.job_id) FILTER (WHERE j.state IN ('pending','processing','retry')) AS staged_count,MIN(j.activity_id) FILTER (WHERE j.state IN ('pending','processing','retry')) AS oldest_pending_activity_id FROM sync_checkpoints c LEFT JOIN outbound_jobs j ON j.mapping_id=c.mapping_id WHERE c.mapping_id=$1 GROUP BY c.mapping_id,c.activity_cursor,c.updated_at")
        .bind(&request.mapping_id).fetch_optional(postgres.pool()).await.map_err(|error| runtime("get checkpoint", error))?.ok_or(DomainFailure::NotFound)?;
    let updated_at: OffsetDateTime = row
        .try_get("updated_at")
        .map_err(|error| runtime("decode checkpoint", error))?;
    decode(
        "decode checkpoint",
        json!({
            "mapping_id":row.try_get::<String,_>("mapping_id").map_err(|error| runtime("decode checkpoint", error))?,
            "activity_cursor":row.try_get::<Option<String>,_>("activity_cursor").map_err(|error| runtime("decode checkpoint", error))?,
            "staged_count":row.try_get::<i64,_>("staged_count").map_err(|error| runtime("decode checkpoint", error))?,
            "oldest_pending_activity_id":row.try_get::<Option<String>,_>("oldest_pending_activity_id").map_err(|error| runtime("decode checkpoint", error))?,
            "updated_at":format_time(updated_at)?
        }),
    )
}

#[derive(Clone, Debug)]
pub(crate) struct ClaimedJob {
    pub job_id: String,
    pub mapping_id: String,
    pub activity_id: String,
    pub entity_kind: String,
    pub entity_id: String,
    pub issue_id: Option<String>,
    pub operation: String,
    pub lenso_revision: Option<i64>,
    pub attempts: i32,
    pub fence: i64,
}

pub(crate) async fn claim_outbound_jobs(
    postgres: &OwnedPostgres,
    worker_id: &str,
    limit: i64,
    lease_seconds: i64,
) -> Result<Vec<ClaimedJob>, StorageError> {
    let rows = sqlx::query("WITH due AS (SELECT j.job_id FROM outbound_jobs j JOIN repository_mappings m ON m.mapping_id=j.mapping_id JOIN github_installations i ON i.installation_id=m.installation_id WHERE m.active AND m.outbound_enabled AND i.active AND ((j.state IN ('pending','retry') AND j.next_attempt_at<=transaction_timestamp()) OR (j.state='processing' AND j.lease_until<transaction_timestamp())) ORDER BY j.next_attempt_at,j.job_id FOR UPDATE OF j SKIP LOCKED LIMIT $1) UPDATE outbound_jobs j SET state='processing',attempts=j.attempts+1,lease_owner=$2,lease_until=transaction_timestamp()+make_interval(secs=>$3::double precision),fence=j.fence+1,updated_at=transaction_timestamp() FROM due WHERE j.job_id=due.job_id RETURNING j.job_id,j.mapping_id,j.activity_id,j.entity_kind,j.entity_id,j.issue_id,j.operation,j.lenso_revision,j.attempts,j.fence")
        .bind(limit).bind(worker_id).bind(lease_seconds).fetch_all(postgres.pool()).await.map_err(|error| runtime("claim outbound jobs", error))?;
    rows.into_iter()
        .map(|row| {
            Ok(ClaimedJob {
                job_id: row
                    .try_get("job_id")
                    .map_err(|error| runtime("decode outbound job", error))?,
                mapping_id: row
                    .try_get("mapping_id")
                    .map_err(|error| runtime("decode outbound job", error))?,
                activity_id: row
                    .try_get("activity_id")
                    .map_err(|error| runtime("decode outbound job", error))?,
                entity_kind: row
                    .try_get("entity_kind")
                    .map_err(|error| runtime("decode outbound job", error))?,
                entity_id: row
                    .try_get("entity_id")
                    .map_err(|error| runtime("decode outbound job", error))?,
                issue_id: row
                    .try_get("issue_id")
                    .map_err(|error| runtime("decode outbound job", error))?,
                operation: row
                    .try_get("operation")
                    .map_err(|error| runtime("decode outbound job", error))?,
                lenso_revision: row
                    .try_get("lenso_revision")
                    .map_err(|error| runtime("decode outbound job", error))?,
                attempts: row
                    .try_get("attempts")
                    .map_err(|error| runtime("decode outbound job", error))?,
                fence: row
                    .try_get("fence")
                    .map_err(|error| runtime("decode outbound job", error))?,
            })
        })
        .collect()
}

pub(crate) async fn mapping_by_id(
    postgres: &OwnedPostgres,
    mapping_id: &str,
) -> Result<Mapping, StorageError> {
    let query = format!(
        "SELECT {INTERNAL_MAPPING_COLUMNS} FROM repository_mappings m JOIN github_installations i ON i.installation_id=m.installation_id WHERE m.mapping_id=$1 AND m.active AND i.active"
    );
    let row = sqlx::query(AssertSqlSafe(query.as_str()))
        .bind(mapping_id)
        .fetch_optional(postgres.pool())
        .await
        .map_err(|error| runtime("get worker mapping", error))?
        .ok_or(DomainFailure::MappingInactive)?;
    internal_mapping(&row)
}

async fn transition_job(
    postgres: &OwnedPostgres,
    claim: &ClaimedJob,
    worker_id: &str,
    state: &str,
    error_code: Option<&str>,
    backoff_seconds: i64,
    receipt: Option<&Value>,
) -> Result<(), StorageError> {
    let updated = sqlx::query("UPDATE outbound_jobs SET state=$4,last_error_code=$5,next_attempt_at=transaction_timestamp()+make_interval(secs=>$6::double precision),remote_receipt=COALESCE($7,remote_receipt),lease_owner=NULL,lease_until=NULL,updated_at=transaction_timestamp() WHERE job_id=$1 AND state='processing' AND lease_owner=$2 AND fence=$3")
        .bind(&claim.job_id).bind(worker_id).bind(claim.fence).bind(state).bind(error_code).bind(backoff_seconds)
        .bind(receipt.map(|value| sqlx::types::Json(value.clone())))
        .execute(postgres.pool()).await.map_err(|error| runtime("transition outbound job", error))?;
    if updated.rows_affected() != 1 {
        return Err(DomainFailure::LeaseLost.into());
    }
    Ok(())
}

pub(crate) async fn succeed_job(
    postgres: &OwnedPostgres,
    claim: &ClaimedJob,
    worker_id: &str,
    receipt: &Value,
) -> Result<(), StorageError> {
    transition_job(
        postgres,
        claim,
        worker_id,
        "succeeded",
        None,
        0,
        Some(receipt),
    )
    .await
}

pub(crate) async fn suppress_job(
    postgres: &OwnedPostgres,
    claim: &ClaimedJob,
    worker_id: &str,
    reason: &str,
) -> Result<(), StorageError> {
    transition_job(
        postgres,
        claim,
        worker_id,
        "suppressed",
        Some(reason),
        0,
        None,
    )
    .await
}

pub(crate) async fn fail_job(
    postgres: &OwnedPostgres,
    claim: &ClaimedJob,
    worker_id: &str,
    error_code: &str,
    retryable: bool,
) -> Result<bool, StorageError> {
    let dead = !retryable || claim.attempts >= 8;
    let exponent = u32::try_from(claim.attempts.clamp(1, 10)).unwrap_or(10);
    let backoff = if dead {
        0
    } else {
        (2_i64.pow(exponent)).min(900)
    };
    transition_job(
        postgres,
        claim,
        worker_id,
        if dead { "dead" } else { "retry" },
        Some(error_code),
        backoff,
        None,
    )
    .await?;
    Ok(dead)
}

pub(crate) async fn save_origin_marker(
    postgres: &OwnedPostgres,
    marker: &str,
    mapping_id: &str,
    job_id: &str,
    github_node_id: Option<&str>,
) -> Result<(), StorageError> {
    sqlx::query("INSERT INTO origin_markers(marker,mapping_id,job_id,github_node_id) VALUES($1,$2,$3,$4) ON CONFLICT(marker) DO NOTHING")
        .bind(marker).bind(mapping_id).bind(job_id).bind(github_node_id).execute(postgres.pool()).await.map_err(|error| runtime("save origin marker", error))?;
    Ok(())
}

pub(crate) async fn known_origin_marker(
    postgres: &OwnedPostgres,
    marker: &str,
    mapping_id: &str,
) -> Result<bool, StorageError> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM origin_markers WHERE marker=$1 AND mapping_id=$2)",
    )
    .bind(marker)
    .bind(mapping_id)
    .fetch_one(postgres.pool())
    .await
    .map_err(|error| runtime("check origin marker", error))?;
    Ok(exists)
}

pub(crate) async fn record_effect(
    postgres: &OwnedPostgres,
    delivery_id: &str,
    effect_key: &str,
    result: &Value,
) -> Result<bool, StorageError> {
    let inserted = sqlx::query("INSERT INTO inbound_effects(delivery_id,effect_key,result) VALUES($1,$2,$3) ON CONFLICT(delivery_id,effect_key) DO NOTHING")
        .bind(delivery_id).bind(effect_key).bind(sqlx::types::Json(result.clone())).execute(postgres.pool()).await.map_err(|error| runtime("record inbound effect", error))?;
    Ok(inserted.rows_affected() == 1)
}

pub(crate) async fn save_inbound_suppression(
    postgres: &OwnedPostgres,
    mapping_id: &str,
    entity_kind: &str,
    entity_id: &str,
    revision: i64,
    delivery_id: &str,
) -> Result<(), StorageError> {
    sqlx::query("INSERT INTO inbound_suppressions(mapping_id,entity_kind,entity_id,revision,delivery_id) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING")
        .bind(mapping_id).bind(entity_kind).bind(entity_id).bind(revision).bind(delivery_id).execute(postgres.pool()).await.map_err(|error|runtime("save inbound suppression",error))?;
    Ok(())
}

pub(crate) async fn effect_exists(
    postgres: &OwnedPostgres,
    delivery_id: &str,
    effect_key: &str,
) -> Result<bool, StorageError> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM inbound_effects WHERE delivery_id=$1 AND effect_key=$2)",
    )
    .bind(delivery_id)
    .bind(effect_key)
    .fetch_one(postgres.pool())
    .await
    .map_err(|error| runtime("check inbound effect", error))?;
    Ok(exists)
}

pub(crate) async fn upsert_comment_binding(
    postgres: &OwnedPostgres,
    mapping_id: &str,
    issue_number: i64,
    lenso_comment_id: &str,
    github_comment_id: &str,
) -> Result<(), StorageError> {
    sqlx::query("INSERT INTO github_comment_bindings(mapping_id,lenso_comment_id,github_comment_id,issue_number) VALUES($1,$2,$3,$4) ON CONFLICT(mapping_id,lenso_comment_id) DO UPDATE SET github_comment_id=EXCLUDED.github_comment_id,issue_number=EXCLUDED.issue_number,updated_at=transaction_timestamp()")
        .bind(mapping_id).bind(lenso_comment_id).bind(github_comment_id).bind(issue_number).execute(postgres.pool()).await.map_err(|error| runtime("upsert comment binding", error))?;
    Ok(())
}

pub(crate) async fn comment_by_github_id(
    postgres: &OwnedPostgres,
    mapping_id: &str,
    github_comment_id: &str,
) -> Result<Option<String>, StorageError> {
    sqlx::query_scalar("SELECT lenso_comment_id FROM github_comment_bindings WHERE mapping_id=$1 AND github_comment_id=$2")
        .bind(mapping_id).bind(github_comment_id).fetch_optional(postgres.pool()).await.map_err(|error| runtime("find GitHub comment binding", error))
}

pub(crate) async fn comment_by_lenso_id(
    postgres: &OwnedPostgres,
    mapping_id: &str,
    lenso_comment_id: &str,
) -> Result<Option<(String, i64)>, StorageError> {
    let row = sqlx::query("SELECT github_comment_id,issue_number FROM github_comment_bindings WHERE mapping_id=$1 AND lenso_comment_id=$2")
        .bind(mapping_id).bind(lenso_comment_id).fetch_optional(postgres.pool()).await.map_err(|error| runtime("find Lenso comment binding", error))?;
    row.map(|row| {
        Ok((
            row.try_get("github_comment_id")
                .map_err(|error| runtime("decode comment binding", error))?,
            row.try_get("issue_number")
                .map_err(|error| runtime("decode comment binding", error))?,
        ))
    })
    .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revisions_are_positive_decimal_cas_values() {
        assert_eq!(parse_revision("7"), Ok(7));
        assert_eq!(parse_revision("0"), Err(DomainFailure::InvalidRequest));
        assert_eq!(parse_revision("01"), Err(DomainFailure::InvalidRequest));
        assert_eq!(parse_revision("-1"), Err(DomainFailure::InvalidRequest));
    }
}
