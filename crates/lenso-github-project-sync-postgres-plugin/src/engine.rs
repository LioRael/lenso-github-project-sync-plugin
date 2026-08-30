#![allow(clippy::too_many_lines)]

use std::collections::BTreeSet;

use lenso::prelude::Ctx;
use lenso_capability_projects as projects;
use lenso_capability_projects_collaboration as collaboration;
use lenso_postgres_kit::OwnedPostgres;
use serde::Deserialize;
use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{
    github::{GithubApi, GithubFailure, response_json},
    storage::{self, ClaimedDelivery, ClaimedJob, Mapping, Settings},
    webhook,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SyncFailure {
    pub code: &'static str,
    pub retryable: bool,
}

impl SyncFailure {
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

impl From<GithubFailure> for SyncFailure {
    fn from(value: GithubFailure) -> Self {
        Self {
            code: value.code,
            retryable: value.retryable,
        }
    }
}

impl From<storage::StorageError> for SyncFailure {
    fn from(value: storage::StorageError) -> Self {
        match value {
            storage::StorageError::Domain(storage::DomainFailure::MappingInactive) => {
                Self::fatal("unknown_mapping")
            }
            storage::StorageError::Domain(storage::DomainFailure::InstallationInactive) => {
                Self::fatal("installation_inactive")
            }
            storage::StorageError::Domain(storage::DomainFailure::LeaseLost) => {
                Self::fatal("lease_lost")
            }
            storage::StorageError::Domain(_) => Self::fatal("sync_state_conflict"),
            storage::StorageError::Runtime(_) => Self::retry("sync_storage_failure"),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct StateMapping {
    workflow_state_id: String,
    github_issue_state: String,
    github_project_option_id: Option<String>,
    inbound_default: bool,
}

#[derive(Clone, Debug, Deserialize)]
struct LabelMapping {
    lenso_label_id: String,
    github_label: String,
}

#[derive(Clone, Debug, Deserialize)]
struct MilestoneMapping {
    lenso_milestone_id: String,
    github_milestone_number: i64,
}

struct InboundIssueCreate<'a> {
    claim: &'a ClaimedDelivery,
    mapping: &'a Mapping,
    number: i64,
    node_id: &'a str,
    issue: &'a Value,
}

struct CommentMutation<'a> {
    claim: &'a ClaimedDelivery,
    mapping: &'a Mapping,
    issue_id: &'a str,
    comment_id: &'a str,
    github_comment: &'a Value,
    deleted: bool,
}

struct ProjectStatusMutation<'a> {
    mapping: &'a Mapping,
    issue_number: i64,
    node_id: &'a str,
    item_id: Option<&'a str>,
    option_id: Option<&'a str>,
}

fn state_mappings(mapping: &Mapping) -> Result<Vec<StateMapping>, SyncFailure> {
    serde_json::from_value(mapping.state_mappings.clone())
        .map_err(|_| SyncFailure::fatal("invalid_state_mapping"))
}
fn label_mappings(mapping: &Mapping) -> Result<Vec<LabelMapping>, SyncFailure> {
    serde_json::from_value(mapping.label_mappings.clone())
        .map_err(|_| SyncFailure::fatal("invalid_label_mapping"))
}
fn milestone_mappings(mapping: &Mapping) -> Result<Vec<MilestoneMapping>, SyncFailure> {
    serde_json::from_value(mapping.milestone_mappings.clone())
        .map_err(|_| SyncFailure::fatal("invalid_milestone_mapping"))
}

fn string_at<'a>(value: &'a Value, path: &[&str]) -> Result<&'a str, SyncFailure> {
    let mut current = value;
    for key in path {
        current = current
            .get(*key)
            .ok_or_else(|| SyncFailure::fatal("invalid_webhook_payload"))?;
    }
    current
        .as_str()
        .ok_or_else(|| SyncFailure::fatal("invalid_webhook_payload"))
}
fn i64_at(value: &Value, path: &[&str]) -> Result<i64, SyncFailure> {
    let mut current = value;
    for key in path {
        current = current
            .get(*key)
            .ok_or_else(|| SyncFailure::fatal("invalid_webhook_payload"))?;
    }
    current
        .as_i64()
        .ok_or_else(|| SyncFailure::fatal("invalid_webhook_payload"))
}
fn id_at(value: &Value, path: &[&str]) -> Result<String, SyncFailure> {
    let mut current = value;
    for key in path {
        current = current
            .get(*key)
            .ok_or_else(|| SyncFailure::fatal("invalid_webhook_payload"))?;
    }
    current
        .as_str()
        .map(ToOwned::to_owned)
        .or_else(|| current.as_i64().map(|value| value.to_string()))
        .ok_or_else(|| SyncFailure::fatal("invalid_webhook_payload"))
}

fn map_inbound_state(mapping: &Mapping, github_state: &str) -> Result<String, SyncFailure> {
    let matches = state_mappings(mapping)?
        .into_iter()
        .filter(|item| item.github_issue_state == github_state && item.inbound_default)
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return Err(SyncFailure::fatal("ambiguous_state_mapping"));
    }
    Ok(matches[0].workflow_state_id.clone())
}

fn mapped_labels(
    mapping: &Mapping,
    github_labels: &Value,
    current: &[String],
) -> Result<Vec<String>, SyncFailure> {
    let mappings = label_mappings(mapping)?;
    let managed = mappings
        .iter()
        .map(|item| item.lenso_label_id.as_str())
        .collect::<BTreeSet<_>>();
    let present = github_labels
        .as_array()
        .ok_or_else(|| SyncFailure::fatal("invalid_webhook_payload"))?
        .iter()
        .filter_map(|item| item.get("name").and_then(Value::as_str))
        .collect::<BTreeSet<_>>();
    let mut output = current
        .iter()
        .filter(|id| !managed.contains(id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    output.extend(
        mappings
            .into_iter()
            .filter(|item| present.contains(item.github_label.as_str()))
            .map(|item| item.lenso_label_id),
    );
    output.sort();
    output.dedup();
    Ok(output)
}

fn mapped_milestone(mapping: &Mapping, milestone: &Value) -> Result<Option<String>, SyncFailure> {
    if milestone.is_null() {
        return Ok(None);
    }
    let number = milestone
        .get("number")
        .and_then(Value::as_i64)
        .ok_or_else(|| SyncFailure::fatal("invalid_webhook_payload"))?;
    milestone_mappings(mapping)?
        .into_iter()
        .find(|item| item.github_milestone_number == number)
        .map(|item| Some(item.lenso_milestone_id))
        .ok_or_else(|| SyncFailure::fatal("unknown_milestone_mapping"))
}

fn project_failure(retryable: bool) -> SyncFailure {
    if retryable {
        SyncFailure::retry("projects_unavailable")
    } else {
        SyncFailure::fatal("projects_rejected")
    }
}

async fn get_issue(
    projects: &projects::ProjectsClient,
    context: &Ctx,
    organization_id: &str,
    issue_id: &str,
) -> Result<projects::GetIssueResponse, SyncFailure> {
    projects
        .get_issue_with_context(
            context.clone(),
            projects::GetIssueRequest {
                organization_id: organization_id.to_owned(),
                issue_ref: issue_id.to_owned(),
            },
        )
        .await
        .map_err(|error| match error {
            projects::ProjectsGetIssueInvocationError::Runtime(_) => project_failure(true),
            projects::ProjectsGetIssueInvocationError::Domain(_) => project_failure(false),
        })
}

async fn put_external_link(
    projects: &projects::ProjectsClient,
    context: &Ctx,
    mapping: &Mapping,
    delivery_id: &str,
    issue_id: &str,
    number: i64,
    url: &str,
) -> Result<(), SyncFailure> {
    projects
        .put_external_link_with_context(
            context.clone(),
            projects::PutExternalLinkRequest {
                idempotency_key: format!("github-delivery-{delivery_id}-external-link"),
                organization_id: mapping.organization_id.clone(),
                issue_id: issue_id.to_owned(),
                provider: "github".to_owned(),
                external_key: format!("{}:{number}", mapping.github_repository_id),
                url: url.to_owned(),
                title: Some(format!(
                    "{}/{}#{number}",
                    mapping.github_owner, mapping.github_repository
                )),
            },
        )
        .await
        .map(|_| ())
        .map_err(|error| match error {
            projects::ProjectsPutExternalLinkInvocationError::Runtime(_) => project_failure(true),
            projects::ProjectsPutExternalLinkInvocationError::Domain(_) => project_failure(false),
        })
}

pub(crate) async fn process_delivery(
    postgres: &OwnedPostgres,
    projects: &projects::ProjectsClient,
    collaboration: &collaboration::ProjectsCollaborationClient,
    api: &GithubApi<'_>,
    settings: &Settings,
    context: &Ctx,
    claim: &ClaimedDelivery,
) -> Result<(), SyncFailure> {
    let payload: Value = serde_json::from_slice(&claim.payload)
        .map_err(|_| SyncFailure::fatal("invalid_webhook_json"))?;
    match claim.event.as_str() {
        "ping" => Ok(()),
        "installation" => process_installation(postgres, &payload).await,
        "installation_repositories" => process_installation_repositories(postgres, &payload).await,
        "issues" => process_issue(postgres, projects, context, claim, &payload).await,
        "issue_comment" => process_comment(postgres, collaboration, context, claim, &payload).await,
        "projects_v2_item" => {
            process_project_item(postgres, projects, api, settings, context, claim, &payload).await
        }
        _ => Err(SyncFailure::fatal("unsupported_webhook_event")),
    }
}

async fn process_project_item(
    postgres: &OwnedPostgres,
    projects_client: &projects::ProjectsClient,
    api: &GithubApi<'_>,
    settings: &Settings,
    context: &Ctx,
    claim: &ClaimedDelivery,
    payload: &Value,
) -> Result<(), SyncFailure> {
    let action = string_at(payload, &["action"])?;
    if !matches!(action, "edited" | "created" | "converted") {
        return Ok(());
    }
    let installation_id = id_at(payload, &["installation", "id"])?;
    let item = payload
        .get("projects_v2_item")
        .ok_or_else(|| SyncFailure::fatal("invalid_webhook_payload"))?;
    let project_id = item
        .get("project_node_id")
        .or_else(|| item.get("project").and_then(|value| value.get("node_id")))
        .and_then(Value::as_str)
        .ok_or_else(|| SyncFailure::fatal("invalid_project_item_payload"))?;
    let item_id = item
        .get("node_id")
        .or_else(|| item.get("id"))
        .and_then(Value::as_str)
        .ok_or_else(|| SyncFailure::fatal("invalid_project_item_payload"))?;
    let mapping =
        storage::active_mapping_for_project(postgres, &installation_id, project_id).await?;
    let field_id = mapping
        .github_project_status_field_id
        .as_deref()
        .ok_or_else(|| SyncFailure::fatal("project_status_field_missing"))?;
    let value=api.graphql(settings,&mapping,"query($item:ID!){node(id:$item){... on ProjectV2Item{id content{... on Issue{id}} fieldValues(first:100){nodes{... on ProjectV2ItemFieldSingleSelectValue{optionId field{... on ProjectV2FieldCommon{id}}}}}}}}",json!({"item":item_id})).await?;
    let node = value
        .pointer("/data/node")
        .ok_or_else(|| SyncFailure::fatal("invalid_project_item_response"))?;
    let content_id = node
        .pointer("/content/id")
        .and_then(Value::as_str)
        .ok_or_else(|| SyncFailure::fatal("project_item_is_not_issue"))?;
    let option_id = node
        .pointer("/fieldValues/nodes")
        .and_then(Value::as_array)
        .and_then(|nodes| {
            nodes
                .iter()
                .find(|field| field.pointer("/field/id").and_then(Value::as_str) == Some(field_id))
        })
        .and_then(|field| field.get("optionId"))
        .and_then(Value::as_str)
        .ok_or_else(|| SyncFailure::fatal("project_status_value_missing"))?;
    let target = state_mappings(&mapping)?
        .into_iter()
        .filter(|state| state.github_project_option_id.as_deref() == Some(option_id))
        .collect::<Vec<_>>();
    if target.len() != 1 {
        return Err(SyncFailure::fatal("unknown_project_status_mapping"));
    }
    let binding = storage::binding_by_github_node(postgres, &mapping.mapping_id, content_id)
        .await?
        .ok_or_else(|| SyncFailure::retry("issue_binding_not_ready"))?;
    storage::set_project_item(
        postgres,
        &mapping.mapping_id,
        binding.github_issue_number,
        item_id,
    )
    .await?;
    for attempt in 0..3 {
        let current = get_issue(
            projects_client,
            context,
            &mapping.organization_id,
            &binding.lenso_issue_id,
        )
        .await?;
        if current.workflow_state_id == target[0].workflow_state_id {
            return Ok(());
        }
        let result = projects_client
            .update_issue_with_context(
                context.clone(),
                projects::UpdateIssueRequest {
                    cycle_id: current.cycle_id,
                    description: current.description,
                    expected_revision: current.revision,
                    idempotency_key: format!(
                        "github-delivery-{}-project-status-{attempt}",
                        claim.delivery_id
                    ),
                    issue_id: current.issue_id.clone(),
                    label_ids: current.label_ids,
                    milestone_id: current.milestone_id,
                    organization_id: mapping.organization_id.clone(),
                    parent_issue_id: current.parent_issue_id,
                    priority: current.priority,
                    title: current.title,
                    workflow_state_id: target[0].workflow_state_id.clone(),
                },
            )
            .await;
        match result {
            Ok(updated) => {
                if let Ok(revision) = updated.revision.parse() {
                    storage::save_inbound_suppression(
                        postgres,
                        &mapping.mapping_id,
                        "issue",
                        &updated.issue_id,
                        revision,
                        &claim.delivery_id,
                    )
                    .await?;
                }
                return Ok(());
            }
            Err(projects::ProjectsUpdateIssueInvocationError::Domain(
                projects::UpdateIssueError::RevisionConflict,
            )) => {}
            Err(projects::ProjectsUpdateIssueInvocationError::Runtime(_)) => {
                return Err(project_failure(true));
            }
            Err(projects::ProjectsUpdateIssueInvocationError::Domain(_)) => {
                return Err(project_failure(false));
            }
        }
    }
    Err(SyncFailure::retry("projects_revision_conflict"))
}

async fn process_installation(
    postgres: &OwnedPostgres,
    payload: &Value,
) -> Result<(), SyncFailure> {
    let action = string_at(payload, &["action"])?;
    let installation_id = id_at(payload, &["installation", "id"])?;
    let login = string_at(payload, &["installation", "account", "login"])?;
    let account_id = id_at(payload, &["installation", "account", "id"])?;
    let active = matches!(
        action,
        "created" | "unsuspended" | "new_permissions_accepted"
    );
    if !active && !matches!(action, "deleted" | "suspend") {
        return Ok(());
    }
    storage::apply_installation_event(postgres, &installation_id, login, &account_id, active)
        .await?;
    Ok(())
}

async fn process_installation_repositories(
    postgres: &OwnedPostgres,
    payload: &Value,
) -> Result<(), SyncFailure> {
    if string_at(payload, &["action"])? != "removed" {
        return Ok(());
    }
    let installation_id = id_at(payload, &["installation", "id"])?;
    let removed = payload
        .get("repositories_removed")
        .and_then(Value::as_array)
        .ok_or_else(|| SyncFailure::fatal("invalid_webhook_payload"))?;
    for repo in removed {
        let repository_id = repo
            .get("id")
            .and_then(|v| {
                v.as_i64()
                    .map(|n| n.to_string())
                    .or_else(|| v.as_str().map(ToOwned::to_owned))
            })
            .ok_or_else(|| SyncFailure::fatal("invalid_webhook_payload"))?;
        storage::disable_repository_mapping_event(postgres, &installation_id, &repository_id)
            .await?;
    }
    Ok(())
}

fn clean_body(body: Option<&str>) -> Option<String> {
    let body = body?;
    let cleaned = if let Some(start) = body.rfind("<!-- lenso-github-sync:") {
        &body[..start]
    } else {
        body
    };
    let cleaned = cleaned.trim_end();
    (!cleaned.is_empty()).then(|| cleaned.to_owned())
}

fn parse_time(value: &str) -> Result<OffsetDateTime, SyncFailure> {
    OffsetDateTime::parse(value, &Rfc3339)
        .map_err(|_| SyncFailure::fatal("invalid_webhook_timestamp"))
}

async fn process_issue(
    postgres: &OwnedPostgres,
    projects_client: &projects::ProjectsClient,
    context: &Ctx,
    claim: &ClaimedDelivery,
    payload: &Value,
) -> Result<(), SyncFailure> {
    if payload
        .get("issue")
        .and_then(|issue| issue.get("pull_request"))
        .is_some()
    {
        return Err(SyncFailure::fatal("pull_request_semantics_unavailable"));
    }
    let action = string_at(payload, &["action"])?;
    if !matches!(
        action,
        "opened"
            | "edited"
            | "closed"
            | "reopened"
            | "deleted"
            | "labeled"
            | "unlabeled"
            | "milestoned"
            | "demilestoned"
    ) {
        return Ok(());
    }
    let installation_id = id_at(payload, &["installation", "id"])?;
    let repository_id = id_at(payload, &["repository", "id"])?;
    let mapping =
        storage::active_mapping_for_repository(postgres, &installation_id, &repository_id, true)
            .await?;
    let issue = payload
        .get("issue")
        .ok_or_else(|| SyncFailure::fatal("invalid_webhook_payload"))?;
    let number = i64_at(payload, &["issue", "number"])?;
    let node_id = string_at(payload, &["issue", "node_id"])?;
    let marker = webhook::find_origin_marker(issue.get("body").and_then(Value::as_str));
    if let Some(marker) = marker.filter(|marker| !marker.is_empty())
        && storage::known_origin_marker(postgres, marker, &mapping.mapping_id).await?
    {
        let _ = storage::record_effect(
            postgres,
            &claim.delivery_id,
            "loop-suppressed",
            &json!({"marker":marker}),
        )
        .await?;
        return Ok(());
    }
    let effect_key = format!("issue:{node_id}:{action}");
    if storage::effect_exists(postgres, &claim.delivery_id, &effect_key).await? {
        return Ok(());
    }
    let binding = storage::binding_by_github_issue(postgres, &mapping.mapping_id, number).await?;
    if action == "deleted" {
        let Some(binding) = binding else {
            return Err(SyncFailure::fatal("unknown_issue_binding"));
        };
        let current = get_issue(
            projects_client,
            context,
            &mapping.organization_id,
            &binding.lenso_issue_id,
        )
        .await?;
        let archived = projects_client
            .archive_issue_with_context(
                context.clone(),
                projects::ArchiveIssueRequest {
                    archived: true,
                    expected_revision: current.revision,
                    idempotency_key: format!("github-delivery-{}-archive", claim.delivery_id),
                    issue_id: binding.lenso_issue_id,
                    organization_id: mapping.organization_id.clone(),
                },
            )
            .await
            .map_err(|error| match error {
                projects::ProjectsArchiveIssueInvocationError::Runtime(_) => project_failure(true),
                projects::ProjectsArchiveIssueInvocationError::Domain(_) => project_failure(false),
            })?;
        if let Ok(revision) = archived.revision.parse() {
            storage::save_inbound_suppression(
                postgres,
                &mapping.mapping_id,
                "issue",
                &archived.issue_id,
                revision,
                &claim.delivery_id,
            )
            .await?;
        }
        storage::record_effect(
            postgres,
            &claim.delivery_id,
            &effect_key,
            &json!({"archived":true}),
        )
        .await?;
        return Ok(());
    }
    if let Some(binding) = binding {
        update_lenso_issue_from_github(
            postgres,
            projects_client,
            context,
            claim,
            &mapping,
            &binding.lenso_issue_id,
            issue,
        )
        .await?;
    } else {
        create_lenso_issue_from_github(
            postgres,
            projects_client,
            context,
            InboundIssueCreate {
                claim,
                mapping: &mapping,
                number,
                node_id,
                issue,
            },
        )
        .await?;
    }
    storage::record_effect(
        postgres,
        &claim.delivery_id,
        &effect_key,
        &json!({"lenso_synced":true}),
    )
    .await?;
    Ok(())
}

async fn create_lenso_issue_from_github(
    postgres: &OwnedPostgres,
    projects_client: &projects::ProjectsClient,
    context: &Ctx,
    request: InboundIssueCreate<'_>,
) -> Result<(), SyncFailure> {
    let InboundIssueCreate {
        claim,
        mapping,
        number,
        node_id,
        issue,
    } = request;
    let title = issue
        .get("title")
        .and_then(Value::as_str)
        .ok_or_else(|| SyncFailure::fatal("invalid_webhook_payload"))?;
    let state = issue
        .get("state")
        .and_then(Value::as_str)
        .ok_or_else(|| SyncFailure::fatal("invalid_webhook_payload"))?;
    let workflow_state_id = map_inbound_state(mapping, state)?;
    let labels = mapped_labels(
        mapping,
        issue.get("labels").unwrap_or(&Value::Array(Vec::new())),
        &[],
    )?;
    let milestone = mapped_milestone(mapping, issue.get("milestone").unwrap_or(&Value::Null))?;
    let issue_id = uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        format!("lenso-github:{}:{node_id}", mapping.mapping_id).as_bytes(),
    )
    .to_string();
    let created = projects_client
        .create_issue_with_context(
            context.clone(),
            projects::CreateIssueRequest {
                cycle_id: None,
                description: clean_body(issue.get("body").and_then(Value::as_str)),
                idempotency_key: format!("github-delivery-{}-create", claim.delivery_id),
                issue_id: issue_id.clone(),
                label_ids: labels,
                milestone_id: milestone,
                organization_id: mapping.organization_id.clone(),
                parent_issue_id: None,
                priority: projects::Priority::None,
                project_id: mapping.project_id.clone(),
                team_id: mapping.team_id.clone(),
                title: title.to_owned(),
                workflow_state_id: Some(workflow_state_id),
            },
        )
        .await
        .map_err(|error| match error {
            projects::ProjectsCreateIssueInvocationError::Runtime(_) => project_failure(true),
            projects::ProjectsCreateIssueInvocationError::Domain(_) => project_failure(false),
        })?;
    let url = issue
        .get("html_url")
        .and_then(Value::as_str)
        .ok_or_else(|| SyncFailure::fatal("invalid_webhook_payload"))?;
    put_external_link(
        projects_client,
        context,
        mapping,
        &claim.delivery_id,
        &created.issue_id,
        number,
        url,
    )
    .await?;
    let updated = issue
        .get("updated_at")
        .and_then(Value::as_str)
        .map(parse_time)
        .transpose()?;
    storage::upsert_issue_binding(
        postgres,
        mapping,
        storage::IssueBindingWrite {
            github_issue_number: number,
            github_node_id: node_id,
            lenso_issue_id: &created.issue_id,
            github_project_item_id: None,
            github_updated_at: updated,
            lenso_revision: created.revision.parse().ok(),
        },
    )
    .await?;
    if let Ok(revision) = created.revision.parse() {
        storage::save_inbound_suppression(
            postgres,
            &mapping.mapping_id,
            "issue",
            &created.issue_id,
            revision,
            &claim.delivery_id,
        )
        .await?;
    }
    Ok(())
}

fn timestamp_value<T: serde::Serialize>(value: &T) -> Option<String> {
    serde_json::to_value(value)
        .ok()?
        .as_str()
        .map(ToOwned::to_owned)
}

async fn update_lenso_issue_from_github(
    postgres: &OwnedPostgres,
    projects_client: &projects::ProjectsClient,
    context: &Ctx,
    claim: &ClaimedDelivery,
    mapping: &Mapping,
    issue_id: &str,
    github_issue: &Value,
) -> Result<(), SyncFailure> {
    if mapping.conflict_policy == "lenso_wins" {
        return Ok(());
    }
    if mapping.conflict_policy == "manual" {
        return Err(SyncFailure::fatal("manual_conflict"));
    }
    let github_updated_text = github_issue
        .get("updated_at")
        .and_then(Value::as_str)
        .ok_or_else(|| SyncFailure::fatal("invalid_webhook_payload"))?;
    let github_updated = parse_time(github_updated_text)?;
    for attempt in 0..3 {
        let current =
            get_issue(projects_client, context, &mapping.organization_id, issue_id).await?;
        if mapping.conflict_policy == "latest_updated_at" {
            let lenso_updated = timestamp_value(&current.updated_at)
                .and_then(|value| OffsetDateTime::parse(&value, &Rfc3339).ok())
                .ok_or_else(|| SyncFailure::fatal("invalid_projects_timestamp"))?;
            if lenso_updated > github_updated {
                return Ok(());
            }
        }
        let title = github_issue
            .get("title")
            .and_then(Value::as_str)
            .ok_or_else(|| SyncFailure::fatal("invalid_webhook_payload"))?
            .to_owned();
        let description = clean_body(github_issue.get("body").and_then(Value::as_str));
        let workflow_state_id = map_inbound_state(
            mapping,
            github_issue
                .get("state")
                .and_then(Value::as_str)
                .ok_or_else(|| SyncFailure::fatal("invalid_webhook_payload"))?,
        )?;
        let labels = mapped_labels(
            mapping,
            github_issue
                .get("labels")
                .unwrap_or(&Value::Array(Vec::new())),
            &current.label_ids,
        )?;
        let milestone = if milestone_mappings(mapping)?.is_empty() {
            current.milestone_id.clone()
        } else {
            mapped_milestone(
                mapping,
                github_issue.get("milestone").unwrap_or(&Value::Null),
            )?
        };
        let expected = current.revision.clone();
        let result = projects_client
            .update_issue_with_context(
                context.clone(),
                projects::UpdateIssueRequest {
                    cycle_id: current.cycle_id,
                    description,
                    expected_revision: expected,
                    idempotency_key: format!(
                        "github-delivery-{}-update-{attempt}",
                        claim.delivery_id
                    ),
                    issue_id: current.issue_id.clone(),
                    label_ids: labels,
                    milestone_id: milestone,
                    organization_id: mapping.organization_id.clone(),
                    parent_issue_id: current.parent_issue_id,
                    priority: current.priority,
                    title,
                    workflow_state_id,
                },
            )
            .await;
        match result {
            Ok(updated) => {
                storage::upsert_issue_binding(
                    postgres,
                    mapping,
                    storage::IssueBindingWrite {
                        github_issue_number: i64_at(github_issue, &["number"])?,
                        github_node_id: string_at(github_issue, &["node_id"])?,
                        lenso_issue_id: &updated.issue_id,
                        github_project_item_id: None,
                        github_updated_at: Some(github_updated),
                        lenso_revision: updated.revision.parse().ok(),
                    },
                )
                .await?;
                if let Ok(revision) = updated.revision.parse() {
                    storage::save_inbound_suppression(
                        postgres,
                        &mapping.mapping_id,
                        "issue",
                        &updated.issue_id,
                        revision,
                        &claim.delivery_id,
                    )
                    .await?;
                }
                return Ok(());
            }
            Err(projects::ProjectsUpdateIssueInvocationError::Domain(
                projects::UpdateIssueError::RevisionConflict,
            )) => {}
            Err(projects::ProjectsUpdateIssueInvocationError::Runtime(_)) => {
                return Err(project_failure(true));
            }
            Err(projects::ProjectsUpdateIssueInvocationError::Domain(_)) => {
                return Err(project_failure(false));
            }
        }
    }
    Err(SyncFailure::retry("projects_revision_conflict"))
}

async fn process_comment(
    postgres: &OwnedPostgres,
    collaboration_client: &collaboration::ProjectsCollaborationClient,
    context: &Ctx,
    claim: &ClaimedDelivery,
    payload: &Value,
) -> Result<(), SyncFailure> {
    let action = string_at(payload, &["action"])?;
    if !matches!(action, "created" | "edited" | "deleted") {
        return Ok(());
    }
    let installation_id = id_at(payload, &["installation", "id"])?;
    let repository_id = id_at(payload, &["repository", "id"])?;
    let mapping =
        storage::active_mapping_for_repository(postgres, &installation_id, &repository_id, true)
            .await?;
    let issue_number = i64_at(payload, &["issue", "number"])?;
    let binding = storage::binding_by_github_issue(postgres, &mapping.mapping_id, issue_number)
        .await?
        .ok_or_else(|| SyncFailure::retry("issue_binding_not_ready"))?;
    let comment = payload
        .get("comment")
        .ok_or_else(|| SyncFailure::fatal("invalid_webhook_payload"))?;
    let github_comment_id = id_at(comment, &["id"])?;
    if let Some(marker) = webhook::find_origin_marker(comment.get("body").and_then(Value::as_str))
        && storage::known_origin_marker(postgres, marker, &mapping.mapping_id).await?
    {
        let _ = storage::record_effect(
            postgres,
            &claim.delivery_id,
            "loop-suppressed-comment",
            &json!({"marker":marker}),
        )
        .await?;
        return Ok(());
    }
    let effect_key = format!("comment:{github_comment_id}:{action}");
    if storage::effect_exists(postgres, &claim.delivery_id, &effect_key).await? {
        return Ok(());
    }
    match action {
        "created" => {
            let comment_id = uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_URL,
                format!(
                    "lenso-github-comment:{}:{github_comment_id}",
                    mapping.mapping_id
                )
                .as_bytes(),
            )
            .to_string();
            let body = clean_body(comment.get("body").and_then(Value::as_str))
                .unwrap_or_else(|| "(empty GitHub comment)".to_owned());
            let created = collaboration_client
                .add_comment_with_context(
                    context.clone(),
                    collaboration::AddCommentRequest {
                        body,
                        comment_id: comment_id.clone(),
                        idempotency_key: format!(
                            "github-delivery-{}-comment-create",
                            claim.delivery_id
                        ),
                        issue_id: binding.lenso_issue_id,
                        organization_id: mapping.organization_id.clone(),
                    },
                )
                .await
                .map_err(|error| match error {
                    collaboration::ProjectsCollaborationAddCommentInvocationError::Runtime(_) => {
                        project_failure(true)
                    }
                    collaboration::ProjectsCollaborationAddCommentInvocationError::Domain(_) => {
                        project_failure(false)
                    }
                })?;
            storage::upsert_comment_binding(
                postgres,
                &mapping.mapping_id,
                issue_number,
                &created.comment_id,
                &github_comment_id,
            )
            .await?;
            if let Ok(revision) = created.revision.parse() {
                storage::save_inbound_suppression(
                    postgres,
                    &mapping.mapping_id,
                    "comment",
                    &created.comment_id,
                    revision,
                    &claim.delivery_id,
                )
                .await?;
            }
        }
        "edited" | "deleted" => {
            let comment_id =
                storage::comment_by_github_id(postgres, &mapping.mapping_id, &github_comment_id)
                    .await?
                    .ok_or_else(|| SyncFailure::retry("comment_binding_not_ready"))?;
            mutate_lenso_comment(
                collaboration_client,
                context,
                CommentMutation {
                    claim,
                    mapping: &mapping,
                    issue_id: &binding.lenso_issue_id,
                    comment_id: &comment_id,
                    github_comment: comment,
                    deleted: action == "deleted",
                },
            )
            .await?;
            let updated = find_comment(
                collaboration_client,
                context,
                &mapping.organization_id,
                &binding.lenso_issue_id,
                &comment_id,
            )
            .await?;
            if let Ok(revision) = updated.revision.parse() {
                storage::save_inbound_suppression(
                    postgres,
                    &mapping.mapping_id,
                    "comment",
                    &comment_id,
                    revision,
                    &claim.delivery_id,
                )
                .await?;
            }
        }
        _ => {}
    }
    storage::record_effect(
        postgres,
        &claim.delivery_id,
        &effect_key,
        &json!({"lenso_synced":true}),
    )
    .await?;
    Ok(())
}

async fn find_comment(
    collaboration_client: &collaboration::ProjectsCollaborationClient,
    context: &Ctx,
    organization_id: &str,
    issue_id: &str,
    comment_id: &str,
) -> Result<collaboration::Comment, SyncFailure> {
    let mut after = None;
    for _ in 0..20 {
        let page = collaboration_client
            .list_comments_with_context(
                context.clone(),
                collaboration::ListCommentsRequest {
                    after: after.clone(),
                    limit: 100,
                    organization_id: organization_id.to_owned(),
                    issue_id: issue_id.to_owned(),
                },
            )
            .await
            .map_err(|error| match error {
                collaboration::ProjectsCollaborationListCommentsInvocationError::Runtime(_) => {
                    project_failure(true)
                }
                collaboration::ProjectsCollaborationListCommentsInvocationError::Domain(_) => {
                    project_failure(false)
                }
            })?;
        if let Some(comment) = page
            .items
            .into_iter()
            .find(|item| item.comment_id == comment_id)
        {
            return Ok(comment);
        }
        if page.next_cursor == after || page.next_cursor.is_none() {
            break;
        }
        after = page.next_cursor;
    }
    Err(SyncFailure::retry("comment_not_ready"))
}

async fn mutate_lenso_comment(
    collaboration_client: &collaboration::ProjectsCollaborationClient,
    context: &Ctx,
    request: CommentMutation<'_>,
) -> Result<(), SyncFailure> {
    let CommentMutation {
        claim,
        mapping,
        issue_id,
        comment_id,
        github_comment,
        deleted,
    } = request;
    for attempt in 0..3 {
        let current = find_comment(
            collaboration_client,
            context,
            &mapping.organization_id,
            issue_id,
            comment_id,
        )
        .await?;
        let result = if deleted {
            collaboration_client
                .delete_comment_with_context(
                    context.clone(),
                    collaboration::DeleteCommentRequest {
                        comment_id: comment_id.to_owned(),
                        expected_revision: current.revision,
                        idempotency_key: format!(
                            "github-delivery-{}-comment-delete-{attempt}",
                            claim.delivery_id
                        ),
                        organization_id: mapping.organization_id.clone(),
                    },
                )
                .await
                .map(|_| ())
                .map_err(|error| match error {
                    collaboration::ProjectsCollaborationDeleteCommentInvocationError::Domain(
                        collaboration::DeleteCommentError::RevisionConflict,
                    ) => SyncFailure::retry("projects_revision_conflict"),
                    collaboration::ProjectsCollaborationDeleteCommentInvocationError::Runtime(
                        _,
                    ) => project_failure(true),
                    collaboration::ProjectsCollaborationDeleteCommentInvocationError::Domain(_) => {
                        project_failure(false)
                    }
                })
        } else {
            let body = clean_body(github_comment.get("body").and_then(Value::as_str))
                .unwrap_or_else(|| "(empty GitHub comment)".to_owned());
            collaboration_client
                .update_comment_with_context(
                    context.clone(),
                    collaboration::UpdateCommentRequest {
                        body,
                        comment_id: comment_id.to_owned(),
                        expected_revision: current.revision,
                        idempotency_key: format!(
                            "github-delivery-{}-comment-update-{attempt}",
                            claim.delivery_id
                        ),
                        organization_id: mapping.organization_id.clone(),
                    },
                )
                .await
                .map(|_| ())
                .map_err(|error| match error {
                    collaboration::ProjectsCollaborationUpdateCommentInvocationError::Domain(
                        collaboration::UpdateCommentError::RevisionConflict,
                    ) => SyncFailure::retry("projects_revision_conflict"),
                    collaboration::ProjectsCollaborationUpdateCommentInvocationError::Runtime(
                        _,
                    ) => project_failure(true),
                    collaboration::ProjectsCollaborationUpdateCommentInvocationError::Domain(_) => {
                        project_failure(false)
                    }
                })
        };
        match result {
            Ok(()) => return Ok(()),
            Err(error) if error.code == "projects_revision_conflict" => {}
            Err(error) => return Err(error),
        }
    }
    Err(SyncFailure::retry("projects_revision_conflict"))
}

pub(crate) async fn stage_project_activity(
    postgres: &OwnedPostgres,
    projects_client: &projects::ProjectsClient,
    context: &Ctx,
    limit: i64,
) -> Result<i64, SyncFailure> {
    let mappings = storage::active_outbound_mappings(postgres).await?;
    let mut staged = 0_i64;
    for mapping in mappings {
        let cursor = storage::checkpoint_cursor(postgres, &mapping.mapping_id).await?;
        let page = projects_client
            .list_activity_with_context(
                context.clone(),
                projects::ListActivityRequest {
                    after: cursor.clone(),
                    issue_id: None,
                    limit,
                    organization_id: mapping.organization_id.clone(),
                    project_id: Some(mapping.project_id.clone()),
                },
            )
            .await
            .map_err(|error| match error {
                projects::ProjectsListActivityInvocationError::Runtime(_) => project_failure(true),
                projects::ProjectsListActivityInvocationError::Domain(_) => project_failure(false),
            })?;
        let mut items = Vec::with_capacity(page.items.len());
        for item in page.items {
            let revision = item
                .revision
                .as_deref()
                .map(|value| {
                    value
                        .parse::<i64>()
                        .map_err(|_| SyncFailure::fatal("invalid_projects_revision"))
                })
                .transpose()?;
            items.push(storage::ActivityStub {
                activity_id: item.activity_id,
                entity_kind: item.entity_kind,
                entity_id: item.entity_id,
                issue_id: item.issue_id,
                operation: item.operation,
                revision,
            });
        }
        staged += storage::stage_activity_page(
            postgres,
            &mapping.mapping_id,
            cursor.as_deref(),
            page.next_cursor.as_deref(),
            &items,
        )
        .await?;
    }
    Ok(staged)
}

pub(crate) async fn process_outbound_job(
    postgres: &OwnedPostgres,
    projects_client: &projects::ProjectsClient,
    collaboration_client: &collaboration::ProjectsCollaborationClient,
    api: &GithubApi<'_>,
    settings: &Settings,
    context: &Ctx,
    claim: &ClaimedJob,
) -> Result<Value, SyncFailure> {
    let mapping = storage::mapping_by_id(postgres, &claim.mapping_id).await?;
    let mut receipt = match claim.entity_kind.as_str() {
        "issue" => {
            export_issue(
                postgres,
                projects_client,
                api,
                settings,
                context,
                claim,
                &mapping,
            )
            .await
        }
        "comment" => {
            export_comment(
                postgres,
                collaboration_client,
                api,
                settings,
                context,
                claim,
                &mapping,
            )
            .await
        }
        _ => Err(SyncFailure::fatal("semantic_not_mapped")),
    }?;
    if let Some(object) = receipt.as_object_mut() {
        object.insert(
            "activity_id".to_owned(),
            Value::String(claim.activity_id.clone()),
        );
        object.insert(
            "operation".to_owned(),
            Value::String(claim.operation.clone()),
        );
        object.insert(
            "lenso_revision".to_owned(),
            claim
                .lenso_revision
                .map_or(Value::Null, |value| Value::String(value.to_string())),
        );
    }
    Ok(receipt)
}

fn outbound_fields(
    mapping: &Mapping,
    issue: &projects::GetIssueResponse,
) -> Result<Value, SyncFailure> {
    let states = state_mappings(mapping)?;
    let state = states
        .iter()
        .find(|item| item.workflow_state_id == issue.workflow_state_id)
        .ok_or_else(|| SyncFailure::fatal("unknown_state_mapping"))?;
    let labels_map = label_mappings(mapping)?;
    let labels = labels_map
        .into_iter()
        .filter(|item| issue.label_ids.contains(&item.lenso_label_id))
        .map(|item| Value::String(item.github_label))
        .collect::<Vec<_>>();
    let milestone = match issue.milestone_id.as_deref() {
        None => Value::Null,
        Some(id) => Value::Number(
            milestone_mappings(mapping)?
                .into_iter()
                .find(|item| item.lenso_milestone_id == id)
                .ok_or_else(|| SyncFailure::fatal("unknown_milestone_mapping"))?
                .github_milestone_number
                .into(),
        ),
    };
    Ok(
        json!({"state":state.github_issue_state,"labels":labels,"milestone":milestone,"project_option_id":state.github_project_option_id}),
    )
}

async fn find_recent_issue_by_marker(
    api: &GithubApi<'_>,
    settings: &Settings,
    mapping: &Mapping,
    marker: &str,
) -> Result<Option<Value>, SyncFailure> {
    let path = format!(
        "/repos/{}/{}/issues?state=all&sort=created&direction=desc&per_page=100",
        mapping.github_owner, mapping.github_repository
    );
    let response = api.rest(settings, mapping, "GET", &path, None).await?;
    let value = response_json(&response, &[200])?;
    Ok(value.as_array().and_then(|items| {
        items
            .iter()
            .find(|issue| {
                webhook::find_origin_marker(issue.get("body").and_then(Value::as_str))
                    == Some(marker)
            })
            .cloned()
    }))
}

async fn export_issue(
    postgres: &OwnedPostgres,
    projects_client: &projects::ProjectsClient,
    api: &GithubApi<'_>,
    settings: &Settings,
    context: &Ctx,
    claim: &ClaimedJob,
    mapping: &Mapping,
) -> Result<Value, SyncFailure> {
    let issue = get_issue(
        projects_client,
        context,
        &mapping.organization_id,
        &claim.entity_id,
    )
    .await?;
    let fields = outbound_fields(mapping, &issue)?;
    let marker = webhook::origin_marker(&claim.job_id);
    storage::save_origin_marker(postgres, &marker, &mapping.mapping_id, &claim.job_id, None)
        .await?;
    let body = webhook::append_origin_marker(issue.description.as_deref(), &marker);
    let mut binding =
        storage::binding_by_lenso_issue(postgres, &mapping.mapping_id, &issue.issue_id).await?;
    if binding.is_none()
        && let Some(found) = find_recent_issue_by_marker(api, settings, mapping, &marker).await?
    {
        let number = found
            .get("number")
            .and_then(Value::as_i64)
            .ok_or_else(|| SyncFailure::fatal("invalid_github_response"))?;
        let node_id = found
            .get("node_id")
            .and_then(Value::as_str)
            .ok_or_else(|| SyncFailure::fatal("invalid_github_response"))?;
        storage::upsert_issue_binding(
            postgres,
            mapping,
            storage::IssueBindingWrite {
                github_issue_number: number,
                github_node_id: node_id,
                lenso_issue_id: &issue.issue_id,
                github_project_item_id: None,
                github_updated_at: None,
                lenso_revision: issue.revision.parse().ok(),
            },
        )
        .await?;
        binding =
            storage::binding_by_lenso_issue(postgres, &mapping.mapping_id, &issue.issue_id).await?;
    }
    if let Some(existing) = binding {
        let path = format!(
            "/repos/{}/{}/issues/{}",
            mapping.github_owner, mapping.github_repository, existing.github_issue_number
        );
        let response=api.rest(settings,mapping,"PATCH",&path,Some(&json!({"title":issue.title,"body":body,"state":if issue.archived{"closed"}else{fields["state"].as_str().unwrap_or("open")},"labels":fields["labels"],"milestone":fields["milestone"]}))).await?;
        let remote = response_json(&response, &[200])?;
        let updated = remote
            .get("updated_at")
            .and_then(Value::as_str)
            .map(parse_time)
            .transpose()?;
        storage::upsert_issue_binding(
            postgres,
            mapping,
            storage::IssueBindingWrite {
                github_issue_number: existing.github_issue_number,
                github_node_id: &existing.github_node_id,
                lenso_issue_id: &issue.issue_id,
                github_project_item_id: existing.github_project_item_id.as_deref(),
                github_updated_at: updated,
                lenso_revision: issue.revision.parse().ok(),
            },
        )
        .await?;
        ensure_project_status(
            postgres,
            api,
            settings,
            ProjectStatusMutation {
                mapping,
                issue_number: existing.github_issue_number,
                node_id: &existing.github_node_id,
                item_id: existing.github_project_item_id.as_deref(),
                option_id: fields["project_option_id"].as_str(),
            },
        )
        .await?;
        put_external_link(
            projects_client,
            context,
            mapping,
            &claim.job_id,
            &issue.issue_id,
            existing.github_issue_number,
            remote
                .get("html_url")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        )
        .await?;
        return Ok(
            json!({"github_issue_number":existing.github_issue_number,"github_node_id":existing.github_node_id,"action":"updated"}),
        );
    }
    let path = format!(
        "/repos/{}/{}/issues",
        mapping.github_owner, mapping.github_repository
    );
    let response=api.rest(settings,mapping,"POST",&path,Some(&json!({"title":issue.title,"body":body,"labels":fields["labels"],"milestone":fields["milestone"]}))).await?;
    let remote = response_json(&response, &[201])?;
    let number = remote
        .get("number")
        .and_then(Value::as_i64)
        .ok_or_else(|| SyncFailure::fatal("invalid_github_response"))?;
    let node_id = remote
        .get("node_id")
        .and_then(Value::as_str)
        .ok_or_else(|| SyncFailure::fatal("invalid_github_response"))?;
    let updated = remote
        .get("updated_at")
        .and_then(Value::as_str)
        .map(parse_time)
        .transpose()?;
    storage::upsert_issue_binding(
        postgres,
        mapping,
        storage::IssueBindingWrite {
            github_issue_number: number,
            github_node_id: node_id,
            lenso_issue_id: &issue.issue_id,
            github_project_item_id: None,
            github_updated_at: updated,
            lenso_revision: issue.revision.parse().ok(),
        },
    )
    .await?;
    ensure_project_status(
        postgres,
        api,
        settings,
        ProjectStatusMutation {
            mapping,
            issue_number: number,
            node_id,
            item_id: None,
            option_id: fields["project_option_id"].as_str(),
        },
    )
    .await?;
    put_external_link(
        projects_client,
        context,
        mapping,
        &claim.job_id,
        &issue.issue_id,
        number,
        remote
            .get("html_url")
            .and_then(Value::as_str)
            .ok_or_else(|| SyncFailure::fatal("invalid_github_response"))?,
    )
    .await?;
    Ok(json!({"github_issue_number":number,"github_node_id":node_id,"action":"created"}))
}

async fn ensure_project_status(
    postgres: &OwnedPostgres,
    api: &GithubApi<'_>,
    settings: &Settings,
    request: ProjectStatusMutation<'_>,
) -> Result<(), SyncFailure> {
    let ProjectStatusMutation {
        mapping,
        issue_number,
        node_id,
        item_id,
        option_id,
    } = request;
    let Some(project_id) = mapping.github_project_id.as_deref() else {
        return Ok(());
    };
    let Some(field_id) = mapping.github_project_status_field_id.as_deref() else {
        return Err(SyncFailure::fatal("project_status_field_missing"));
    };
    let Some(option_id) = option_id else {
        return Err(SyncFailure::fatal("project_status_option_missing"));
    };
    let item_id = if let Some(item_id) = item_id {
        item_id.to_owned()
    } else {
        let value=api.graphql(settings,mapping,"mutation($project:ID!,$content:ID!){addProjectV2ItemById(input:{projectId:$project,contentId:$content}){item{id}}}",json!({"project":project_id,"content":node_id})).await?;
        let item = value
            .pointer("/data/addProjectV2ItemById/item/id")
            .and_then(Value::as_str)
            .ok_or_else(|| SyncFailure::fatal("invalid_project_item_response"))?
            .to_owned();
        storage::set_project_item(postgres, &mapping.mapping_id, issue_number, &item).await?;
        item
    };
    api.graphql(settings,mapping,"mutation($project:ID!,$item:ID!,$field:ID!,$option:String!){updateProjectV2ItemFieldValue(input:{projectId:$project,itemId:$item,fieldId:$field,value:{singleSelectOptionId:$option}}){projectV2Item{id}}}",json!({"project":project_id,"item":item_id,"field":field_id,"option":option_id})).await?;
    Ok(())
}

async fn find_recent_comment_by_marker(
    api: &GithubApi<'_>,
    settings: &Settings,
    mapping: &Mapping,
    number: i64,
    marker: &str,
) -> Result<Option<Value>, SyncFailure> {
    let path = format!(
        "/repos/{}/{}/issues/{number}/comments?per_page=100",
        mapping.github_owner, mapping.github_repository
    );
    let response = api.rest(settings, mapping, "GET", &path, None).await?;
    let value = response_json(&response, &[200])?;
    Ok(value.as_array().and_then(|items| {
        items
            .iter()
            .find(|comment| {
                webhook::find_origin_marker(comment.get("body").and_then(Value::as_str))
                    == Some(marker)
            })
            .cloned()
    }))
}

async fn export_comment(
    postgres: &OwnedPostgres,
    collaboration_client: &collaboration::ProjectsCollaborationClient,
    api: &GithubApi<'_>,
    settings: &Settings,
    context: &Ctx,
    claim: &ClaimedJob,
    mapping: &Mapping,
) -> Result<Value, SyncFailure> {
    let issue_id = claim
        .issue_id
        .as_deref()
        .ok_or_else(|| SyncFailure::fatal("comment_issue_missing"))?;
    let issue_binding = storage::binding_by_lenso_issue(postgres, &mapping.mapping_id, issue_id)
        .await?
        .ok_or_else(|| SyncFailure::retry("issue_binding_not_ready"))?;
    let comment = find_comment(
        collaboration_client,
        context,
        &mapping.organization_id,
        issue_id,
        &claim.entity_id,
    )
    .await?;
    let marker = webhook::origin_marker(&claim.job_id);
    storage::save_origin_marker(postgres, &marker, &mapping.mapping_id, &claim.job_id, None)
        .await?;
    let mut remote_binding =
        storage::comment_by_lenso_id(postgres, &mapping.mapping_id, &comment.comment_id).await?;
    if remote_binding.is_none()
        && !comment.deleted
        && let Some(found) = find_recent_comment_by_marker(
            api,
            settings,
            mapping,
            issue_binding.github_issue_number,
            &marker,
        )
        .await?
    {
        let id = found
            .get("id")
            .and_then(|value| {
                value
                    .as_i64()
                    .map(|n| n.to_string())
                    .or_else(|| value.as_str().map(ToOwned::to_owned))
            })
            .ok_or_else(|| SyncFailure::fatal("invalid_github_response"))?;
        storage::upsert_comment_binding(
            postgres,
            &mapping.mapping_id,
            issue_binding.github_issue_number,
            &comment.comment_id,
            &id,
        )
        .await?;
        remote_binding = Some((id, issue_binding.github_issue_number));
    }
    if let Some((github_comment_id, number)) = remote_binding {
        if comment.deleted {
            let path = format!(
                "/repos/{}/{}/issues/comments/{github_comment_id}",
                mapping.github_owner, mapping.github_repository
            );
            let response = api.rest(settings, mapping, "DELETE", &path, None).await?;
            response_json(&response, &[204])?;
            return Ok(json!({"github_comment_id":github_comment_id,"action":"deleted"}));
        }
        let path = format!(
            "/repos/{}/{}/issues/comments/{github_comment_id}",
            mapping.github_owner, mapping.github_repository
        );
        let body = webhook::append_origin_marker(Some(&comment.body), &marker);
        let response = api
            .rest(
                settings,
                mapping,
                "PATCH",
                &path,
                Some(&json!({"body":body})),
            )
            .await?;
        response_json(&response, &[200])?;
        return Ok(
            json!({"github_comment_id":github_comment_id,"github_issue_number":number,"action":"updated"}),
        );
    }
    if comment.deleted {
        return Err(SyncFailure::fatal("comment_not_exported"));
    }
    let path = format!(
        "/repos/{}/{}/issues/{}/comments",
        mapping.github_owner, mapping.github_repository, issue_binding.github_issue_number
    );
    let body = webhook::append_origin_marker(Some(&comment.body), &marker);
    let response = match api
        .rest(
            settings,
            mapping,
            "POST",
            &path,
            Some(&json!({"body":body})),
        )
        .await
    {
        Ok(response) => response,
        Err(error) if error.retryable => {
            return Err(SyncFailure::fatal("uncertain_comment_create"));
        }
        Err(error) => return Err(error.into()),
    };
    let remote = response_json(&response, &[201])?;
    let id = remote
        .get("id")
        .and_then(|value| {
            value
                .as_i64()
                .map(|n| n.to_string())
                .or_else(|| value.as_str().map(ToOwned::to_owned))
        })
        .ok_or_else(|| SyncFailure::fatal("invalid_github_response"))?;
    storage::upsert_comment_binding(
        postgres,
        &mapping.mapping_id,
        issue_binding.github_issue_number,
        &comment.comment_id,
        &id,
    )
    .await?;
    Ok(
        json!({"github_comment_id":id,"github_issue_number":issue_binding.github_issue_number,"action":"created"}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mapping() -> Mapping {
        Mapping {
            mapping_id: "map".into(),
            installation_id: "1".into(),
            github_repository_id: "2".into(),
            github_owner: "o".into(),
            github_repository: "r".into(),
            organization_id: "org".into(),
            team_id: "team".into(),
            project_id: "project".into(),
            github_project_id: None,
            github_project_status_field_id: None,
            conflict_policy: "github_wins".into(),
            state_mappings: json!([
                {"workflow_state_id":"todo","github_issue_state":"open","github_project_option_id":null,"inbound_default":true},
                {"workflow_state_id":"doing","github_issue_state":"open","github_project_option_id":null,"inbound_default":false},
                {"workflow_state_id":"done","github_issue_state":"closed","github_project_option_id":null,"inbound_default":true}
            ]),
            label_mappings: json!([{"lenso_label_id":"bug-id","github_label":"bug"}]),
            milestone_mappings: json!([{"lenso_milestone_id":"m1","github_milestone_number":7}]),
        }
    }

    #[test]
    fn inbound_state_requires_one_explicit_default() {
        let mapping = mapping();
        assert_eq!(map_inbound_state(&mapping, "open").unwrap(), "todo");
        assert!(map_inbound_state(&mapping, "unknown").is_err());
    }

    #[test]
    fn labels_preserve_non_github_owned_values() {
        let mapping = mapping();
        let labels = mapped_labels(
            &mapping,
            &json!([{"name":"bug"}]),
            &["private".into(), "bug-id".into()],
        )
        .unwrap();
        assert_eq!(labels, vec!["bug-id", "private"]);
    }
}
