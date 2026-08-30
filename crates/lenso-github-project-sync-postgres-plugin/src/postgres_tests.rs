use super::*;

use lenso_capability_github_project_sync_admin as admin;
use sqlx::AssertSqlSafe;

async fn prepare() -> Option<(String, String, OwnedPostgres)> {
    let database_url = std::env::var("LENSO_GITHUB_SYNC_TEST_DATABASE_URL").ok()?;
    let database_name = database_url
        .split('?')
        .next()
        .and_then(|value| value.rsplit('/').next())
        .unwrap_or_default();
    assert!(
        database_name.starts_with("lenso_github_sync_test"),
        "acceptance requires a dedicated lenso_github_sync_test* database"
    );
    let schema_name = format!("github_sync_test_{}", uuid::Uuid::new_v4().simple());
    GithubProjectSyncOperator::setup(&database_url, &schema_name)
        .await
        .unwrap();
    let postgres = OwnedPostgres::prepare(
        &database_url,
        schema::schema_plan(schema_name.clone()).unwrap(),
    )
    .await
    .unwrap();
    storage::initialize_settings(
        &postgres,
        "123",
        "secret://github/private-key",
        "secret://github/webhook",
    )
    .await
    .unwrap();
    Some((database_url, schema_name, postgres))
}

async fn cleanup(database_url: &str, schema_name: &str, postgres: OwnedPostgres) {
    postgres.pool().close().await;
    let pool = sqlx::PgPool::connect(database_url).await.unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "DROP SCHEMA \"{schema_name}\" CASCADE"
    )))
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;
}

fn mapping_request() -> admin::PutMappingRequest {
    serde_json::from_value(serde_json::json!({
    "mapping_id":"mapping_acme","installation_id":"1001","github_repository_id":"2002","github_owner":"acme","github_repository":"widgets","organization_id":"org_acme","team_id":"team_eng","project_id":"project_sync",
    "github_project_id":"PVT_project","github_project_status_field_id":"PVTF_status","inbound_enabled":true,"outbound_enabled":true,"conflict_policy":"github_wins",
    "state_mappings":[{"workflow_state_id":"state_todo","github_issue_state":"open","github_project_option_id":"opt_todo","inbound_default":true},{"workflow_state_id":"state_doing","github_issue_state":"open","github_project_option_id":"opt_doing","inbound_default":false},{"workflow_state_id":"state_done","github_issue_state":"closed","github_project_option_id":"opt_done","inbound_default":true}],
    "label_mappings":[{"lenso_label_id":"label_bug","github_label":"bug"}],"milestone_mappings":[{"lenso_milestone_id":"milestone_1","github_milestone_number":1}],"expected_revision":null
})).unwrap()
}

#[tokio::test]
async fn restart_duplicate_out_of_order_concurrency_fencing_and_cursor_no_skip() {
    let Some((database_url, schema_name, postgres)) = prepare().await else {
        eprintln!("skipping PostgreSQL acceptance; LENSO_GITHUB_SYNC_TEST_DATABASE_URL is unset");
        return;
    };
    storage::put_installation(
        &postgres,
        &admin::PutInstallationRequest {
            installation_id: "1001".into(),
            account_login: "acme".into(),
            account_id: "9001".into(),
            expected_revision: None,
        },
    )
    .await
    .unwrap();
    storage::put_mapping(&postgres, &mapping_request())
        .await
        .unwrap();
    assert!(matches!(
        storage::active_mapping_for_repository(&postgres, "1001", "unknown", true).await,
        Err(storage::StorageError::Domain(
            storage::DomainFailure::MappingInactive
        ))
    ));

    let body1 = br#"{"installation":{"id":1001},"repository":{"id":2002},"issue":{"number":2}}"#;
    let body2 = br#"{"installation":{"id":1001},"repository":{"id":2002},"issue":{"number":1}}"#;
    let first = storage::insert_delivery(
        &postgres,
        "delivery-later",
        "issues",
        body1,
        &webhook::payload_sha256(body1),
    )
    .await
    .unwrap();
    assert!(!first.duplicate);
    let duplicate = storage::insert_delivery(
        &postgres,
        "delivery-later",
        "issues",
        body1,
        &webhook::payload_sha256(body1),
    )
    .await
    .unwrap();
    assert!(duplicate.duplicate);
    assert!(matches!(
        storage::insert_delivery(
            &postgres,
            "delivery-later",
            "issues",
            body2,
            &webhook::payload_sha256(body2)
        )
        .await,
        Err(storage::StorageError::Domain(
            storage::DomainFailure::DeliveryConflict
        ))
    ));
    storage::insert_delivery(
        &postgres,
        "delivery-earlier",
        "issues",
        body2,
        &webhook::payload_sha256(body2),
    )
    .await
    .unwrap();

    let (left, right) = tokio::join!(
        storage::claim_deliveries(&postgres, "worker-a", 1, 30),
        storage::claim_deliveries(&postgres, "worker-b", 1, 30)
    );
    let left = left.unwrap();
    let right = right.unwrap();
    assert_eq!(left.len() + right.len(), 2);
    assert_ne!(left[0].delivery_id, right[0].delivery_id);
    sqlx::query("UPDATE webhook_deliveries SET lease_until=transaction_timestamp()-interval '1 second' WHERE delivery_id=$1").bind(&left[0].delivery_id).execute(postgres.pool()).await.unwrap();
    let reclaimed = storage::claim_deliveries(&postgres, "worker-c", 1, 30)
        .await
        .unwrap();
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].delivery_id, left[0].delivery_id);
    assert!(reclaimed[0].fence > left[0].fence);
    assert!(matches!(
        storage::succeed_delivery(&postgres, &left[0], "worker-a").await,
        Err(storage::StorageError::Domain(
            storage::DomainFailure::LeaseLost
        ))
    ));
    storage::succeed_delivery(&postgres, &reclaimed[0], "worker-c")
        .await
        .unwrap();
    storage::succeed_delivery(&postgres, &right[0], "worker-b")
        .await
        .unwrap();

    let page = vec![
        storage::ActivityStub {
            activity_id: "10".into(),
            entity_kind: "issue".into(),
            entity_id: "issue-a".into(),
            issue_id: Some("issue-a".into()),
            operation: "create_issue".into(),
            revision: Some(1),
        },
        storage::ActivityStub {
            activity_id: "11".into(),
            entity_kind: "issue".into(),
            entity_id: "issue-b".into(),
            issue_id: Some("issue-b".into()),
            operation: "update_issue".into(),
            revision: Some(2),
        },
    ];
    let (staged_a, staged_b) = tokio::join!(
        storage::stage_activity_page(&postgres, "mapping_acme", None, Some("11"), &page),
        storage::stage_activity_page(&postgres, "mapping_acme", None, Some("11"), &page)
    );
    assert_eq!(staged_a.unwrap() + staged_b.unwrap(), 2);
    assert_eq!(
        storage::checkpoint_cursor(&postgres, "mapping_acme")
            .await
            .unwrap()
            .as_deref(),
        Some("11")
    );
    let next = vec![storage::ActivityStub {
        activity_id: "12".into(),
        entity_kind: "comment".into(),
        entity_id: "comment-a".into(),
        issue_id: Some("issue-a".into()),
        operation: "add_comment".into(),
        revision: Some(1),
    }];
    assert_eq!(
        storage::stage_activity_page(&postgres, "mapping_acme", Some("11"), Some("12"), &next)
            .await
            .unwrap(),
        1
    );
    let checkpoint = storage::get_checkpoint(
        &postgres,
        &admin::GetCheckpointRequest {
            mapping_id: "mapping_acme".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(checkpoint.activity_cursor.as_deref(), Some("12"));
    assert_eq!(checkpoint.staged_count, 3);
    assert_eq!(checkpoint.oldest_pending_activity_id.as_deref(), Some("10"));

    postgres.pool().close().await;
    let restarted = OwnedPostgres::prepare(
        &database_url,
        schema::schema_plan(schema_name.clone()).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        storage::checkpoint_cursor(&restarted, "mapping_acme")
            .await
            .unwrap()
            .as_deref(),
        Some("12")
    );
    let detail = storage::inspect_delivery(
        &restarted,
        &admin::InspectDeliveryRequest {
            delivery_id: "delivery-later".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(detail.state, admin::InspectDeliveryResponseState::Succeeded);
    cleanup(&database_url, &schema_name, restarted).await;
}
