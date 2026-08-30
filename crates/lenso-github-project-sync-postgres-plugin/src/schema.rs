use lenso_postgres_kit::{Migration, PlanError, SchemaPlan, sql_migrations};

const MIGRATIONS: &[Migration] = sql_migrations![(
    1,
    "create-github-project-sync",
    "migrations/001_create_sync.sql"
),];

pub(crate) fn schema_plan(schema: impl Into<std::sync::Arc<str>>) -> Result<SchemaPlan, PlanError> {
    SchemaPlan::new(schema, MIGRATIONS)
}

#[cfg(test)]
mod tests {
    use super::MIGRATIONS;

    #[test]
    fn migration_encodes_durable_sync_invariants() {
        let sql = MIGRATIONS[0].sql();
        assert!(sql.contains("payload_sha256 TEXT NOT NULL"));
        assert!(sql.contains("delivery_id TEXT PRIMARY KEY"));
        assert!(sql.contains("fence BIGINT NOT NULL"));
        assert!(!sql.contains("FOR UPDATE SKIP LOCKED"));
        assert!(sql.contains("UNIQUE (mapping_id, activity_id)"));
        assert!(sql.contains("activity_cursor TEXT"));
        assert!(sql.contains("github_issue_bindings"));
        assert!(sql.contains("origin_markers"));
    }
}
