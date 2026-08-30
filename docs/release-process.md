# Release process

1. Merge only after CI, boundary, and PostgreSQL acceptance pass.
2. Let release-plz open the version PR.
3. Review generated changelog/version changes.
4. Run the manual `Release-plz` workflow on `main`, set `live=true`, and type `publish`.
5. The release environment exchanges GitHub OIDC for crates.io Trusted Publishing credentials; no long-lived crates.io token is stored.

