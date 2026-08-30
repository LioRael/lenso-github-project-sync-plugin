# Release process

1. Merge only after CI, boundary, and PostgreSQL acceptance pass.
2. Let release-plz open the version PR.
3. Review generated changelog/version changes.
4. Run the manual `Release-plz` workflow on `main`, set `live=true`, and type `publish`.
5. Configure crates.io Trusted Publishing with owner `LioRael`, repository
   `lenso-github-project-sync-plugin`, workflow `release-plz.yml`, and no
   GitHub environment restriction.
6. The release job exchanges GitHub OIDC for a short-lived crates.io
   credential; no long-lived crates.io token is stored.
