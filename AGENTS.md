# Repository instructions

- This repository owns GitHub-specific synchronization state. It must never read or migrate the Projects database.
- All Projects mutations use the portable capability and stable Projects UUIDs. Preserve non-GitHub-owned fields on full-replacement updates.
- Webhook HMAC validation happens over the exact raw request bytes before JSON parsing.
- Secrets and installation tokens must never be persisted or included in debug output.
- A declared operation must be implemented; do not add `not_supported` placeholders.
- Run Cargo through `/Users/leosouthey/Projects/framework/.lenso-tools/bin/lenso-cargo`.

