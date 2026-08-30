# Plugin card: GitHub Project Sync

Outcome: keep one Lenso Project and one GitHub repository / optional Projects v2 board synchronized without coupling either state owner.

Owns: GitHub installation metadata, repository mappings, field maps, stable issue/comment bindings, raw verified webhook inbox, Projects activity checkpoints, outbound jobs, leases/fences, origin receipts, attempts, and dead letters.

Requires: Secrets, HTTP Client, Projects, and Projects Collaboration. The Host must route exact webhook bytes and invoke the worker under an actor authorized by the mapped Lenso organization/project.

Replaceability proof: uninstalling this Plugin removes sync behavior and its schema; Projects entities and GitHub Issues remain independently usable. Replacing the implementation is safe at its durable Capability and cursor boundaries.
