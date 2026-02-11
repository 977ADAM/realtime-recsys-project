Migration files are applied by `app.db.init_db()` in lexicographic order.

Conventions:
- Use zero-padded prefixes: `0001_*.sql`, `0002_*.sql`, ...
- Never edit an already applied migration.
- Add a new file for every schema change.

Applied migrations are tracked in the Postgres table `schema_migrations`
with a stored SHA-256 checksum for drift detection.

Validation helpers:
- local-only check: `python src/check_migrations.py --skip-db-check`
- local + live DB checksum/drift check: `python src/check_migrations.py --database-url ...`
