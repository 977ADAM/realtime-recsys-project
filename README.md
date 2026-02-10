# Real-time Recommender MVP

## PostgreSQL configuration
Set database connection string via `DATABASE_URL`.

Example:

```bash
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/recsys"
```

Optional pool settings:

```bash
export DB_POOL_MIN_SIZE=1
export DB_POOL_MAX_SIZE=10
```

Database schema is initialized on app startup from `sql/schema.sql`.
