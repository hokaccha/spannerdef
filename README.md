# spannerdef

Idempotent Google Cloud Spanner schema management by SQL, inspired by [sqldef](https://github.com/sqldef/sqldef).

## Features

- **Idempotent**: Safe to run multiple times
- **Declarative**: Describe what you want, not how to get there
- **SQL-based**: No custom DSL, just standard Spanner DDL
- **Diff preview**: See what changes will be made before applying them

### Supported Operations

- **Tables**: CREATE TABLE, DROP TABLE
- **Columns**: ADD COLUMN, DROP COLUMN
- **Indexes**: CREATE INDEX, DROP INDEX

## Installation

```bash
go install github.com/ubie-sandbox/spannerdef/cmd/spannerdef@latest
```

## Usage

```bash
spannerdef --project=PROJECT_ID --instance=INSTANCE_ID --database=DATABASE_ID < schema.sql
```

### Options

```
Usage:
  spannerdef [OPTIONS] < desired.sql

Application Options:
  -p, --project=project_id     Google Cloud Project ID (required)
  -i, --instance=instance_id   Spanner Instance ID (required)
  -d, --database=database_id   Spanner Database ID (required)
      --file=sql_file          Read desired SQL from the file, rather than stdin
      --dry-run                Don't run DDLs but just show them
      --export                 Just dump the current schema to stdout
      --enable-drop            Enable destructive changes such as DROP TABLE, DROP INDEX
      --config=                YAML file to specify: target_tables, skip_tables
      --help                   Show this help
      --version                Show this version
```

## Examples

### Export current schema

```bash
spannerdef --project=my-project --instance=my-instance --database=my-db --export
```

### Preview changes (dry run)

```bash
spannerdef --project=my-project --instance=my-instance --database=my-db --dry-run < schema.sql
```

### Apply changes

```bash
spannerdef --project=my-project --instance=my-instance --database=my-db < schema.sql
```

### Example schema file

```sql
-- schema.sql
CREATE TABLE Users (
    Id INT64 NOT NULL,
    Name STRING(100),
    Email STRING(255),
    CreatedAt TIMESTAMP
) PRIMARY KEY (Id);

CREATE INDEX IdxEmail ON Users (Email);

CREATE TABLE Posts (
    Id INT64 NOT NULL,
    UserId INT64 NOT NULL,
    Title STRING(255),
    Content STRING(MAX),
    CreatedAt TIMESTAMP
) PRIMARY KEY (Id);

CREATE INDEX IdxUserId ON Posts (UserId);
```

## Authentication

spannerdef uses Google Cloud authentication. Make sure you have:

1. `GOOGLE_APPLICATION_CREDENTIALS` environment variable set, or
2. `gcloud auth application-default login` configured, or
3. Running on Google Cloud with appropriate service account

## Development

### Build

```bash
go build ./cmd/spannerdef
```

### Testing

spannerdef includes comprehensive tests that use Spanner emulator.

**Requirements**: Docker/Docker Compose for Spanner emulator.

```bash
# Start Spanner emulator
docker-compose up

# Run all tests
make test

# Run specific tests
go test -v . -run TestBasicOperations
```

## Limitations

Because spannerdef distinguishes tables/indexes by name, it does NOT support:

- RENAME TABLE
- RENAME INDEX
- Complex schema changes that require data migration

To handle these cases, you would need to apply changes manually and use `--export` to capture the new schema.

## Architecture

spannerdef is built with the following components:

- **memefish**: Spanner SQL parser for parsing DDL statements
- **Google Cloud Spanner Go SDK**: For connecting to and managing Spanner databases
- **schema package**: Core logic for schema comparison and DDL generation
- **database/spanner package**: Spanner-specific database operations

## License

MIT
