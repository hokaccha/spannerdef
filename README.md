# spannerdef

Idempotent Google Cloud Spanner schema management by SQL, inspired by [sqldef](https://github.com/sqldef/sqldef).

## Features

- **Idempotent**: Safe to run multiple times
- **Declarative**: Describe what you want, not how to get there
- **SQL-based**: No custom DSL, just standard Spanner DDL
- **Diff preview**: See what changes will be made before applying them

### Supported Operations

- **Schemas**: create named schemas and use qualified object names
- **Tables**: create/drop tables, preserve explicit empty keys and implicit rowid keys, change interleave ON DELETE actions
- **Columns**: add/drop columns, change supported types, defaults, nullability, and options; preserve generated, identity, hidden, and ON UPDATE definitions
- **Indexes**: create/drop regular, unique, null-filtered, and expression indexes; preserve key directions and STORING columns
- **Constraints**: add/drop/recreate CHECK and foreign key constraints, including enforcement and ON DELETE actions
- **Schema objects**: sequences, views, search/vector indexes, change streams, property graphs (see [support boundaries](#additional-googlesql-schema-objects))
- **TTL**: add/replace/drop row deletion policies, including zero-day policies

Desired schema files use CREATE declarations (and ALTER TABLE ADD CONSTRAINT as emitted by Spanner exports). They are schema descriptions, not a sequential migration script. Unsupported statements and definition changes return errors before execution.

Without `--enable-drop`, table, index, column, and standalone constraint removals are skipped. Replacing a constraint still drops and recreates that constraint. Preview and execution use the same filtering. If any removal is skipped, plans that also alter columns, TTL, delete actions, or add constraints are conservatively rejected before execution: retained indexes, constraints, or interleaved children may block those changes. Keep the existing objects in the desired schema or explicitly enable their removal. Other changes, including enabling CASCADE or shortening TTL, can affect data and should be reviewed with `--dry-run`.

## Installation

```bash
go install github.com/hokaccha/spannerdef/cmd/spannerdef@latest
```

Release binaries are built with the current Go release; the macOS binaries require macOS 13 or later.

Or install with [mise](https://mise.jdx.dev/):

```bash
mise use -g github:hokaccha/spannerdef@latest
```

Or run the container image (multi-arch `linux/amd64` + `linux/arm64`, built with [ko](https://ko.build) on every release):

```bash
docker run --rm ghcr.io/hokaccha/spannerdef:latest --help
```

The image is the static binary (at `/ko-app/spannerdef`, the entrypoint) on a distroless base running as a non-root user, so it works directly as a migration step in CI or as a Cloud Run job:

```bash
docker run --rm -v "$PWD/schema.sql:/schema.sql:ro" \
  --add-host=host.docker.internal:host-gateway \
  -e SPANNER_EMULATOR_HOST=host.docker.internal:9010 \
  ghcr.io/hokaccha/spannerdef:latest --project=P --instance=I --database=D --file=/schema.sql
```

(`--add-host` is needed on Linux Docker Engine; Docker Desktop resolves `host.docker.internal` by itself.)

Successful builds of `main` also publish `ghcr.io/hokaccha/spannerdef:main`, the unreleased latest build (`--version` reports `main-<commit>`; superseded builds may be cancelled). It is a moving tag with no compatibility promise; pin the digest printed in the workflow's job summary if you rely on a particular build. Old `main` builds stay pullable by digest for 30 days, after which a weekly cleanup removes them.

## Upgrading

For upgrades from v1.5.0 or earlier, read the [upgrade guide](UPGRADING.md) before applying a schema with v1.6.0 or later. Newer versions expose previously ignored schema differences, reject unsafe plans, and manage additional objects. Export each database and review a dry run before applying changes.

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
      --timeout=               Limit database operations (e.g. 30m; default 0 has no deadline)
      --resume-operation=      Wait for a full DDL operation name without submitting a new plan
      --enable-drop            Enable destructive changes such as DROP TABLE, DROP INDEX
      --config=                YAML file to specify: target_tables, skip_tables
      --impersonate-service-account=email
                               Run as this service account using short-lived credentials from the IAM
                               Credentials API (the caller needs roles/iam.serviceAccountTokenCreator on it)
      --help                   Show this help
      --version                Show this version
```

### Input and configuration

Multiple `--file` inputs (or comma-separated paths) are joined with a newline so a trailing SQL line comment cannot hide the next file. Keep complete, semicolon-separated DDL declarations in each file; do not split a token or string literal across files. Single-file input is unchanged.

Configuration accepts one YAML document with `target_tables` and `skip_tables`. Unknown keys, duplicate keys, and extra documents are errors, so a misspelled filter cannot silently expose tables to removal.

### Long-running schema updates

Use `--timeout=30m` to limit database operations; the default `0` adds no overall deadline. Ctrl-C and SIGTERM cancel the client's RPCs and wait. **They do not cancel or roll back a DDL operation already accepted by Spanner.**

Each submitted batch prints its full operation name to stderr before submission. SDK retries keep that operation ID. If the response is lost, use that name to inspect the operation or resume waiting:

```sh
spannerdef --project=my-project --instance=my-instance --database=my-db \
  --resume-operation=projects/my-project/instances/my-instance/databases/my-db/operations/spannerdef_ID \
  --timeout=30m
```

`--resume-operation` only waits; it does not submit SQL or continue later batches. If submission never reached Spanner, the operation may not exist. After the operation finishes, export and dry-run the desired schema again before applying any remaining changes.

Execution preserves plan order, splits batches conservatively around Spanner's limit of ten statements requiring backfill or validation, and waits for each batch before sending the next. A new table and its immediately following ordinary indexes stay together when the table declaration has no table constraints. Errors stop subsequent batches; earlier changes remain applied. See [Spanner schema update guidance](https://docs.cloud.google.com/spanner/docs/schema-updates-best-practices).

Exports preserve the order returned by Spanner, including dependencies, instead of sorting statements alphabetically.

### Authentication

spannerdef uses [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials). To apply DDL as a different identity, for example when only a dedicated migration service account holds `roles/spanner.databaseAdmin`, pass `--impersonate-service-account`: every API call then uses short-lived credentials for that account, minted through the IAM Credentials API. The caller only needs `roles/iam.serviceAccountTokenCreator` on the target account.

```bash
spannerdef --project=my-project --instance=my-instance --database=my-db \
  --impersonate-service-account=migrate@my-project.iam.gserviceaccount.com < schema.sql
```

When `SPANNER_EMULATOR_HOST` is set the emulator ignores credentials, so impersonation is skipped.

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

## Additional GoogleSQL schema objects

Desired schema files can declare `CREATE SEQUENCE`, `CREATE VIEW`, `CREATE SEARCH INDEX`, `CREATE VECTOR INDEX`, `CREATE CHANGE STREAM`, and `CREATE PROPERTY GRAPH`. Definitions retain their parsed clauses and are compared after normalization. Creation and removal follow dependencies, including sequence defaults, views over views, graph sources and search index base tables.

- Sequences use `OPTIONS` syntax. Changed options use `ALTER SEQUENCE`; omitting an initial `start_with_counter` does not reset a running sequence. Removing skip-range options resets them with `NULL`.
- View and graph changes use `CREATE OR REPLACE`. Search/vector index changes rebuild the index. Structural column changes also rebuild dependent views, graphs and search/vector indexes, including indirect view dependencies. Rebuilds require `--enable-drop`.
- Change streams use `ALTER ... SET FOR`, `DROP FOR ALL`, and `SET OPTIONS`, retaining existing history. If changing an explicit tracking list both requires new columns and releases a table or explicitly tracked column being dropped, split the migration into separate steps. The planner does not silently suspend capture. `FOR ALL` continues to follow schema changes automatically.
- All six object kinds obey `--enable-drop`. Mixed plans that require skipped removals fail before execution. Dropping a change stream deletes its history; dropping a sequence discards its state.
- `target_tables` and `skip_tables` also filter search/vector indexes by their base table. Sequences, views, change streams and graphs are managed globally and must remain in the desired schema if they should be retained. Graph shorthand keys and properties require the source table in the managed schema; use explicit keys and properties for views or excluded tables.

Support is limited to GoogleSQL syntax understood by the pinned memefish parser. In particular, sequence declarations require an `OPTIONS` clause, and qualified names for vector indexes, change streams and graph elements, and newer graph semantic options are not yet accepted by that parser. Other object kinds (queues, models, functions, proto bundles, locality groups and roles/grants) remain unsupported and produce errors. Cross-kind renames/replacements require separate migrations. Use `GenerateDDLsChecked` or `GenerateIdempotentDDLs` when embedding the generator so dependency errors are returned to the caller.

The integration suite checks object creation, changes, deletion and export round trips against Omni and Emulator. Emulator 1.5.58 exports graph property names without their derived expressions and omits `NO PROPERTIES` and dynamic label/property clauses; the complex graph round-trip case runs on Omni, which preserves those definitions. This emulator limitation is not treated as expression equivalence by the generator.

## Using wrench alongside spannerdef

Exclude the migration history table from the declarative schema when using [wrench](https://github.com/cloudspannerecosystem/wrench), especially with `--enable-drop`:

```yaml
# spannerdef.yml
skip_tables: |
  SchemaMigrations
```

```sh
spannerdef --project=my-project --instance=my-instance --database=my-db \
  --config=spannerdef.yml --dry-run < schema.sql
```

If wrench uses a custom migration table, put that exact name in `skip_tables` instead. wrench's `--migration_table_name` support is on its main branch after v1.13.5; check your installed version. Use the same custom name for wrench migration and truncate commands. spannerdef does not automatically reserve table names.

## Running against Spanner Emulator / Spanner Omni

spannerdef works against both the [Spanner Emulator](https://cloud.google.com/spanner/docs/emulator) and [Spanner Omni](https://cloud.google.com/spanner-omni/docs) without any code changes — just point the Google Cloud Go SDK at a local endpoint via `SPANNER_EMULATOR_HOST`.

### Spanner Emulator

```bash
docker run -d --name spanner-emulator \
  -p 9010:9010 -p 9020:9020 \
  gcr.io/cloud-spanner-emulator/emulator:1.5.58

export SPANNER_EMULATOR_HOST=localhost:9010
spannerdef --project=my-project --instance=my-instance --database=my-db < schema.sql
```

Any project/instance/database IDs are accepted; you typically create them via `gcloud spanner instances create ...` first.

### Spanner Omni (pre-GA)

Spanner Omni runs the actual Spanner binary locally. In single-server mode it exposes a gRPC endpoint on port `15000` with `project=default` and `instance=default` hardcoded.

```bash
docker run -d -p 15000:15000 \
  --name spanneromni \
  -v spanneromni-2026-r2-1-beta:/spanner \
  us-docker.pkg.dev/spanner-omni/images/spanner-omni:2026.r2.1-beta \
  start-single-server

# Create a database first
docker exec spanneromni /google/spanner/bin/spanner databases create my-db

export SPANNER_EMULATOR_HOST=localhost:15000
spannerdef --project=default --instance=default --database=my-db < schema.sql
```

Omni r2.1 does not support in-place upgrades of older deployments. These examples use a new version-specific volume; keep older volumes until any needed data has been exported and migrated.

Note: Spanner Omni is pre-GA and licensed for development, testing, prototyping, and demonstration only.

## Development

### Build

```bash
go build ./cmd/spannerdef
```

### Testing

spannerdef's integration tests run against [Spanner Omni](https://cloud.google.com/spanner-omni/docs) — the offline single-server distribution of Spanner. The full suite runs on Omni 2026.r2.1-beta. A separate CI job runs schema regressions on Emulator 1.5.58 to catch differences in recently introduced DDL features. Neither local runtime replaces validation against your managed Spanner configuration.

**Requirements**: Docker.

```bash
# Start Spanner Omni
make omni-up

# Run all tests
make test

# Run specific tests
go test -v . -run TestBasicOperations

# Stop Spanner Omni and clear its volume
make omni-down
```

## Limitations

Because spannerdef distinguishes tables/indexes by name, it does NOT support:

- RENAME TABLE
- RENAME INDEX
- Complex schema changes that require data migration
- Existing primary key/ordinary index definition changes or generated/identity/ON UPDATE definition changes
- Changing only the case of an existing schema, table, column, index, or constraint name (Spanner rejects names that differ only in case, so these return explicit errors)
- Database/table options and unmodeled schema objects such as queues, models, functions, proto bundles, locality groups, and roles/grants (these return explicit errors). See [supported schema objects and their limits](#additional-googlesql-schema-objects).

To handle these cases, you would need to apply changes manually and use `--export` to capture the new schema.

## Library usage

Use `RunContext(ctx, db, options)` and `ParseGeneratorConfigChecked(path)` to handle errors without terminating your process. `NewDatabaseContext`, `NewAdminDatabaseContext`, `RunDDLsContext`, and the concrete database's context methods accept caller cancellation. `WithDDLOperationObserver` reports operation names; `DDLOperationError` preserves both the name and the underlying error for `errors.Is`/`errors.As` and gRPC status inspection. `WaitDDLOperation` resumes a wait without submitting SQL.

The existing `Database` interface, public configuration struct layouts, and legacy function signatures remain compatible. Custom `Database` implementations still work; implement the optional `ContextDatabase` methods to support cancellation of in-flight calls. Legacy `Run` and `ParseGeneratorConfig` retain their exit-on-error behavior and are deprecated. DDL administration uses only the admin client, without creating data sessions.

## Architecture

spannerdef is built with the following components:

- **memefish**: Spanner SQL parser for parsing DDL statements
- **Google Cloud Spanner Go SDK**: For connecting to and managing Spanner databases
- **parser.go / ddl_order.go**: schema models, comparison, and dependency ordering
- **ddl_plan.go / database.go**: shared preview/execution plan and drop filtering
- **spanner.go**: Spanner database administration

## License

MIT
