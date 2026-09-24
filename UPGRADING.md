# Upgrading from v1.5.0 or earlier

v1.6.0 and v1.7.0 changed which schema details spannerdef preserves and which migration plans it accepts. An unchanged desired schema file can therefore produce new DDL or an error when first used with a newer version. Upgrading the binary does not repair differences left in the database by an older version.

This guide covers upgrades through v1.7.0. See the [v1.6.0](https://github.com/hokaccha/spannerdef/releases/tag/v1.6.0) and [v1.7.0](https://github.com/hokaccha/spannerdef/releases/tag/v1.7.0) release notes for the complete changes.

## Differences that older versions could leave behind

Versions through v1.5.0 could omit or misrepresent these details while parsing schemas or creating objects:

| Schema detail | What to compare in the live DDL and desired schema |
| --- | --- |
| Index keys | `ASC`/`DESC` direction and `NULL_FILTERED` |
| Interleaving | `INTERLEAVE IN` versus `INTERLEAVE IN PARENT` |
| Foreign keys | `NOT ENFORCED` versus enforced constraints |
| Columns | Generated expressions, `HIDDEN`, identity/`AUTO_INCREMENT`, and `ON UPDATE` commit timestamp expressions |
| Object names | Schema-qualified names and distinct objects in different named schemas |

v1.6.0 and later retain these distinctions. If an older version created an object without an intended attribute, the new version can expose that drift. Some differences require an explicit migration: existing primary keys, ordinary index definitions, interleave relationships, and generated/identity/ON UPDATE definitions cannot simply be changed by the generator.

For example, an index intended as:

```sql
CREATE INDEX EventsByCreatedAt ON Events (CreatedAt DESC);
```

may have been created without `DESC`. The new version reports:

```text
unsupported definition change for index EventsByCreatedAt; migrate the index explicitly
```

`--enable-drop` does not bypass this validation. Reconcile the live index with the intended definition through a separately reviewed migration, such as dropping and recreating the index. Preserve its other clauses and account for dependencies and any uniqueness enforcement during the replacement. Keep that migration separate from the desired schema file, which describes the final schema rather than a sequence of migration steps.

v1.6.0 also generates changes to column defaults and nullability, row deletion policies, and interleaved `ON DELETE` actions. Differences ignored by older releases may now produce `ALTER` statements. These changes can affect existing data even without `--enable-drop`; the flag is not a general data-protection switch.

## Changes in planning and execution

### v1.6.0

- **Unsupported DDL fails explicitly.** Statements and clauses that were silently ignored can now stop planning. `--export` still returns the database's raw DDL. Table filters exclude table-scoped definitions before supported-feature validation; they do not suppress errors for globally scoped unsupported objects.
- **Constraint removals honor drop filtering.** Without `--enable-drop`, standalone constraint removals are skipped along with table, index, and column removals. Replacing a named constraint can still drop and recreate it.
- **Mixed plans can be rejected.** If a removal is skipped, plans that also alter columns, TTL, delete actions, or add constraints are conservatively rejected because retained dependencies may prevent execution. Retain wanted objects in the desired schema, or explicitly review their removal.
- **Dry runs and execution use the same SQL-aware drop filtering.** For example, `DROP TABLE` inside a string literal no longer causes an otherwise executable statement to be skipped.

### v1.7.0

- **Case-only name changes fail explicitly.** Changes such as `Users` to `users` are rejected for tables, named schemas, ordinary indexes, columns, and named constraints. The guard also covers filtered definitions and the checked library API. Keep existing spelling, or perform a supported rename explicitly before updating the desired definition.
- **Six more object kinds are managed.** Sequences, views, search/vector indexes, change streams, and property graphs now participate in the diff. Existing objects missing from the desired schema can become drop candidates with `--enable-drop`.
- **Table filters do not limit every object.** Sequences, views, change streams, and property graphs are managed globally, even with `target_tables` or `skip_tables`. Search/vector indexes follow their base table's filter. Retain every globally managed object that should remain in the desired schema.
- **Dependent rebuilds and stateful objects need attention.** Index or dependent-object rebuilds require `--enable-drop`; mixed plans involving skipped object removals may be rejected. Dropping a sequence discards its state, and dropping a change stream deletes its history. See [object-specific support and limits](README.md#additional-googlesql-schema-objects).

## Upgrade procedure

Repeat this process for each environment; their live schemas may differ. Replace the connection identifiers below and use the same authentication and `--config` options as your normal workflow. Pin the new binary or container version while validating the upgrade.

1. **Export and compare the live schema.** Preserve both the existing desired schema and the export. An export captures DDL, not a data backup.

   ```sh
   spannerdef --project=PROJECT_ID --instance=INSTANCE_ID --database=DATABASE_ID \
     --export > live.sql
   diff -u live.sql schema.sql
   ```

   Investigate semantic differences such as missing `DESC`; ordering and formatting differences alone may not matter. Do not overwrite the desired schema with the export without reconciling the intended definitions.

2. **Preview with the new version, initially without `--enable-drop`.**

   ```sh
   spannerdef --project=PROJECT_ID --instance=INSTANCE_ID --database=DATABASE_ID \
     --dry-run < schema.sql
   ```

   If planning returns an error, a complete preview is not available yet. Resolve the reported difference and repeat the dry run. For local rehearsal, load the exported DDL into a separate Emulator/Omni database and point spannerdef at that database. This does not reproduce production data or every managed Spanner feature; see the [local runtime guidance](README.md#running-against-spanner-emulator--spanner-omni).

3. **Reconcile drift and unsupported changes explicitly.** For example, restore an index's intended direction through a separate migration. Primary-key, interleave, or generated-column differences may require a broader migration. Export again after repairs and repeat the preview until every proposed change is understood and intended.

4. **Retain newly managed objects before enabling removals.** Add the live sequences, views, change streams, graphs, and relevant search/vector indexes that should remain to the desired schema. Table filters alone do not protect globally managed objects. When using wrench, continue excluding its migration history table as described in the [wrench configuration guide](README.md#using-wrench-alongside-spannerdef).

5. **Review the exact plan you intend to apply.** If removals or rebuilds are intentional, preview again with `--dry-run --enable-drop` before applying with the same configuration and schema. If no removals are intended, keep `--enable-drop` off and resolve any skipped-removal conflicts by retaining the objects. A successful command can still skip removals; check the output and repeat the preview after applying.

## Library callers

`GenerateDDLs` is deprecated in v1.7.0. Use `GenerateDDLsChecked(current, desired)` or `GenerateIdempotentDDLs(desiredSQL, currentSQL, config)` and handle the returned error before applying any DDL. The legacy function cannot return planning errors.

`Schema` gained an exported `Objects` field in v1.7.0. Positional literals written against v1.5.0/v1.6.0 may no longer compile; use keyed literals or construct schemas with `ParseDDLs`. Filtering metadata is internal and is not part of the public struct.
