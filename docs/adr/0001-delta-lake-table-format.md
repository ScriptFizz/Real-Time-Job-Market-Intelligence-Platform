# ADR 0001: Delta Lake for managed pipeline tables

- Status: Accepted
- Date: 2026-08-24

## Context

The first implementation stored processed datasets as Parquet and implemented
keyed updates by reading the current files, unioning incoming rows, deduplicating,
and overwriting the original path. That made a single retry idempotent but did not
make the update atomic. An interrupted overwrite could expose incomplete data,
and concurrent writers could silently lose each other's changes.

The platform needs atomic partition replacement, keyed upserts, optimistic
concurrency control, schema evolution, and readable previous versions. The main
candidates were plain Parquet, Delta Lake, and Apache Iceberg.

## Decision

Use Delta Lake for Silver, Gold, Feature, ML, processing-ledger, and control
tables. Keep Bronze as immutable JSON Lines because it is the source-preserving
landing layer.

Delta was selected because it integrates directly with the existing PySpark
runtime and supplies transactional `MERGE`, atomic partition replacement,
optimistic concurrency, and versioned table history without introducing a
separate catalog service for the local portfolio stack.

## Alternatives

### Plain Parquet

Parquet remains an excellent columnar file format, but it does not define table
transactions, concurrent commit semantics, or keyed updates. Implementing those
correctly in application code would duplicate a table-format transaction log.

### Apache Iceberg

Iceberg offers strong multi-engine interoperability, hidden partitioning, and
catalog-centered table management. It would be a strong choice for a platform
shared by Spark, Trino, Flink, and warehouse engines. This project currently has
one compute engine and no production catalog, so Iceberg would add operational
surface without demonstrating a needed capability.

## Consequences

- Each table write is transactional and conflict-detected.
- Keyed retries converge through deterministic merge keys and ordering.
- A multi-output stage is still not one distributed transaction; downstream
  scheduling must use the committed processing ledger.
- Spark must load Delta Lake JVM artifacts. The first execution may need Maven
  access unless those artifacts are pre-cached by the runtime image or cluster.
- GCS execution also requires the appropriate Hadoop GCS connector and cloud
  identity configuration in the Spark environment.
- Migrating to Iceberg later would require rewriting table I/O and control-table
  implementations, but transformation logic and stage contracts can remain.
