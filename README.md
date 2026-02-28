# icepeek

A terminal-based Apache Iceberg table viewer.

Browse table data, inspect schema, snapshots, manifests, partitions, and data file statistics from your terminal.

## Features

- **Data view**: Browse table rows with scrolling and column resizing
- **Schema browser**: Explore field trees, types, and schema history
- **Snapshots & time travel**: Browse snapshot history, press Enter to view historical data
- **Manifests & data files**: Inspect manifest entries and per-file statistics
- **Properties**: Format version, table UUID, partition specs, sort orders, and table properties
- **Column selector**: Toggle visible columns on the fly
- **Live filtering**: Filter rows with SQL-like expressions

## What it is NOT for

`icepeek` is **not** a query engine. It does not replace Spark, Trino, DuckDB, or any SQL-over-Iceberg tool. It is a
lightweight TUI for quickly inspecting Iceberg tables, such as checking schema, previewing rows, browsing snapshot
history, and examining metadata without spinning up a full compute engine.

## Installation

```sh
cargo install icepeek
```

## Usage

### Local table

```sh
icepeek open /path/to/iceberg/table
```

### Cloud

```sh
icepeek open s3://bucket/path/to/table --s3-endpoint http://localhost:9000 --s3-region us-east-1
```

Note: Currently supporting S3-compatible storage only

### REST catalog

```sh
icepeek catalog --uri http://localhost:8181 --table namespace.table_name
```

Note: Currently supporting RESTful Iceberg catalogs only

### Row limit

By default, icepeek loads up to 500 rows at one time to avoid loading in excessive data for huge tables. Override with
`--limit` or load everything with `--no-limit`:

```sh
icepeek open /path/to/table --limit 1000
icepeek open /path/to/table --no-limit
```

## Filter syntax

The filter bar (press `/`) supports:

| Expression     | Example                             |
|----------------|-------------------------------------|
| Comparison     | `age > 30`, `price <= 100`          |
| Equality       | `status = 'active'`, `id != 5`      |
| Null checks    | `email IS NULL`, `name IS NOT NULL` |
| Set membership | `city IN ('NYC', 'LA', 'SF')`       |
| Combinators    | `age > 18 AND status = 'active'`    |
|                | `role = 'admin' OR role = 'owner'`  |

Unquoted values are parsed as numbers; quoted values as strings.

## Time travel

Navigate to the **Snapshots** tab and press `Enter` on any snapshot to load its data. The status bar shows
`Snapshot: <id>` when viewing a historical snapshot.

While time-traveling:

- Filters apply to the selected snapshot's data
- Reload (`r`) stays on the selected snapshot
- The Files tab shows manifests for the selected snapshot

## Examples

The [examples](./examples) directory contains a script that generates sample Iceberg tables and launch `icepeek`
against them.

### Local table

No external dependencies. Generate a local Iceberg table and open it directly:

```sh
cd examples
make local
```

### S3 table (no catalog)

Start a MinIO container, write sample data to `s3://warehouse/sample_table`, and open it via S3 path:

```sh
cd examples
make s3
```

### S3 table with REST catalog

Starts MinIO + a REST catalog (`tabulario/iceberg-rest`), register sample data through the catalog, and open it:

```sh
cd examples
make s3-with-catalog
```

### Cleanup

Stop containers and remove generated data:

```sh
cd examples
make clean
```

> **Prerequisites:** Docker is required for the S3 and catalog examples.

## Requirements

- Rust 1.88+ (for `cargo install`)

## License

MIT
