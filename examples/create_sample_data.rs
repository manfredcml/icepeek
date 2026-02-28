//! Creates a sample Iceberg table for testing the TUI.
//!
//! Supports three targets:
//!   - `local`   — writes to `examples/sample_table/` on disk
//!   - `s3`      — writes to S3 (Minio)
//!   - `catalog` — writes via REST catalog
//!
//! Local and S3 targets create 3 snapshots with **schema evolution**:
//!   - Snapshot 1: 50 employees  (schema 0 — 8 columns)
//!   - Snapshot 2: 150 employees (schema 1 — adds optional `title`)
//!   - Snapshot 3: 200 employees (schema 1) — current HEAD
//!
//! Catalog target creates 3 snapshots (schema 0, no evolution):
//!   - Snapshot 1:  50 employees
//!   - Snapshot 2: 150 employees
//!   - Snapshot 3: 200 employees — current HEAD
//!
//! Usage:
//!   cargo run --example create_sample_data -- local
//!   cargo run --example create_sample_data -- s3
//!   cargo run --example create_sample_data -- catalog

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::{
    Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use clap::{Parser, ValueEnum};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::FileIOBuilder;
use iceberg::spec::{
    DataFileFormat, FormatVersion, NestedField, Operation, PrimitiveType, Schema, Snapshot,
    SortOrder, Summary, Type, UnboundPartitionSpec,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalogBuilder, REST_CATALOG_PROP_URI};
use parquet::file::properties::WriterProperties;

// ─── Constants ──────────────────────────────────────────────

const DEPARTMENTS: [&str; 5] = ["Engineering", "Marketing", "Sales", "HR", "Finance"];
const TITLES: [&str; 6] = [
    "Engineer",
    "Senior Engineer",
    "Manager",
    "Director",
    "VP",
    "Analyst",
];
const FIRST_NAMES: [&str; 20] = [
    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack", "Karen",
    "Leo", "Mia", "Noah", "Olivia", "Paul", "Quinn", "Rose", "Sam", "Tina",
];
const LAST_NAMES: [&str; 10] = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
];

// 2025-01-01T00:00:00Z in microseconds
const BASE_TS: i64 = 1735689600000000;

// ─── CLI ────────────────────────────────────────────────────

#[derive(Parser)]
#[command(about = "Create sample Iceberg table for testing the TUI")]
struct Args {
    target: Target,
}

#[derive(Clone, ValueEnum)]
enum Target {
    Local,
    S3,
    Catalog,
}

// ─── Schema ─────────────────────────────────────────────────

fn base_fields() -> Vec<Arc<NestedField>> {
    vec![
        Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        )),
        Arc::new(NestedField::required(
            2,
            "name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::required(
            3,
            "email",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            4,
            "age",
            Type::Primitive(PrimitiveType::Long),
        )),
        Arc::new(NestedField::required(
            5,
            "salary",
            Type::Primitive(PrimitiveType::Double),
        )),
        Arc::new(NestedField::required(
            6,
            "department",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::required(
            7,
            "is_active",
            Type::Primitive(PrimitiveType::Boolean),
        )),
        Arc::new(NestedField::required(
            8,
            "created_at",
            Type::Primitive(PrimitiveType::Timestamptz),
        )),
    ]
}

fn build_schema_v0() -> Result<Schema> {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(base_fields())
        .build()
        .context("failed to build schema v0")
}

fn build_schema_v1() -> Result<Schema> {
    let mut fields = base_fields();
    fields.push(Arc::new(NestedField::optional(
        9,
        "title",
        Type::Primitive(PrimitiveType::String),
    )));
    Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .context("failed to build schema v1")
}

// ─── Data generation ────────────────────────────────────────

fn generate_batch(
    arrow_schema: Arc<arrow_schema::Schema>,
    start: usize,
    count: usize,
    include_title: bool,
) -> Result<RecordBatch> {
    let mut ids = Vec::with_capacity(count);
    let mut names = Vec::with_capacity(count);
    let mut emails = Vec::with_capacity(count);
    let mut ages: Vec<Option<i64>> = Vec::with_capacity(count);
    let mut salaries = Vec::with_capacity(count);
    let mut depts = Vec::with_capacity(count);
    let mut active = Vec::with_capacity(count);
    let mut created = Vec::with_capacity(count);

    for i in start..start + count {
        ids.push((i + 1) as i64);
        let first = FIRST_NAMES[i % FIRST_NAMES.len()];
        let last = LAST_NAMES[i % LAST_NAMES.len()];
        names.push(format!("{} {}", first, last));
        emails.push(format!(
            "{}.{}@example.com",
            first.to_lowercase(),
            last.to_lowercase()
        ));
        ages.push(if i % 7 == 0 {
            None
        } else {
            Some(25 + (i % 40) as i64)
        });
        salaries.push(50000.0 + (i as f64 * 500.0) + ((i * 7) % 1000) as f64);
        depts.push(DEPARTMENTS[i % DEPARTMENTS.len()].to_string());
        active.push(i % 5 != 0);
        created.push(BASE_TS + (i as i64 * 3600000000));
    }

    let mut columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(StringArray::from(
            names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            emails.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(ages)),
        Arc::new(Float64Array::from(salaries)),
        Arc::new(StringArray::from(
            depts.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        )),
        Arc::new(BooleanArray::from(active)),
        Arc::new(TimestampMicrosecondArray::from(created).with_timezone("+00:00")),
    ];

    if include_title {
        let titles: Vec<Option<&str>> = (start..start + count)
            .map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    Some(TITLES[i % TITLES.len()])
                }
            })
            .collect();
        columns.push(Arc::new(StringArray::from(titles)));
    }

    Ok(RecordBatch::try_new(arrow_schema, columns)?)
}

// ─── Shared data-file writer ────────────────────────────────

async fn write_data_files(
    file_io: &iceberg::io::FileIO,
    schema: &Arc<Schema>,
    data_location: &str,
    snap_id: usize,
    batch: RecordBatch,
) -> Result<Vec<iceberg::spec::DataFile>> {
    let location_gen = DefaultLocationGenerator::with_data_location(data_location.to_string());
    let file_name_gen = DefaultFileNameGenerator::new(
        format!("snap{}-data", snap_id),
        None,
        DataFileFormat::Parquet,
    );
    let pw = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        pw,
        file_io.clone(),
        location_gen,
        file_name_gen,
    );
    let mut writer = DataFileWriterBuilder::new(rolling)
        .build(None)
        .await
        .context("failed to build data file writer")?;

    writer.write(batch).await.context("failed to write batch")?;
    writer.close().await.context("failed to close writer")
}

// ─── Manual metadata mode (local + s3) ─────────────────────

struct SnapshotConfig {
    id: i64,
    parent_id: Option<i64>,
    seq: i64,
    schema_id: i32,
    start_row: usize,
    row_count: usize,
    total_rows: usize,
    total_files: usize,
}

async fn run_manual(file_io: iceberg::io::FileIO, table_location: &str) -> Result<()> {
    let schema_v0 = build_schema_v0()?;
    let schema_v1 = build_schema_v1()?;
    let schema_v0_ref = Arc::new(schema_v0.clone());
    let schema_v1_ref = Arc::new(schema_v1.clone());

    let partition_spec = UnboundPartitionSpec::builder().build();
    let bound_partition = partition_spec.clone().bind(schema_v0_ref.clone())?;

    let arrow_v0 = Arc::new(schema_to_arrow_schema(&schema_v0).context("arrow schema v0")?);
    let arrow_v1 = Arc::new(schema_to_arrow_schema(&schema_v1).context("arrow schema v1")?);

    let data_location = format!("{}/data", table_location);
    let metadata_dir = format!("{}/metadata", table_location);

    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut all_manifest_files = Vec::new();

    let properties = HashMap::from([
        ("owner".into(), "iceberg-tui-demo".into()),
        ("created-at".into(), "2025-01-01T00:00:00Z".into()),
        ("write.format.default".into(), "parquet".into()),
        ("write.parquet.compression-codec".into(), "snappy".into()),
    ]);

    let configs = [
        SnapshotConfig {
            id: 1,
            parent_id: None,
            seq: 1,
            schema_id: 0,
            start_row: 0,
            row_count: 50,
            total_rows: 50,
            total_files: 1,
        },
        SnapshotConfig {
            id: 2,
            parent_id: Some(1),
            seq: 2,
            schema_id: 1,
            start_row: 50,
            row_count: 100,
            total_rows: 150,
            total_files: 2,
        },
        SnapshotConfig {
            id: 3,
            parent_id: Some(2),
            seq: 3,
            schema_id: 1,
            start_row: 150,
            row_count: 50,
            total_rows: 200,
            total_files: 3,
        },
    ];

    let mut prev_metadata: Option<iceberg::spec::TableMetadata> = None;
    let mut version = 0u32;

    for cfg in &configs {
        let use_v1 = cfg.schema_id == 1;
        let schema_ref = if use_v1 {
            schema_v1_ref.clone()
        } else {
            schema_v0_ref.clone()
        };
        let arrow_schema = if use_v1 {
            arrow_v1.clone()
        } else {
            arrow_v0.clone()
        };

        println!(
            "\n--- Snapshot {} (schema {}, rows {}..{}, {} total) ---",
            cfg.id,
            cfg.schema_id,
            cfg.start_row,
            cfg.start_row + cfg.row_count,
            cfg.total_rows
        );

        let batch = generate_batch(arrow_schema, cfg.start_row, cfg.row_count, use_v1)?;
        let data_files = write_data_files(
            &file_io,
            &schema_ref,
            &data_location,
            cfg.id as usize,
            batch,
        )
        .await?;

        for df in &data_files {
            println!("  Data file: {}", df.file_path());
        }

        let manifest_path = format!("{}/snap-{}-manifest-0.avro", metadata_dir, cfg.id);
        let manifest_output = file_io.new_output(&manifest_path)?;

        let mut manifest_writer = iceberg::spec::ManifestWriterBuilder::new(
            manifest_output,
            Some(cfg.id),
            None,
            schema_ref.clone(),
            bound_partition.clone(),
        )
        .build_v2_data();

        for data_file in &data_files {
            manifest_writer.add_file(data_file.clone(), cfg.seq)?;
        }

        let manifest_file = manifest_writer
            .write_manifest_file()
            .await
            .context("failed to write manifest")?;

        println!("  Manifest: {}", manifest_file.manifest_path);
        let mut mf = manifest_file;
        mf.sequence_number = cfg.seq;
        all_manifest_files.push(mf);

        let manifest_list_path = format!("{}/snap-{}-manifest-list.avro", metadata_dir, cfg.id);
        let manifest_list_output = file_io.new_output(&manifest_list_path)?;

        let mut ml_writer = iceberg::spec::ManifestListWriter::v2(
            manifest_list_output,
            cfg.id,
            cfg.parent_id,
            cfg.seq,
        );
        ml_writer
            .add_manifests(all_manifest_files.clone().into_iter())
            .context("failed to add manifests")?;
        ml_writer
            .close()
            .await
            .context("failed to write manifest list")?;
        println!("  Manifest list: {}", manifest_list_path);

        let snap_ts = now_ms - ((configs.len() as i64 - cfg.id) * 60_000);
        let snap = Snapshot::builder()
            .with_snapshot_id(cfg.id)
            .with_parent_snapshot_id(cfg.parent_id)
            .with_sequence_number(cfg.seq)
            .with_schema_id(cfg.schema_id)
            .with_timestamp_ms(snap_ts)
            .with_manifest_list(manifest_list_path)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from([
                    ("added-data-files".into(), data_files.len().to_string()),
                    ("added-records".into(), cfg.row_count.to_string()),
                    ("total-records".into(), cfg.total_rows.to_string()),
                    ("total-data-files".into(), cfg.total_files.to_string()),
                ]),
            })
            .build();

        version += 1;
        let prev_path = if version > 1 {
            Some(format!("{}/v{}.metadata.json", metadata_dir, version - 1))
        } else {
            None
        };

        let mut builder = if let Some(meta) = prev_metadata.take() {
            meta.into_builder(prev_path)
        } else {
            iceberg::spec::TableMetadataBuilder::new(
                schema_v0.clone(),
                partition_spec.clone(),
                SortOrder::unsorted_order(),
                table_location.to_string(),
                FormatVersion::V2,
                properties.clone(),
            )?
        };

        if use_v1 && cfg.parent_id.is_none_or(|pid| pid < 2) {
            builder = builder.add_current_schema(schema_v1.clone())?;
        }
        builder = builder.set_branch_snapshot(snap, "main")?;

        let result = builder.build()?;
        let metadata_json = serde_json::to_string_pretty(&result.metadata)?;

        let metadata_path = format!("{}/v{}.metadata.json", metadata_dir, version);
        let out = file_io.new_output(&metadata_path)?;
        out.write(metadata_json.into())
            .await
            .context("failed to write metadata")?;
        println!("  Wrote metadata: {}", metadata_path);

        prev_metadata = Some(result.metadata);
    }

    let hint_path = format!("{}/version-hint.text", metadata_dir);
    let output = file_io.new_output(&hint_path)?;
    output
        .write(version.to_string().into())
        .await
        .context("failed to write version-hint")?;
    println!("\n  Wrote version-hint: {}", hint_path);

    println!();
    println!("Sample table created with 3 snapshots + schema evolution (3 metadata files):");
    println!("  v1.metadata.json — Snapshot 1:  50 rows (schema 0 — 8 columns)");
    println!("  v2.metadata.json — Snapshot 2: 150 rows (schema 1 — adds `title`)");
    println!("  v3.metadata.json — Snapshot 3: 200 rows (schema 1) <- current HEAD");

    Ok(())
}

// ─── Catalog mode (Transaction API) ─────────────────────────

async fn run_catalog() -> Result<()> {
    let catalog_uri = env_or("CATALOG_URI", "http://localhost:8181");
    let s3_endpoint = env_or("S3_ENDPOINT", "http://localhost:9000");

    println!("Connecting to REST catalog at: {}", catalog_uri);

    let mut props = HashMap::from([(REST_CATALOG_PROP_URI.to_string(), catalog_uri.clone())]);
    if let Ok(ak) = std::env::var("AWS_ACCESS_KEY_ID") {
        props.insert("s3.access-key-id".into(), ak);
    }
    if let Ok(sk) = std::env::var("AWS_SECRET_ACCESS_KEY") {
        props.insert("s3.secret-access-key".into(), sk);
    }
    props.insert("s3.endpoint".into(), s3_endpoint);
    props.insert("s3.region".into(), "us-east-1".into());
    props.insert("s3.path-style-access".into(), "true".into());

    let catalog = RestCatalogBuilder::default()
        .load("rest", props)
        .await
        .context("failed to connect to REST catalog")?;

    let ns = NamespaceIdent::new("demo".to_string());
    if let Err(e) = catalog.create_namespace(&ns, HashMap::new()).await {
        let msg = e.to_string();
        if msg.contains("already exists") || msg.contains("AlreadyExists") {
            println!("Namespace already exists: demo");
        } else {
            return Err(anyhow::anyhow!("{msg}")).context("failed to create namespace");
        }
    } else {
        println!("Created namespace: demo");
    }

    let table_ident = TableIdent::new(ns.clone(), "sample_data".to_string());
    if let Err(e) = catalog.drop_table(&table_ident).await {
        let msg = e.to_string();
        if !msg.contains("NoSuchTable") && !msg.contains("not exist") {
            return Err(anyhow::anyhow!("{msg}")).context("failed to drop table");
        }
    } else {
        println!("Dropped existing table: demo.sample_data");
    }

    let schema = build_schema_v0()?;
    let creation = TableCreation::builder()
        .name("sample_data".to_string())
        .schema(schema.clone())
        .build();

    let table = catalog
        .create_table(&ns, creation)
        .await
        .context("failed to create table")?;

    println!(
        "Created table: demo.sample_data at {}",
        table.metadata().location()
    );

    let schema_ref = Arc::new(schema.clone());
    let arrow_schema =
        Arc::new(schema_to_arrow_schema(&schema).context("failed to convert schema to arrow")?);

    let batches: [(&str, usize, usize, usize); 3] = [
        ("initial load", 0, 50, 50),
        ("+100 employees", 50, 100, 150),
        ("+50 employees", 150, 50, 200),
    ];

    for (i, (label, start, count, total)) in batches.iter().enumerate() {
        println!(
            "\n--- Snapshot {} ({}, {} total rows) ---",
            i + 1,
            label,
            total
        );

        let table = catalog
            .load_table(&table_ident)
            .await
            .context("failed to reload table")?;

        let batch = generate_batch(arrow_schema.clone(), *start, *count, false)?;
        let data_location = format!("{}/data", table.metadata().location());
        let data_files =
            write_data_files(table.file_io(), &schema_ref, &data_location, i + 1, batch).await?;

        println!(
            "  Wrote {} data file(s) with {} rows",
            data_files.len(),
            count
        );
        for df in &data_files {
            println!("    - {}", df.file_path());
        }

        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(data_files);
        let tx = action.apply(tx)?;
        tx.commit(&catalog)
            .await
            .context("failed to commit transaction")?;

        println!("  Committed snapshot {}", i + 1);
    }

    println!();
    println!("Sample table created with 3 snapshots:");
    println!("  Snapshot 1:  50 rows (initial load)");
    println!("  Snapshot 2: 150 rows (+100 employees)");
    println!("  Snapshot 3: 200 rows (+50 employees) <- current HEAD");
    println!();
    println!("Read with:");
    println!(
        "  cargo run -- catalog --uri {} --table demo.sample_data",
        catalog_uri
    );

    Ok(())
}

// ─── Helpers ────────────────────────────────────────────────

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

// ─── Main ───────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    match args.target {
        Target::Local => {
            let table_dir = std::env::current_dir()?
                .join("examples")
                .join("sample_table");

            if table_dir.exists() {
                std::fs::remove_dir_all(&table_dir)?;
            }
            std::fs::create_dir_all(table_dir.join("metadata"))?;
            std::fs::create_dir_all(table_dir.join("data"))?;

            let table_location = table_dir.canonicalize()?.to_string_lossy().to_string();
            let file_io = FileIOBuilder::new_fs_io().build()?;

            println!("Creating sample Iceberg table at: {}", table_dir.display());
            run_manual(file_io, &table_location).await?;

            println!();
            println!("Test with:");
            println!("  cargo run -- open examples/sample_table");
            println!();
            println!("Try time-travel + schema evolution:");
            println!("  1. Press 3 for Snapshots tab, select snapshot 1, press Enter");
            println!("  2. Press 2 — Schema tab shows schema 0 (8 fields, no `title`)");
            println!("  3. Press 1 — Data tab shows 50 rows, no `title` column");
            println!("  4. Press 5 — Props tab shows Snapshot 1 details");
            println!("  5. Select snapshot 3 to return — schema 1 (9 fields with `title`)");
        }
        Target::S3 => {
            let endpoint = env_or("S3_ENDPOINT", "http://localhost:9000");
            let region = env_or("AWS_REGION", "us-east-1");
            let bucket = env_or("S3_BUCKET", "warehouse");
            let table_name = env_or("S3_TABLE_NAME", "sample_table");

            let table_location = format!("s3://{}/{}", bucket, table_name);

            let mut builder = FileIOBuilder::new("s3")
                .with_prop("s3.endpoint", &endpoint)
                .with_prop("s3.region", &region)
                .with_prop("s3.path-style-access", "true");

            if let Ok(ak) = std::env::var("AWS_ACCESS_KEY_ID") {
                builder = builder.with_prop("s3.access-key-id", &ak);
            }
            if let Ok(sk) = std::env::var("AWS_SECRET_ACCESS_KEY") {
                builder = builder.with_prop("s3.secret-access-key", &sk);
            }

            let file_io = builder.build().context("failed to build S3 FileIO")?;

            println!("Creating sample Iceberg table at: {}", table_location);
            println!("  Endpoint: {}", endpoint);
            println!("  Region:   {}", region);
            run_manual(file_io, &table_location).await?;

            println!();
            println!("Read with:");
            println!("  AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \\");
            println!(
                "    cargo run -- open {} --endpoint {}",
                table_location, endpoint
            );
            println!();
            println!(
                "Try time-travel: press 3 for Snapshots tab, select an older snapshot, press Enter."
            );
        }
        Target::Catalog => {
            run_catalog().await?;
            println!();
            println!(
                "Try time-travel: press 3 for Snapshots tab, select an older snapshot, press Enter."
            );
        }
    }

    Ok(())
}
