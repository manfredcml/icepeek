use clap::{Parser, Subcommand};

use crate::loader::file_io::StorageConfig;

pub const DEFAULT_PAGE_SIZE: usize = 500;

pub fn effective_limit(limit: Option<usize>, no_limit: bool) -> Option<usize> {
    if no_limit {
        None
    } else {
        Some(limit.unwrap_or(DEFAULT_PAGE_SIZE))
    }
}

#[derive(Parser)]
#[command(name = "icepeek", about = "Terminal-based Apache Iceberg table viewer")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Clone)]
pub enum Command {
    /// Open a table from a path or S3 URL
    Open {
        path: String,

        #[arg(short, long, value_delimiter = ',')]
        columns: Option<Vec<String>>,

        #[arg(short, long)]
        limit: Option<usize>,

        #[arg(long)]
        no_limit: bool,

        #[command(flatten)]
        storage: StorageConfig,
    },

    /// Open a table from a REST catalog
    Catalog {
        #[arg(long)]
        uri: String,

        #[arg(long)]
        table: String,

        #[arg(short, long, value_delimiter = ',')]
        columns: Option<Vec<String>>,

        #[arg(short, long)]
        limit: Option<usize>,

        #[arg(long)]
        no_limit: bool,

        #[command(flatten)]
        storage: StorageConfig,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_open_with_limit() {
        let cli = Cli::parse_from(["icepeek", "open", "/tmp/table", "--limit", "100"]);
        let Command::Open { limit, .. } = cli.command else {
            panic!("expected Open");
        };
        assert_eq!(limit, Some(100));
    }

    #[test]
    fn parse_open_without_limit() {
        let cli = Cli::parse_from(["icepeek", "open", "/tmp/table"]);
        let Command::Open { limit, .. } = cli.command else {
            panic!("expected Open");
        };
        assert_eq!(limit, None);
    }

    #[test]
    fn parse_open_limit_short_flag() {
        let cli = Cli::parse_from(["icepeek", "open", "/tmp/table", "-l", "50"]);
        let Command::Open { limit, .. } = cli.command else {
            panic!("expected Open");
        };
        assert_eq!(limit, Some(50));
    }

    #[test]
    fn parse_open_no_limit_flag() {
        let cli = Cli::parse_from(["icepeek", "open", "/tmp/table", "--no-limit"]);
        let Command::Open {
            limit, no_limit, ..
        } = cli.command
        else {
            panic!("expected Open");
        };
        assert_eq!(limit, None);
        assert!(no_limit);
    }

    #[test]
    fn parse_catalog_with_limit() {
        let cli = Cli::parse_from([
            "icepeek",
            "catalog",
            "--uri",
            "http://localhost",
            "--table",
            "db.t",
            "--limit",
            "200",
        ]);
        let Command::Catalog { limit, .. } = cli.command else {
            panic!("expected Catalog");
        };
        assert_eq!(limit, Some(200));
    }

    #[test]
    fn parse_catalog_without_limit() {
        let cli = Cli::parse_from([
            "icepeek",
            "catalog",
            "--uri",
            "http://localhost",
            "--table",
            "db.t",
        ]);
        let Command::Catalog { limit, .. } = cli.command else {
            panic!("expected Catalog");
        };
        assert_eq!(limit, None);
    }

    #[test]
    fn parse_catalog_no_limit_flag() {
        let cli = Cli::parse_from([
            "icepeek",
            "catalog",
            "--uri",
            "http://localhost",
            "--table",
            "db.t",
            "--no-limit",
        ]);
        let Command::Catalog {
            limit, no_limit, ..
        } = cli.command
        else {
            panic!("expected Catalog");
        };
        assert_eq!(limit, None);
        assert!(no_limit);
    }

    #[test]
    fn parse_open_with_s3_endpoint() {
        let cli = Cli::parse_from([
            "icepeek",
            "open",
            "s3://bucket/table",
            "--s3-endpoint",
            "http://localhost:9000",
        ]);
        let Command::Open { storage, .. } = cli.command else {
            panic!("expected Open");
        };
        assert_eq!(
            storage.s3_endpoint.as_deref(),
            Some("http://localhost:9000")
        );
    }

    #[test]
    fn parse_catalog_with_storage_config() {
        let cli = Cli::parse_from([
            "icepeek",
            "catalog",
            "--uri",
            "http://localhost:8181",
            "--table",
            "db.t",
            "--s3-region",
            "eu-west-1",
        ]);
        let Command::Catalog { storage, .. } = cli.command else {
            panic!("expected Catalog");
        };
        assert_eq!(storage.s3_region, "eu-west-1");
    }

    #[test]
    fn effective_limit_default() {
        assert_eq!(effective_limit(None, false), Some(DEFAULT_PAGE_SIZE));
    }

    #[test]
    fn effective_limit_explicit() {
        assert_eq!(effective_limit(Some(100), false), Some(100));
    }

    #[test]
    fn effective_limit_no_limit() {
        assert_eq!(effective_limit(None, true), None);
    }

    #[test]
    fn effective_limit_no_limit_overrides_explicit() {
        assert_eq!(effective_limit(Some(100), true), None);
    }
}
