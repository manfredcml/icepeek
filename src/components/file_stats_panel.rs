use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

use crate::event::{Action, AppMessage};
use crate::model::table_info::DataFileInfo;
use crate::ui::layout::SplitLayout;
use crate::ui::theme::Theme;

use super::Component;

const BYTES_PER_KB: i64 = 1024;
const BYTES_PER_MB: i64 = BYTES_PER_KB * 1024;
const BYTES_PER_GB: i64 = BYTES_PER_MB * 1024;
const LEFT_PANEL_PERCENT: u16 = 40;
const BLOCK_CHARS: [char; 8] = ['█', '▉', '▊', '▋', '▌', '▍', '▎', '▏'];

pub struct FileStatsPanel {
    files: Vec<DataFileInfo>,
    stats: Option<FileStats>,
    scroll_offset: u16,
    loaded: bool,
}

struct FileStats {
    total_files: usize,
    total_size: i64,
    total_rows: i64,
    avg_size: f64,
    median_size: i64,
    min_size: i64,
    max_size: i64,
    avg_rows: f64,
    median_rows: i64,
    min_rows: i64,
    max_rows: i64,
    small_file_count: usize,
    large_file_count: usize,
    size_buckets: Vec<Bucket>,
    row_buckets: Vec<Bucket>,
}

struct Bucket {
    label: String,
    count: usize,
}

impl FileStatsPanel {
    pub fn new() -> Self {
        Self {
            files: vec![],
            stats: None,
            scroll_offset: 0,
            loaded: false,
        }
    }

    pub fn needs_load(&self) -> bool {
        !self.loaded
    }

    pub fn invalidate(&mut self) {
        self.loaded = false;
        self.files.clear();
        self.stats = None;
        self.scroll_offset = 0;
    }

    fn compute_stats(files: &[DataFileInfo]) -> FileStats {
        if files.is_empty() {
            return FileStats {
                total_files: 0,
                total_size: 0,
                total_rows: 0,
                avg_size: 0.0,
                median_size: 0,
                min_size: 0,
                max_size: 0,
                avg_rows: 0.0,
                median_rows: 0,
                min_rows: 0,
                max_rows: 0,
                small_file_count: 0,
                large_file_count: 0,
                size_buckets: Self::compute_size_buckets(&[]),
                row_buckets: Self::compute_row_buckets(&[]),
            };
        }

        let mut sizes: Vec<i64> = files.iter().map(|f| f.file_size_bytes).collect();
        let mut rows: Vec<i64> = files.iter().map(|f| f.record_count).collect();
        sizes.sort_unstable();
        rows.sort_unstable();

        let n = files.len();
        let total_size: i64 = sizes.iter().sum();
        let total_rows: i64 = rows.iter().sum();

        let small_file_count = sizes.iter().filter(|&&s| s < BYTES_PER_MB).count();
        let large_file_count = sizes.iter().filter(|&&s| s > 100 * BYTES_PER_MB).count();

        FileStats {
            total_files: n,
            total_size,
            total_rows,
            avg_size: total_size as f64 / n as f64,
            median_size: sizes[n / 2],
            min_size: sizes[0],
            max_size: sizes[n - 1],
            avg_rows: total_rows as f64 / n as f64,
            median_rows: rows[n / 2],
            min_rows: rows[0],
            max_rows: rows[n - 1],
            small_file_count,
            large_file_count,
            size_buckets: Self::compute_size_buckets(&sizes),
            row_buckets: Self::compute_row_buckets(&rows),
        }
    }

    fn compute_size_buckets(sizes: &[i64]) -> Vec<Bucket> {
        let thresholds: [(i64, &str); 5] = [
            (BYTES_PER_MB, "< 1 MB"),
            (10 * BYTES_PER_MB, "1-10 MB"),
            (50 * BYTES_PER_MB, "10-50 MB"),
            (100 * BYTES_PER_MB, "50-100 MB"),
            (i64::MAX, "> 100 MB"),
        ];

        let mut buckets: Vec<Bucket> = thresholds
            .iter()
            .map(|(_, label)| Bucket {
                label: label.to_string(),
                count: 0,
            })
            .collect();

        for &s in sizes {
            for (i, &(thresh, _)) in thresholds.iter().enumerate() {
                if i == 0 && s < thresh {
                    buckets[i].count += 1;
                    break;
                } else if i > 0 && i < thresholds.len() - 1 {
                    let prev = thresholds[i - 1].0;
                    if s >= prev && s < thresh {
                        buckets[i].count += 1;
                        break;
                    }
                } else if i == thresholds.len() - 1 {
                    let prev = thresholds[i - 1].0;
                    if s >= prev {
                        buckets[i].count += 1;
                        break;
                    }
                }
            }
        }

        buckets
    }

    fn compute_row_buckets(rows: &[i64]) -> Vec<Bucket> {
        let thresholds: [(i64, &str); 5] = [
            (1_000, "0-1K"),
            (10_000, "1K-10K"),
            (100_000, "10K-100K"),
            (1_000_000, "100K-1M"),
            (i64::MAX, "> 1M"),
        ];

        let mut buckets: Vec<Bucket> = thresholds
            .iter()
            .map(|(_, label)| Bucket {
                label: label.to_string(),
                count: 0,
            })
            .collect();

        for &r in rows {
            for (i, &(thresh, _)) in thresholds.iter().enumerate() {
                if i == 0 && r < thresh {
                    buckets[i].count += 1;
                    break;
                } else if i > 0 && i < thresholds.len() - 1 {
                    let prev = thresholds[i - 1].0;
                    if r >= prev && r < thresh {
                        buckets[i].count += 1;
                        break;
                    }
                } else if i == thresholds.len() - 1 {
                    let prev = thresholds[i - 1].0;
                    if r >= prev {
                        buckets[i].count += 1;
                        break;
                    }
                }
            }
        }

        buckets
    }

    fn format_size(bytes: i64) -> String {
        if bytes < BYTES_PER_KB {
            format!("{} B", bytes)
        } else if bytes < BYTES_PER_MB {
            format!("{:.1} KB", bytes as f64 / BYTES_PER_KB as f64)
        } else if bytes < BYTES_PER_GB {
            format!("{:.1} MB", bytes as f64 / BYTES_PER_MB as f64)
        } else {
            format!("{:.1} GB", bytes as f64 / BYTES_PER_GB as f64)
        }
    }

    fn render_bar(fraction: f64, max_width: u16) -> String {
        if fraction <= 0.0 || max_width == 0 {
            return String::new();
        }
        let f = fraction.min(1.0);
        let total_eighths = (f * max_width as f64 * 8.0) as usize;
        let full = total_eighths / 8;
        let remainder = total_eighths % 8;

        let mut bar = String::with_capacity(full + 1);
        for _ in 0..full {
            bar.push(BLOCK_CHARS[0]);
        }
        if remainder > 0 {
            bar.push(BLOCK_CHARS[8 - remainder]);
        }
        bar
    }

    fn build_summary_lines(&self) -> Vec<Line<'_>> {
        let Some(ref s) = self.stats else {
            return vec![Line::styled(
                "No file statistics available",
                Theme::field_id(),
            )];
        };

        vec![
            Line::styled("─── Summary ───", Theme::title()),
            Line::raw(""),
            Line::from(vec![
                Span::styled("Total files:  ", Theme::label()),
                Span::styled(s.total_files.to_string(), Theme::value()),
            ]),
            Line::from(vec![
                Span::styled("Total size:   ", Theme::label()),
                Span::styled(Self::format_size(s.total_size), Theme::value()),
            ]),
            Line::from(vec![
                Span::styled("Total rows:   ", Theme::label()),
                Span::styled(s.total_rows.to_string(), Theme::value()),
            ]),
            Line::raw(""),
            Line::styled("─── File Size ───", Theme::title()),
            Line::raw(""),
            Line::from(vec![
                Span::styled("Average:      ", Theme::label()),
                Span::styled(Self::format_size(s.avg_size as i64), Theme::value()),
            ]),
            Line::from(vec![
                Span::styled("Median:       ", Theme::label()),
                Span::styled(Self::format_size(s.median_size), Theme::value()),
            ]),
            Line::from(vec![
                Span::styled("Min:          ", Theme::label()),
                Span::styled(Self::format_size(s.min_size), Theme::value()),
            ]),
            Line::from(vec![
                Span::styled("Max:          ", Theme::label()),
                Span::styled(Self::format_size(s.max_size), Theme::value()),
            ]),
            Line::raw(""),
            Line::styled("─── Row Count ───", Theme::title()),
            Line::raw(""),
            Line::from(vec![
                Span::styled("Average:      ", Theme::label()),
                Span::styled(format!("{:.0}", s.avg_rows), Theme::value()),
            ]),
            Line::from(vec![
                Span::styled("Median:       ", Theme::label()),
                Span::styled(s.median_rows.to_string(), Theme::value()),
            ]),
            Line::from(vec![
                Span::styled("Min:          ", Theme::label()),
                Span::styled(s.min_rows.to_string(), Theme::value()),
            ]),
            Line::from(vec![
                Span::styled("Max:          ", Theme::label()),
                Span::styled(s.max_rows.to_string(), Theme::value()),
            ]),
            Line::raw(""),
            Line::styled("─── Alerts ───", Theme::title()),
            Line::raw(""),
            Line::from(vec![
                Span::styled("Small (< 1MB): ", Theme::label()),
                Span::styled(s.small_file_count.to_string(), Theme::value()),
            ]),
            Line::from(vec![
                Span::styled("Large (>100MB):", Theme::label()),
                Span::styled(format!(" {}", s.large_file_count), Theme::value()),
            ]),
        ]
    }

    fn build_histogram_lines<'a>(
        bar_width: u16,
        buckets: &[Bucket],
        title: &'a str,
    ) -> Vec<Line<'a>> {
        let max_count = buckets.iter().map(|b| b.count).max().unwrap_or(0);
        let mut lines = vec![
            Line::styled(format!("─── {} ───", title), Theme::title()),
            Line::raw(""),
        ];

        for b in buckets {
            let fraction = if max_count > 0 {
                b.count as f64 / max_count as f64
            } else {
                0.0
            };
            let bar = Self::render_bar(fraction, bar_width.saturating_sub(20));
            lines.push(Line::from(vec![
                Span::styled(format!("{:>10} ", b.label), Theme::label()),
                Span::styled(bar, Theme::value()),
                Span::styled(format!(" {}", b.count), Theme::field_id()),
            ]));
        }

        lines
    }
}

impl Component for FileStatsPanel {
    fn handle_key(&mut self, key: KeyEvent) -> Option<Action> {
        match key.code {
            KeyCode::Down | KeyCode::Char('j') => {
                self.scroll_offset = self.scroll_offset.saturating_add(1);
                None
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.scroll_offset = self.scroll_offset.saturating_sub(1);
                None
            }
            _ => None,
        }
    }

    fn handle_message(&mut self, msg: &AppMessage) -> Option<Action> {
        let AppMessage::DataFileStatsReady(grouped) = msg else {
            return None;
        };
        self.files = grouped.iter().flatten().cloned().collect();
        self.stats = Some(Self::compute_stats(&self.files));
        self.loaded = true;
        self.scroll_offset = 0;
        None
    }

    fn render(&mut self, frame: &mut Frame, area: Rect, _focused: bool) {
        if !self.loaded {
            let block = Block::default()
                .borders(Borders::ALL)
                .title(" Stats ")
                .border_style(Theme::border_unfocused());
            let p = Paragraph::new(Line::styled(
                "Press 3 (Files tab) first to load file data, then switch to 6 (Stats)",
                Theme::status_loading(),
            ))
            .block(block);
            frame.render_widget(p, area);
            return;
        }

        let split = SplitLayout::new(area, LEFT_PANEL_PERCENT);

        let summary_lines = self.build_summary_lines();
        let left_block = Block::default()
            .borders(Borders::ALL)
            .title(" Summary ")
            .border_style(Theme::border_focused());
        let left = Paragraph::new(summary_lines)
            .block(left_block)
            .wrap(Wrap { trim: false })
            .scroll((self.scroll_offset, 0));
        frame.render_widget(left, split.left);

        let right_chunks =
            Layout::vertical([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(split.right);

        let inner_width = right_chunks[0].width.saturating_sub(2);

        let size_lines =
            Self::build_histogram_lines(inner_width, &self.size_buckets(), "Size Distribution");
        let size_block = Block::default()
            .borders(Borders::ALL)
            .title(" Size Distribution ")
            .border_style(Theme::border_unfocused());
        let size_p = Paragraph::new(size_lines)
            .block(size_block)
            .wrap(Wrap { trim: false });
        frame.render_widget(size_p, right_chunks[0]);

        let row_lines =
            Self::build_histogram_lines(inner_width, &self.row_buckets(), "Row Distribution");
        let row_block = Block::default()
            .borders(Borders::ALL)
            .title(" Row Distribution ")
            .border_style(Theme::border_unfocused());
        let row_p = Paragraph::new(row_lines)
            .block(row_block)
            .wrap(Wrap { trim: false });
        frame.render_widget(row_p, right_chunks[1]);
    }
}

impl FileStatsPanel {
    fn size_buckets(&self) -> Vec<Bucket> {
        self.stats
            .as_ref()
            .map(|s| {
                s.size_buckets
                    .iter()
                    .map(|b| Bucket {
                        label: b.label.clone(),
                        count: b.count,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn row_buckets(&self) -> Vec<Bucket> {
        self.stats
            .as_ref()
            .map(|s| {
                s.row_buckets
                    .iter()
                    .map(|b| Bucket {
                        label: b.label.clone(),
                        count: b.count,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_file(size: i64, rows: i64) -> DataFileInfo {
        DataFileInfo {
            file_path: format!("/data/file_{}_{}.parquet", size, rows),
            file_format: "Parquet".into(),
            record_count: rows,
            file_size_bytes: size,
            null_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            partition_data: HashMap::new(),
        }
    }

    #[test]
    fn initial_state() {
        let panel = FileStatsPanel::new();
        assert!(!panel.loaded);
        assert!(panel.files.is_empty());
        assert!(panel.stats.is_none());
        assert!(panel.needs_load());
    }

    #[test]
    fn compute_stats_empty() {
        let s = FileStatsPanel::compute_stats(&[]);
        assert_eq!(s.total_files, 0);
        assert_eq!(s.total_size, 0);
        assert_eq!(s.total_rows, 0);
        assert_eq!(s.avg_size, 0.0);
        assert_eq!(s.median_size, 0);
    }

    #[test]
    fn compute_stats_single_file() {
        let files = [make_file(5_000_000, 1000)];
        let s = FileStatsPanel::compute_stats(&files);
        assert_eq!(s.total_files, 1);
        assert_eq!(s.total_size, 5_000_000);
        assert_eq!(s.total_rows, 1000);
        assert_eq!(s.avg_size, 5_000_000.0);
        assert_eq!(s.median_size, 5_000_000);
        assert_eq!(s.min_size, 5_000_000);
        assert_eq!(s.max_size, 5_000_000);
        assert_eq!(s.avg_rows, 1000.0);
        assert_eq!(s.median_rows, 1000);
        assert_eq!(s.min_rows, 1000);
        assert_eq!(s.max_rows, 1000);
    }

    #[test]
    fn compute_stats_multiple_files() {
        let files = [
            make_file(1_000_000, 100),
            make_file(3_000_000, 300),
            make_file(5_000_000, 500),
        ];
        let s = FileStatsPanel::compute_stats(&files);
        assert_eq!(s.total_files, 3);
        assert_eq!(s.total_size, 9_000_000);
        assert_eq!(s.total_rows, 900);
        assert!((s.avg_size - 3_000_000.0).abs() < 1.0);
        assert_eq!(s.median_size, 3_000_000);
        assert_eq!(s.min_size, 1_000_000);
        assert_eq!(s.max_size, 5_000_000);
        assert!((s.avg_rows - 300.0).abs() < 0.1);
        assert_eq!(s.median_rows, 300);
        assert_eq!(s.min_rows, 100);
        assert_eq!(s.max_rows, 500);
    }

    #[test]
    fn small_large_file_counts() {
        let files = [
            make_file(500, 10),             // small (< 1MB)
            make_file(500_000, 100),        // small (< 1MB)
            make_file(5_000_000, 1000),     // normal
            make_file(200_000_000, 50_000), // large (> 100MB)
        ];
        let s = FileStatsPanel::compute_stats(&files);
        assert_eq!(s.small_file_count, 2);
        assert_eq!(s.large_file_count, 1);
    }

    #[test]
    fn size_bucket_distribution() {
        let files = [
            make_file(500, 10),                // < 1 MB
            make_file(5 * BYTES_PER_MB, 10),   // 1-10 MB
            make_file(30 * BYTES_PER_MB, 10),  // 10-50 MB
            make_file(75 * BYTES_PER_MB, 10),  // 50-100 MB
            make_file(200 * BYTES_PER_MB, 10), // > 100 MB
        ];
        let s = FileStatsPanel::compute_stats(&files);
        assert_eq!(s.size_buckets[0].count, 1); // < 1 MB
        assert_eq!(s.size_buckets[1].count, 1); // 1-10 MB
        assert_eq!(s.size_buckets[2].count, 1); // 10-50 MB
        assert_eq!(s.size_buckets[3].count, 1); // 50-100 MB
        assert_eq!(s.size_buckets[4].count, 1); // > 100 MB
    }

    #[test]
    fn row_bucket_distribution() {
        let files = [
            make_file(100, 500),       // 0-1K
            make_file(100, 5_000),     // 1K-10K
            make_file(100, 50_000),    // 10K-100K
            make_file(100, 500_000),   // 100K-1M
            make_file(100, 5_000_000), // > 1M
        ];
        let s = FileStatsPanel::compute_stats(&files);
        assert_eq!(s.row_buckets[0].count, 1); // 0-1K
        assert_eq!(s.row_buckets[1].count, 1); // 1K-10K
        assert_eq!(s.row_buckets[2].count, 1); // 10K-100K
        assert_eq!(s.row_buckets[3].count, 1); // 100K-1M
        assert_eq!(s.row_buckets[4].count, 1); // > 1M
    }

    #[test]
    fn render_bar_full() {
        let bar = FileStatsPanel::render_bar(1.0, 10);
        assert_eq!(bar.chars().count(), 10);
        assert!(bar.chars().all(|c| c == '█'));
    }

    #[test]
    fn render_bar_zero() {
        let bar = FileStatsPanel::render_bar(0.0, 10);
        assert!(bar.is_empty());
    }

    #[test]
    fn render_bar_fractional() {
        let bar = FileStatsPanel::render_bar(0.5, 10);
        assert!(!bar.is_empty());
        let full_blocks = bar.chars().filter(|&c| c == '█').count();
        assert!(full_blocks >= 4 && full_blocks <= 5);
    }

    #[test]
    fn format_size_works() {
        assert_eq!(FileStatsPanel::format_size(500), "500 B");
        assert_eq!(FileStatsPanel::format_size(1500), "1.5 KB");
        assert_eq!(FileStatsPanel::format_size(1_500_000), "1.4 MB");
        assert_eq!(FileStatsPanel::format_size(1_500_000_000), "1.4 GB");
    }

    #[test]
    fn handle_message_flattens_grouped() {
        let mut panel = FileStatsPanel::new();
        let grouped = vec![
            vec![make_file(100, 10), make_file(200, 20)],
            vec![make_file(300, 30)],
        ];
        panel.handle_message(&AppMessage::DataFileStatsReady(grouped));
        assert!(panel.loaded);
        assert_eq!(panel.files.len(), 3);
        assert!(panel.stats.is_some());
    }

    #[test]
    fn invalidate_resets() {
        let mut panel = FileStatsPanel::new();
        panel.handle_message(&AppMessage::DataFileStatsReady(vec![vec![make_file(
            100, 10,
        )]]));
        assert!(panel.loaded);

        panel.invalidate();
        assert!(!panel.loaded);
        assert!(panel.files.is_empty());
        assert!(panel.stats.is_none());
        assert!(panel.needs_load());
    }
}
