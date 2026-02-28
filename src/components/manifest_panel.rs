use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Frame;

use crate::event::{Action, AppMessage};
use crate::model::table_info::{DataFileInfo, ManifestInfo};
use crate::ui::layout::SplitLayout;
use crate::ui::theme::Theme;

use super::Component;

const BYTES_PER_KB: i64 = 1024;
const BYTES_PER_MB: i64 = BYTES_PER_KB * 1024;
const BYTES_PER_GB: i64 = BYTES_PER_MB * 1024;
const LEFT_PANEL_PERCENT: u16 = 40;

pub struct ManifestPanel {
    manifests: Vec<ManifestInfo>,
    files_by_manifest: Vec<Vec<DataFileInfo>>,
    manifest_list_state: ListState,
    data_file_list_state: ListState,
    focus_left: bool,
    loaded: bool,
}

impl ManifestPanel {
    pub fn new() -> Self {
        Self {
            manifests: vec![],
            files_by_manifest: vec![],
            manifest_list_state: ListState::default(),
            data_file_list_state: ListState::default(),
            focus_left: true,
            loaded: false,
        }
    }

    pub fn needs_load(&self) -> bool {
        !self.loaded
    }

    pub fn invalidate(&mut self) {
        self.loaded = false;
        self.manifests.clear();
        self.files_by_manifest.clear();
        self.manifest_list_state = ListState::default();
        self.data_file_list_state = ListState::default();
    }

    fn selected_files(&self) -> &[DataFileInfo] {
        let Some(idx) = self.manifest_list_state.selected() else {
            return &[];
        };
        self.files_by_manifest
            .get(idx)
            .map_or(&[], |v| v.as_slice())
    }

    fn active_list(&mut self) -> (&mut ListState, usize) {
        if self.focus_left {
            (&mut self.manifest_list_state, self.manifests.len())
        } else {
            let len = self
                .manifest_list_state
                .selected()
                .and_then(|i| self.files_by_manifest.get(i))
                .map_or(0, |v| v.len());
            (&mut self.data_file_list_state, len)
        }
    }

    fn selected_data_file(&self) -> Option<&DataFileInfo> {
        let files = self.selected_files();
        self.data_file_list_state
            .selected()
            .and_then(|i| files.get(i))
    }

    fn reset_data_file_cursor(&mut self) {
        let has_files = !self.selected_files().is_empty();
        self.data_file_list_state
            .select(if has_files { Some(0) } else { None });
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

    fn build_right_panel_lines(&self) -> Vec<Line<'_>> {
        if !self.loaded {
            return vec![Line::styled(
                "Loading manifests...",
                Theme::status_loading(),
            )];
        }
        let files = self.selected_files();
        if files.is_empty() {
            return vec![Line::styled("No data files found", Theme::field_id())];
        }

        let total_files = files.len();
        let total_rows: i64 = files.iter().map(|f| f.record_count).sum();
        let total_size: i64 = files.iter().map(|f| f.file_size_bytes).sum();

        let mut lines = vec![
            Line::from(vec![
                Span::styled("Files: ", Theme::label()),
                Span::styled(total_files.to_string(), Theme::value()),
                Span::raw("  "),
                Span::styled("Rows: ", Theme::label()),
                Span::styled(total_rows.to_string(), Theme::value()),
                Span::raw("  "),
                Span::styled("Size: ", Theme::label()),
                Span::styled(Self::format_size(total_size), Theme::value()),
            ]),
            Line::raw(""),
        ];

        if let Some(df) = self.selected_data_file() {
            lines.extend(Self::build_data_file_lines(df));
        }

        lines
    }

    fn build_data_file_lines(df: &DataFileInfo) -> Vec<Line<'_>> {
        let filename = df
            .file_path
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .to_string();
        let mut lines = vec![
            Line::from(vec![
                Span::styled("File: ", Theme::label()),
                Span::styled(filename, Theme::value()),
            ]),
            Line::from(vec![
                Span::styled("Format: ", Theme::label()),
                Span::styled(df.file_format.clone(), Theme::value()),
            ]),
            Line::from(vec![
                Span::styled("Records: ", Theme::label()),
                Span::styled(df.record_count.to_string(), Theme::value()),
            ]),
            Line::from(vec![
                Span::styled("Size: ", Theme::label()),
                Span::styled(Self::format_size(df.file_size_bytes), Theme::value()),
            ]),
        ];

        if !df.partition_data.is_empty() {
            lines.push(Line::raw(""));
            lines.push(Line::styled("─── Partition ───", Theme::title()));
            for (k, v) in &df.partition_data {
                lines.push(Line::from(vec![
                    Span::styled(format!("  {}: ", k), Theme::label()),
                    Span::styled(v.clone(), Theme::value()),
                ]));
            }
        }

        if df.lower_bounds.is_empty() && df.upper_bounds.is_empty() {
            return lines;
        }

        lines.push(Line::raw(""));
        lines.push(Line::styled("─── Column Stats ───", Theme::title()));

        let mut col_ids: Vec<i32> = df
            .lower_bounds
            .keys()
            .chain(df.upper_bounds.keys())
            .copied()
            .collect();
        col_ids.sort();
        col_ids.dedup();

        for id in col_ids {
            let lower = df.lower_bounds.get(&id).cloned().unwrap_or("-".into());
            let upper = df.upper_bounds.get(&id).cloned().unwrap_or("-".into());
            let nulls = df
                .null_value_counts
                .get(&id)
                .map_or("-".to_string(), |n| n.to_string());
            lines.push(Line::from(vec![
                Span::styled(format!("  col {}: ", id), Theme::label()),
                Span::styled(
                    format!("[{} .. {}] nulls={}", lower, upper, nulls),
                    Theme::value(),
                ),
            ]));
        }

        lines
    }
}

impl Component for ManifestPanel {
    fn handle_key(&mut self, key: KeyEvent) -> Option<Action> {
        match key.code {
            KeyCode::Tab => {
                self.focus_left = !self.focus_left;
                None
            }
            KeyCode::Up | KeyCode::Char('k') => {
                let on_left = self.focus_left;
                let (state, _) = self.active_list();
                let i = state.selected().unwrap_or(0);
                if i > 0 {
                    state.select(Some(i - 1));
                    if on_left {
                        self.reset_data_file_cursor();
                    }
                }
                None
            }
            KeyCode::Down | KeyCode::Char('j') => {
                let on_left = self.focus_left;
                let (state, len) = self.active_list();
                let i = state.selected().unwrap_or(0);
                if i + 1 < len {
                    state.select(Some(i + 1));
                    if on_left {
                        self.reset_data_file_cursor();
                    }
                }
                None
            }
            _ => None,
        }
    }

    fn handle_message(&mut self, msg: &AppMessage) -> Option<Action> {
        match msg {
            AppMessage::ManifestsReady(manifests) => {
                self.manifests = manifests.clone();
                self.loaded = true;
                if !self.manifests.is_empty() {
                    self.manifest_list_state.select(Some(0));
                }
            }
            AppMessage::DataFileStatsReady(grouped) => {
                self.files_by_manifest = grouped.clone();
                self.reset_data_file_cursor();
            }
            _ => {}
        }
        None
    }

    fn render(&mut self, frame: &mut Frame, area: Rect, focused: bool) {
        let split = SplitLayout::new(area, LEFT_PANEL_PERCENT);

        let items: Vec<ListItem> = self
            .manifests
            .iter()
            .enumerate()
            .map(|(i, m)| {
                let filename = m.path.rsplit('/').next().unwrap();
                let tag = if m.content_type == "deletes" {
                    "[del]"
                } else {
                    "[data]"
                };
                let added_files = m.added_data_files_count.unwrap_or(0);
                let added_rows = m.added_rows_count.unwrap_or(0);
                let mut stats = format!("+{added_files} files, +{added_rows} rows");

                let exist_files = m.existing_data_files_count.unwrap_or(0);
                let exist_rows = m.existing_rows_count.unwrap_or(0);
                if exist_files > 0 || exist_rows > 0 {
                    stats.push_str(&format!(", ={exist_files} files, ={exist_rows} rows"));
                }

                let del_files = m.deleted_data_files_count.unwrap_or(0);
                let del_rows = m.deleted_rows_count.unwrap_or(0);
                if del_files > 0 || del_rows > 0 {
                    stats.push_str(&format!(", -{del_files} files, -{del_rows} rows"));
                }

                let line = Line::from(vec![
                    Span::styled(format!("{:>3}. ", i + 1), Theme::field_id()),
                    Span::styled(format!("{tag} "), Theme::label()),
                    Span::styled(filename, Theme::value()),
                    Span::raw(" "),
                    Span::styled(stats, Theme::field_type()),
                    Span::raw(" "),
                    Span::styled(
                        format!("seq={} spec={}", m.sequence_number, m.partition_spec_id),
                        Theme::field_id(),
                    ),
                ]);
                ListItem::new(line)
            })
            .collect();

        let left_block = Block::default()
            .borders(Borders::ALL)
            .title(format!(" Manifests ({}) ", self.manifests.len()))
            .border_style(if focused && self.focus_left {
                Theme::border_focused()
            } else {
                Theme::border_unfocused()
            });

        let list = List::new(items)
            .block(left_block)
            .highlight_style(Theme::table_row_selected());

        frame.render_stateful_widget(list, split.left, &mut self.manifest_list_state);

        let lines = self.build_right_panel_lines();

        let right_block = Block::default()
            .borders(Borders::ALL)
            .title(format!(" Data Files ({}) ", self.selected_files().len()))
            .border_style(if focused && !self.focus_left {
                Theme::border_focused()
            } else {
                Theme::border_unfocused()
            });

        let detail = Paragraph::new(lines)
            .block(right_block)
            .wrap(Wrap { trim: false });

        frame.render_widget(detail, split.right);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_size_works() {
        assert_eq!(ManifestPanel::format_size(500), "500 B");
        assert_eq!(ManifestPanel::format_size(1500), "1.5 KB");
        assert_eq!(ManifestPanel::format_size(1_500_000), "1.4 MB");
        assert_eq!(ManifestPanel::format_size(1_500_000_000), "1.4 GB");
    }

    #[test]
    fn manifest_panel_initial() {
        let panel = ManifestPanel::new();
        assert!(panel.needs_load());
        assert!(panel.manifests.is_empty());
    }

    fn make_manifest(
        path: &str,
        content_type: &str,
        added_files: Option<i32>,
        added_rows: Option<i64>,
        deleted_files: Option<i32>,
        deleted_rows: Option<i64>,
    ) -> ManifestInfo {
        ManifestInfo {
            path: path.into(),
            content_type: content_type.into(),
            added_data_files_count: added_files,
            added_rows_count: added_rows,
            existing_data_files_count: None,
            existing_rows_count: None,
            deleted_data_files_count: deleted_files,
            deleted_rows_count: deleted_rows,
            sequence_number: 1,
            partition_spec_id: 0,
        }
    }

    #[test]
    fn manifest_panel_handles_manifests_ready() {
        let mut panel = ManifestPanel::new();
        panel.handle_message(&AppMessage::ManifestsReady(vec![make_manifest(
            "/path/to/manifest.avro",
            "data",
            Some(3),
            Some(100),
            None,
            None,
        )]));
        assert!(!panel.needs_load());
        assert_eq!(panel.manifests.len(), 1);
        assert_eq!(panel.manifests[0].content_type, "data");
    }

    #[test]
    fn manifest_panel_handles_delete_manifest() {
        let mut panel = ManifestPanel::new();
        panel.handle_message(&AppMessage::ManifestsReady(vec![make_manifest(
            "/path/to/delete-manifest.avro",
            "deletes",
            Some(2),
            Some(50),
            Some(1),
            Some(25),
        )]));
        assert_eq!(panel.manifests[0].content_type, "deletes");
        assert_eq!(panel.manifests[0].deleted_data_files_count, Some(1));
        assert_eq!(panel.manifests[0].deleted_rows_count, Some(25));
    }

    #[test]
    fn manifest_panel_handles_multiple_manifests() {
        let mut panel = ManifestPanel::new();
        let data_manifest = make_manifest(
            "/path/to/data.avro",
            "data",
            Some(10),
            Some(5000),
            None,
            None,
        );
        let del_manifest = make_manifest(
            "/path/to/deletes.avro",
            "deletes",
            Some(2),
            Some(100),
            Some(3),
            Some(200),
        );
        panel.handle_message(&AppMessage::ManifestsReady(vec![
            data_manifest,
            del_manifest,
        ]));
        assert_eq!(panel.manifests.len(), 2);
        assert_eq!(panel.manifests[0].content_type, "data");
        assert_eq!(panel.manifests[1].content_type, "deletes");
    }

    #[test]
    fn manifest_panel_invalidate_resets_state() {
        let mut panel = ManifestPanel::new();
        panel.handle_message(&AppMessage::ManifestsReady(vec![make_manifest(
            "/path/to/manifest.avro",
            "data",
            Some(3),
            Some(100),
            None,
            None,
        )]));
        assert!(!panel.needs_load());
        assert_eq!(panel.manifests.len(), 1);

        panel.invalidate();
        assert!(panel.needs_load());
        assert!(panel.manifests.is_empty());
        assert!(panel.files_by_manifest.is_empty());
    }

    fn make_data_file(path: &str, records: i64, size: i64) -> DataFileInfo {
        DataFileInfo {
            file_path: path.into(),
            file_format: "Parquet".into(),
            record_count: records,
            file_size_bytes: size,
            null_value_counts: std::collections::HashMap::new(),
            lower_bounds: std::collections::HashMap::new(),
            upper_bounds: std::collections::HashMap::new(),
            partition_data: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn selected_files_follows_manifest_cursor() {
        let mut panel = ManifestPanel::new();
        panel.handle_message(&AppMessage::ManifestsReady(vec![
            make_manifest("/m1.avro", "data", Some(1), Some(10), None, None),
            make_manifest("/m2.avro", "data", Some(2), Some(20), None, None),
        ]));
        panel.handle_message(&AppMessage::DataFileStatsReady(vec![
            vec![make_data_file("/f1.parquet", 10, 1000)],
            vec![
                make_data_file("/f2.parquet", 15, 2000),
                make_data_file("/f3.parquet", 5, 500),
            ],
        ]));

        assert_eq!(panel.selected_files().len(), 1);
        assert_eq!(panel.selected_files()[0].file_path, "/f1.parquet");

        panel.manifest_list_state.select(Some(1));
        panel.reset_data_file_cursor();
        assert_eq!(panel.selected_files().len(), 2);
        assert_eq!(panel.selected_files()[0].file_path, "/f2.parquet");
    }

    #[test]
    fn manifest_info_with_none_counts() {
        let m = make_manifest("/path/to/m.avro", "data", None, None, None, None);
        assert!(m.added_data_files_count.is_none());
        assert!(m.added_rows_count.is_none());
        assert!(m.deleted_data_files_count.is_none());
        assert!(m.deleted_rows_count.is_none());
    }
}
