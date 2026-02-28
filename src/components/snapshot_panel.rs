use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Frame;

use crate::event::{Action, AppMessage};
use crate::model::table_info::SnapshotInfo;
use crate::ui::layout::SplitLayout;
use crate::ui::theme::Theme;

use super::Component;

const LEFT_PANEL_PERCENT: u16 = 45;

pub struct SnapshotPanel {
    snapshots: Vec<SnapshotInfo>,
    current_snapshot_id: Option<i64>,
    viewed_snapshot_id: Option<i64>,
    list_state: ListState,
}

impl SnapshotPanel {
    pub fn new() -> Self {
        Self {
            snapshots: vec![],
            current_snapshot_id: None,
            viewed_snapshot_id: None,
            list_state: ListState::default(),
        }
    }

    pub fn set_viewed_snapshot(&mut self, id: Option<i64>) {
        self.viewed_snapshot_id = id;
    }

    pub fn schema_id_for_snapshot(&self, snapshot_id: i64) -> Option<i32> {
        self.snapshots
            .iter()
            .find(|s| s.snapshot_id == snapshot_id)
            .and_then(|s| s.schema_id)
    }

    pub fn selected_snapshot(&self) -> Option<&SnapshotInfo> {
        self.list_state
            .selected()
            .and_then(|i| self.snapshots.get(i))
    }

    pub fn format_timestamp(ms: i64) -> String {
        chrono::DateTime::from_timestamp_millis(ms)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| format!("{}ms", ms))
    }
}

impl Component for SnapshotPanel {
    fn handle_key(&mut self, key: KeyEvent) -> Option<Action> {
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                let i = self.list_state.selected().unwrap_or(0);
                if i > 0 {
                    self.list_state.select(Some(i - 1));
                }
                None
            }
            KeyCode::Down | KeyCode::Char('j') => {
                let i = self.list_state.selected().unwrap_or(0);
                if i + 1 < self.snapshots.len() {
                    self.list_state.select(Some(i + 1));
                }
                None
            }
            KeyCode::Enter => self
                .selected_snapshot()
                .map(|snap| Action::SelectSnapshot(snap.snapshot_id)),
            _ => None,
        }
    }

    fn handle_message(&mut self, msg: &AppMessage) -> Option<Action> {
        if let AppMessage::MetadataReady(metadata) = msg {
            self.snapshots = metadata.snapshots.clone();
            self.snapshots.sort_by(|a, b| {
                b.timestamp_ms
                    .cmp(&a.timestamp_ms)
                    .then(b.sequence_number.cmp(&a.sequence_number))
            });
            self.current_snapshot_id = metadata.current_snapshot_id;
            if !self.snapshots.is_empty() {
                self.list_state.select(Some(0));
            }
        }
        None
    }

    fn render(&mut self, frame: &mut Frame, area: Rect, focused: bool) {
        let split = SplitLayout::new(area, LEFT_PANEL_PERCENT);

        let items: Vec<ListItem> = self
            .snapshots
            .iter()
            .map(|snap| {
                let is_current = self.current_snapshot_id == Some(snap.snapshot_id);
                let is_viewed = self.viewed_snapshot_id == Some(snap.snapshot_id);
                let marker = match (is_viewed, is_current) {
                    (true, _) => "◆",
                    (false, true) => "▸",
                    _ => " ",
                };
                let ts = Self::format_timestamp(snap.timestamp_ms);

                let added = snap
                    .summary
                    .get("added-records")
                    .or_else(|| snap.summary.get("added-data-files"))
                    .cloned()
                    .unwrap_or_default();

                let line = Line::from(vec![
                    Span::raw(format!("{} ", marker)),
                    Span::styled(snap.operation.clone(), Theme::label()),
                    Span::raw("  "),
                    Span::styled(ts, Theme::value()),
                    if !added.is_empty() {
                        Span::styled(format!(" (+{})", added), Theme::field_type())
                    } else {
                        Span::raw("")
                    },
                ]);
                ListItem::new(line)
            })
            .collect();

        let left_block = Block::default()
            .borders(Borders::ALL)
            .title(format!(" Snapshots ({}) ", self.snapshots.len()))
            .border_style(if focused {
                Theme::border_focused()
            } else {
                Theme::border_unfocused()
            });

        let list = List::new(items)
            .block(left_block)
            .highlight_style(Theme::table_row_selected()); // ratatui still accepts this

        frame.render_stateful_widget(list, split.left, &mut self.list_state);

        let mut lines: Vec<Line> = Vec::new();

        if let Some(snap) = self.selected_snapshot().cloned() {
            lines.push(Line::from(vec![
                Span::styled("Snapshot ID: ", Theme::label()),
                Span::styled(snap.snapshot_id.to_string(), Theme::value()),
            ]));
            lines.push(Line::from(vec![
                Span::styled("Parent: ", Theme::label()),
                Span::styled(
                    snap.parent_snapshot_id
                        .map_or("-".into(), |p| p.to_string()),
                    Theme::value(),
                ),
            ]));
            lines.push(Line::from(vec![
                Span::styled("Timestamp: ", Theme::label()),
                Span::styled(Self::format_timestamp(snap.timestamp_ms), Theme::value()),
            ]));
            lines.push(Line::from(vec![
                Span::styled("Operation: ", Theme::label()),
                Span::styled(snap.operation.clone(), Theme::value()),
            ]));
            if let Some(schema_id) = snap.schema_id {
                lines.push(Line::from(vec![
                    Span::styled("Schema ID: ", Theme::label()),
                    Span::styled(schema_id.to_string(), Theme::value()),
                ]));
            }
            lines.push(Line::from(vec![
                Span::styled("Manifest List: ", Theme::label()),
                Span::styled(snap.manifest_list.clone(), Theme::value()),
            ]));

            lines.push(Line::raw(""));
            lines.push(Line::styled("─── Summary ───", Theme::title()));

            let mut summary_entries: Vec<(String, String)> = snap
                .summary
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            summary_entries.sort_by(|a, b| a.0.cmp(&b.0));
            for (key, val) in &summary_entries {
                lines.push(Line::from(vec![
                    Span::styled(format!("  {}: ", key), Theme::label()),
                    Span::styled(val.clone(), Theme::value()),
                ]));
            }

            lines.push(Line::raw(""));
            lines.push(Line::styled(
                "Press Enter to time-travel to this snapshot",
                Theme::status_key_hint(),
            ));
        } else {
            lines.push(Line::styled("No snapshot selected", Theme::field_id()));
        }

        let right_block = Block::default()
            .borders(Borders::ALL)
            .title(" Snapshot Detail ")
            .border_style(Theme::border_unfocused());

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
    fn format_timestamp_valid() {
        let ts = SnapshotPanel::format_timestamp(1700000000000);
        assert!(ts.contains("2023"));
    }

    #[test]
    fn snapshot_panel_initial() {
        let panel = SnapshotPanel::new();
        assert!(panel.snapshots.is_empty());
        assert!(panel.current_snapshot_id.is_none());
        assert!(panel.viewed_snapshot_id.is_none());
    }

    #[test]
    fn set_viewed_snapshot_updates_state() {
        let mut panel = SnapshotPanel::new();
        panel.set_viewed_snapshot(Some(42));
        assert_eq!(panel.viewed_snapshot_id, Some(42));

        panel.set_viewed_snapshot(None);
        assert!(panel.viewed_snapshot_id.is_none());
    }

    #[test]
    fn schema_id_for_snapshot_found() {
        let mut panel = SnapshotPanel::new();
        panel.snapshots = vec![SnapshotInfo {
            snapshot_id: 100,
            parent_snapshot_id: None,
            sequence_number: 1,
            timestamp_ms: 0,
            operation: "append".into(),
            summary: std::collections::HashMap::new(),
            manifest_list: String::new(),
            schema_id: Some(2),
        }];
        assert_eq!(panel.schema_id_for_snapshot(100), Some(2));
    }

    #[test]
    fn schema_id_for_snapshot_none_when_missing() {
        let panel = SnapshotPanel::new();
        assert_eq!(panel.schema_id_for_snapshot(999), None);
    }

    #[test]
    fn schema_id_for_snapshot_none_when_no_schema() {
        let mut panel = SnapshotPanel::new();
        panel.snapshots = vec![SnapshotInfo {
            snapshot_id: 100,
            parent_snapshot_id: None,
            sequence_number: 1,
            timestamp_ms: 0,
            operation: "append".into(),
            summary: std::collections::HashMap::new(),
            manifest_list: String::new(),
            schema_id: None,
        }];
        assert_eq!(panel.schema_id_for_snapshot(100), None);
    }
}
