use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

use crate::event::{Action, AppMessage};
use crate::model::table_info::TableMetadata;
use crate::ui::theme::Theme;

use super::Component;

const PAGE_SCROLL_SIZE: u16 = 10;

pub struct PropertiesPanel {
    metadata: Option<TableMetadata>,
    selected_snapshot_id: Option<i64>,
    scroll: u16,
}

impl PropertiesPanel {
    pub fn new() -> Self {
        Self {
            metadata: None,
            selected_snapshot_id: None,
            scroll: 0,
        }
    }

    pub fn set_viewed_snapshot(&mut self, id: Option<i64>) {
        self.selected_snapshot_id = id;
        self.scroll = 0;
    }

    fn build_lines(&self) -> Vec<Line<'_>> {
        let Some(meta) = &self.metadata else {
            return vec![Line::styled("No metadata loaded", Theme::field_id())];
        };

        let mut lines = vec![
            Line::styled("═══ General Info ═══", Theme::title()),
            Line::from(vec![
                Span::styled("  Format Version: ", Theme::label()),
                Span::styled(meta.format_version.to_string(), Theme::value()),
            ]),
            Line::from(vec![
                Span::styled("  Table UUID: ", Theme::label()),
                Span::styled(&meta.table_uuid, Theme::value()),
            ]),
            Line::from(vec![
                Span::styled("  Location: ", Theme::label()),
                Span::styled(&meta.location, Theme::value()),
            ]),
            Line::from(vec![
                Span::styled("  Last Updated: ", Theme::label()),
                Span::styled(
                    chrono::DateTime::from_timestamp_millis(meta.last_updated_ms)
                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                        .unwrap_or_else(|| format!("{}ms", meta.last_updated_ms)),
                    Theme::value(),
                ),
            ]),
            Line::from(vec![
                Span::styled("  Current Snapshot: ", Theme::label()),
                Span::styled(
                    meta.current_snapshot_id
                        .map_or("-".into(), |id| id.to_string()),
                    Theme::value(),
                ),
            ]),
            Line::from(vec![
                Span::styled("  Schemas: ", Theme::label()),
                Span::styled(meta.schemas.len().to_string(), Theme::value()),
            ]),
            Line::from(vec![
                Span::styled("  Snapshots: ", Theme::label()),
                Span::styled(meta.snapshots.len().to_string(), Theme::value()),
            ]),
        ];

        if let Some(snap_id) = self.selected_snapshot_id {
            lines.push(Line::raw(""));
            lines.push(Line::styled(
                format!("═══ Snapshot {} ═══", snap_id),
                Theme::title(),
            ));

            let snap = meta.snapshots.iter().find(|s| s.snapshot_id == snap_id);
            if let Some(snap) = snap {
                lines.push(Line::from(vec![
                    Span::styled("  Operation: ", Theme::label()),
                    Span::styled(&snap.operation, Theme::value()),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("  Timestamp: ", Theme::label()),
                    Span::styled(
                        chrono::DateTime::from_timestamp_millis(snap.timestamp_ms)
                            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                            .unwrap_or_else(|| format!("{}ms", snap.timestamp_ms)),
                        Theme::value(),
                    ),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("  Sequence Number: ", Theme::label()),
                    Span::styled(snap.sequence_number.to_string(), Theme::value()),
                ]));
                if let Some(parent) = snap.parent_snapshot_id {
                    lines.push(Line::from(vec![
                        Span::styled("  Parent Snapshot: ", Theme::label()),
                        Span::styled(parent.to_string(), Theme::value()),
                    ]));
                }
                if let Some(schema_id) = snap.schema_id {
                    lines.push(Line::from(vec![
                        Span::styled("  Schema ID: ", Theme::label()),
                        Span::styled(schema_id.to_string(), Theme::value()),
                    ]));
                }

                if !snap.summary.is_empty() {
                    lines.push(Line::raw(""));
                    let mut entries: Vec<_> = snap.summary.iter().collect();
                    entries.sort_by_key(|(k, _)| *k);
                    for (key, val) in entries {
                        lines.push(Line::from(vec![
                            Span::styled(format!("  {}: ", key), Theme::label()),
                            Span::styled(val, Theme::value()),
                        ]));
                    }
                }
            } else {
                lines.push(Line::styled("  Snapshot not found", Theme::field_id()));
            }
        }

        lines.push(Line::raw(""));
        lines.push(Line::styled("═══ Partition Spec ═══", Theme::title()));
        if meta.partition_specs.is_empty() {
            lines.push(Line::styled("  Unpartitioned", Theme::field_id()));
        } else {
            for spec in &meta.partition_specs {
                lines.push(Line::from(vec![Span::styled(
                    format!("  Spec {}: ", spec.spec_id),
                    Theme::label(),
                )]));
                for field in &spec.fields {
                    lines.push(Line::from(vec![
                        Span::raw("    "),
                        Span::styled(&field.name, Theme::field_name()),
                        Span::raw(" = "),
                        Span::styled(&field.transform, Theme::field_type()),
                        Span::raw(format!("(source_id={})", field.source_id)),
                    ]));
                }
            }
        }

        lines.push(Line::raw(""));
        lines.push(Line::styled("═══ Sort Order ═══", Theme::title()));
        if meta.sort_orders.is_empty() || meta.sort_orders.iter().all(|o| o.fields.is_empty()) {
            lines.push(Line::styled("  Unsorted", Theme::field_id()));
        } else {
            for order in &meta.sort_orders {
                if order.fields.is_empty() {
                    continue;
                }
                lines.push(Line::from(vec![Span::styled(
                    format!("  Order {}: ", order.order_id),
                    Theme::label(),
                )]));
                for field in &order.fields {
                    lines.push(Line::from(vec![
                        Span::raw("    "),
                        Span::styled(
                            format!("source_id={}", field.source_id),
                            Theme::field_name(),
                        ),
                        Span::raw(" "),
                        Span::styled(&field.transform, Theme::field_type()),
                        Span::raw(format!(" {} {}", field.direction, field.null_order)),
                    ]));
                }
            }
        }

        lines.push(Line::raw(""));
        lines.push(Line::styled("═══ Table Properties ═══", Theme::title()));
        if meta.properties.is_empty() {
            lines.push(Line::styled("  No properties set", Theme::field_id()));
        } else {
            let mut entries: Vec<_> = meta.properties.iter().collect();
            entries.sort_by_key(|(k, _)| *k);
            for (key, val) in entries {
                lines.push(Line::from(vec![
                    Span::styled(format!("  {}: ", key), Theme::label()),
                    Span::styled(val, Theme::value()),
                ]));
            }
        }

        lines
    }
}

impl Component for PropertiesPanel {
    fn handle_key(&mut self, key: KeyEvent) -> Option<Action> {
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                self.scroll = self.scroll.saturating_sub(1);
                None
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.scroll += 1;
                None
            }
            KeyCode::PageUp => {
                self.scroll = self.scroll.saturating_sub(PAGE_SCROLL_SIZE);
                None
            }
            KeyCode::PageDown => {
                self.scroll += PAGE_SCROLL_SIZE;
                None
            }
            KeyCode::Char('g') => {
                self.scroll = 0;
                None
            }
            _ => None,
        }
    }

    fn handle_message(&mut self, msg: &AppMessage) -> Option<Action> {
        if let AppMessage::MetadataReady(metadata) = msg {
            self.metadata = Some(*metadata.clone());
            self.scroll = 0;
        }
        None
    }

    fn render(&mut self, frame: &mut Frame, area: Rect, focused: bool) {
        let lines = self.build_lines();

        let block = Block::default()
            .borders(Borders::ALL)
            .title(" Properties ")
            .border_style(if focused {
                Theme::border_focused()
            } else {
                Theme::border_unfocused()
            });

        let paragraph = Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: false })
            .scroll((self.scroll, 0));

        frame.render_widget(paragraph, area);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::table_info::SnapshotInfo;
    use std::collections::HashMap;

    fn sample_metadata() -> TableMetadata {
        TableMetadata {
            location: "/tmp/test".into(),
            current_schema: crate::model::table_info::SchemaInfo {
                schema_id: 0,
                fields: vec![],
            },
            schemas: vec![],
            snapshots: vec![
                SnapshotInfo {
                    snapshot_id: 100,
                    parent_snapshot_id: None,
                    sequence_number: 1,
                    timestamp_ms: 1700000000000,
                    operation: "append".into(),
                    summary: HashMap::from([
                        ("added-records".into(), "50".into()),
                        ("total-records".into(), "50".into()),
                    ]),
                    manifest_list: "/path/manifest-list.avro".into(),
                    schema_id: Some(0),
                },
                SnapshotInfo {
                    snapshot_id: 200,
                    parent_snapshot_id: Some(100),
                    sequence_number: 2,
                    timestamp_ms: 1700001000000,
                    operation: "overwrite".into(),
                    summary: HashMap::new(),
                    manifest_list: "/path/manifest-list-2.avro".into(),
                    schema_id: None,
                },
            ],
            partition_specs: vec![],
            sort_orders: vec![],
            properties: HashMap::new(),
            current_snapshot_id: Some(200),
            format_version: 2,
            table_uuid: "test-uuid".into(),
            last_updated_ms: 1700001000000,
        }
    }

    #[test]
    fn properties_panel_initial() {
        let panel = PropertiesPanel::new();
        assert!(panel.metadata.is_none());
        assert!(panel.selected_snapshot_id.is_none());
        assert_eq!(panel.scroll, 0);
    }

    #[test]
    fn scroll_navigation() {
        let mut panel = PropertiesPanel::new();
        use crossterm::event::KeyModifiers;
        let down = KeyEvent::new(KeyCode::Down, KeyModifiers::NONE);
        let up = KeyEvent::new(KeyCode::Up, KeyModifiers::NONE);

        panel.handle_key(down);
        assert_eq!(panel.scroll, 1);
        panel.handle_key(down);
        assert_eq!(panel.scroll, 2);
        panel.handle_key(up);
        assert_eq!(panel.scroll, 1);
    }

    #[test]
    fn set_viewed_snapshot_updates_state() {
        let mut panel = PropertiesPanel::new();
        panel.set_viewed_snapshot(Some(42));
        assert_eq!(panel.selected_snapshot_id, Some(42));
        assert_eq!(panel.scroll, 0);

        panel.scroll = 5;
        panel.set_viewed_snapshot(None);
        assert!(panel.selected_snapshot_id.is_none());
        assert_eq!(panel.scroll, 0);
    }

    #[test]
    fn build_lines_no_snapshot_section_by_default() {
        let mut panel = PropertiesPanel::new();
        panel.metadata = Some(sample_metadata());

        let lines = panel.build_lines();
        let text: String = lines
            .iter()
            .map(|l| l.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(!text.contains("Snapshot 100"));
        assert!(!text.contains("Snapshot 200"));
    }

    #[test]
    fn build_lines_shows_snapshot_section_when_selected() {
        let mut panel = PropertiesPanel::new();
        panel.metadata = Some(sample_metadata());
        panel.set_viewed_snapshot(Some(100));

        let lines = panel.build_lines();
        let text: String = lines
            .iter()
            .map(|l| l.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("Snapshot 100"));
        assert!(text.contains("append"));
        assert!(text.contains("added-records"));
        assert!(text.contains("50"));
    }

    #[test]
    fn build_lines_snapshot_with_parent_and_schema() {
        let mut panel = PropertiesPanel::new();
        panel.metadata = Some(sample_metadata());
        panel.set_viewed_snapshot(Some(200));

        let lines = panel.build_lines();
        let text: String = lines
            .iter()
            .map(|l| l.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("Snapshot 200"));
        assert!(text.contains("overwrite"));
        assert!(text.contains("Parent Snapshot"));
        assert!(text.contains("100"));
    }

    #[test]
    fn build_lines_snapshot_not_found() {
        let mut panel = PropertiesPanel::new();
        panel.metadata = Some(sample_metadata());
        panel.set_viewed_snapshot(Some(999));

        let lines = panel.build_lines();
        let text: String = lines
            .iter()
            .map(|l| l.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("Snapshot 999"));
        assert!(text.contains("Snapshot not found"));
    }
}
