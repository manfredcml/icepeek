use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Frame;

use crate::event::{Action, AppMessage};
use crate::model::table_info::{FieldInfo, SchemaInfo};
use crate::ui::layout::SplitLayout;
use crate::ui::theme::Theme;

use super::Component;

const LEFT_PANEL_PERCENT: u16 = 50;

/// Flattened field entry for display.
struct FlatField {
    depth: usize,
    field: FieldInfo,
    has_children: bool,
}

pub struct SchemaPanel {
    schemas: Vec<SchemaInfo>,
    current_schema_id: i32,
    head_schema_id: i32,
    /// Flattened field list for the current schema.
    flat_fields: Vec<FlatField>,
    list_state: ListState,
    /// Which schema index in the history list is selected.
    schema_list_state: ListState,
    /// Focus: left (field tree) or right (detail).
    focus_left: bool,
}

impl SchemaPanel {
    pub fn new() -> Self {
        Self {
            schemas: vec![],
            current_schema_id: 0,
            head_schema_id: 0,
            flat_fields: vec![],
            list_state: ListState::default(),
            schema_list_state: ListState::default(),
            focus_left: true,
        }
    }

    pub fn set_viewed_schema(&mut self, schema_id: Option<i32>) {
        let id = schema_id.unwrap_or(self.head_schema_id);
        if id == self.current_schema_id {
            return;
        }
        self.current_schema_id = id;
        self.rebuild_flat_fields();
    }

    fn flatten_fields(fields: &[FieldInfo], depth: usize) -> Vec<FlatField> {
        let mut result = Vec::new();
        for field in fields {
            let has_children = !field.children.is_empty();
            result.push(FlatField {
                depth,
                field: field.clone(),
                has_children,
            });
            if has_children {
                result.extend(Self::flatten_fields(&field.children, depth + 1));
            }
        }
        result
    }

    fn rebuild_flat_fields(&mut self) {
        let Some(schema) = self
            .schemas
            .iter()
            .find(|s| s.schema_id == self.current_schema_id)
            .or_else(|| self.schemas.first())
        else {
            return;
        };
        self.flat_fields = Self::flatten_fields(&schema.fields, 0);
        if !self.flat_fields.is_empty() {
            self.list_state.select(Some(0));
        }
    }

    fn selected_field(&self) -> Option<&FieldInfo> {
        self.list_state
            .selected()
            .and_then(|i| self.flat_fields.get(i))
            .map(|ff| &ff.field)
    }
}

impl Component for SchemaPanel {
    fn handle_key(&mut self, key: KeyEvent) -> Option<Action> {
        match key.code {
            KeyCode::Tab => {
                self.focus_left = !self.focus_left;
                None
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if self.focus_left {
                    let i = self.list_state.selected().unwrap_or(0);
                    if i > 0 {
                        self.list_state.select(Some(i - 1));
                    }
                } else {
                    let i = self.schema_list_state.selected().unwrap_or(0);
                    if i > 0 {
                        self.schema_list_state.select(Some(i - 1));
                    }
                }
                None
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if self.focus_left {
                    let i = self.list_state.selected().unwrap_or(0);
                    if i + 1 < self.flat_fields.len() {
                        self.list_state.select(Some(i + 1));
                    }
                } else {
                    let i = self.schema_list_state.selected().unwrap_or(0);
                    if i + 1 < self.schemas.len() {
                        self.schema_list_state.select(Some(i + 1));
                    }
                }
                None
            }
            KeyCode::Enter => {
                if self.focus_left {
                    return None;
                }
                let idx = self.schema_list_state.selected()?;
                let schema = self.schemas.get(idx)?;
                self.current_schema_id = schema.schema_id;
                self.rebuild_flat_fields();
                None
            }
            _ => None,
        }
    }

    fn handle_message(&mut self, msg: &AppMessage) -> Option<Action> {
        if let AppMessage::MetadataReady(metadata) = msg {
            self.schemas = metadata.schemas.clone();
            self.head_schema_id = metadata.current_schema.schema_id;
            self.current_schema_id = self.head_schema_id;
            self.rebuild_flat_fields();
            if !self.schemas.is_empty() {
                self.schema_list_state.select(Some(0));
            }
        }
        None
    }

    fn render(&mut self, frame: &mut Frame, area: Rect, focused: bool) {
        let split = SplitLayout::new(area, LEFT_PANEL_PERCENT);

        let items: Vec<ListItem> = self
            .flat_fields
            .iter()
            .map(|ff| {
                let indent = "  ".repeat(ff.depth);
                let prefix = if ff.has_children { "▼ " } else { "  " };
                let req_marker = if ff.field.required { "" } else { "?" };

                let line = Line::from(vec![
                    Span::raw(indent),
                    Span::raw(prefix),
                    Span::styled(&ff.field.name, Theme::field_name()),
                    Span::styled(req_marker, Theme::field_id()),
                    Span::raw(": "),
                    Span::styled(&ff.field.field_type, Theme::field_type()),
                ]);
                ListItem::new(line)
            })
            .collect();

        let left_block = Block::default()
            .borders(Borders::ALL)
            .title(format!(" Schema (id={}) ", self.current_schema_id))
            .border_style(if focused && self.focus_left {
                Theme::border_focused()
            } else {
                Theme::border_unfocused()
            });

        let list = List::new(items)
            .block(left_block)
            .highlight_style(Theme::table_row_selected());

        frame.render_stateful_widget(list, split.left, &mut self.list_state);

        let mut detail_lines: Vec<Line> = Vec::new();

        if let Some(field) = self.selected_field().cloned() {
            detail_lines.push(Line::from(vec![
                Span::styled("Field: ", Theme::label()),
                Span::styled(field.name.clone(), Theme::field_name()),
            ]));
            detail_lines.push(Line::from(vec![
                Span::styled("ID: ", Theme::label()),
                Span::styled(field.id.to_string(), Theme::value()),
            ]));
            detail_lines.push(Line::from(vec![
                Span::styled("Type: ", Theme::label()),
                Span::styled(field.field_type.clone(), Theme::field_type()),
            ]));
            detail_lines.push(Line::from(vec![
                Span::styled("Required: ", Theme::label()),
                Span::styled(field.required.to_string(), Theme::value()),
            ]));
            if let Some(ref doc) = field.doc {
                detail_lines.push(Line::from(vec![
                    Span::styled("Doc: ", Theme::label()),
                    Span::styled(doc.clone(), Theme::value()),
                ]));
            }
        }

        detail_lines.push(Line::raw(""));
        detail_lines.push(Line::styled("─── Schema History ───", Theme::title()));

        for schema in &self.schemas {
            let marker = if schema.schema_id == self.current_schema_id {
                "▸ "
            } else {
                "  "
            };
            detail_lines.push(Line::from(vec![
                Span::raw(marker),
                Span::styled(
                    format!(
                        "Schema {} ({} fields)",
                        schema.schema_id,
                        schema.fields.len()
                    ),
                    if schema.schema_id == self.current_schema_id {
                        Theme::label()
                    } else {
                        Theme::value()
                    },
                ),
            ]));
        }

        let right_block = Block::default()
            .borders(Borders::ALL)
            .title(" Details ")
            .border_style(if focused && !self.focus_left {
                Theme::border_focused()
            } else {
                Theme::border_unfocused()
            });

        let detail = Paragraph::new(detail_lines)
            .block(right_block)
            .wrap(Wrap { trim: false });

        frame.render_widget(detail, split.right);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::table_info::*;
    use std::collections::HashMap;

    fn make_metadata() -> Box<TableMetadata> {
        Box::new(TableMetadata {
            location: "/test".into(),
            current_schema: SchemaInfo {
                schema_id: 0,
                fields: vec![
                    FieldInfo {
                        id: 1,
                        name: "id".into(),
                        field_type: "int".into(),
                        required: true,
                        doc: None,
                        children: vec![],
                    },
                    FieldInfo {
                        id: 2,
                        name: "data".into(),
                        field_type: "struct".into(),
                        required: false,
                        doc: Some("nested".into()),
                        children: vec![FieldInfo {
                            id: 3,
                            name: "value".into(),
                            field_type: "string".into(),
                            required: true,
                            doc: None,
                            children: vec![],
                        }],
                    },
                ],
            },
            schemas: vec![
                SchemaInfo {
                    schema_id: 0,
                    fields: vec![
                        FieldInfo {
                            id: 1,
                            name: "id".into(),
                            field_type: "int".into(),
                            required: true,
                            doc: None,
                            children: vec![],
                        },
                        FieldInfo {
                            id: 2,
                            name: "data".into(),
                            field_type: "struct".into(),
                            required: false,
                            doc: Some("nested".into()),
                            children: vec![FieldInfo {
                                id: 3,
                                name: "value".into(),
                                field_type: "string".into(),
                                required: true,
                                doc: None,
                                children: vec![],
                            }],
                        },
                    ],
                },
                SchemaInfo {
                    schema_id: 1,
                    fields: vec![FieldInfo {
                        id: 1,
                        name: "id".into(),
                        field_type: "long".into(),
                        required: true,
                        doc: None,
                        children: vec![],
                    }],
                },
            ],
            snapshots: vec![],
            partition_specs: vec![],
            sort_orders: vec![],
            properties: HashMap::new(),
            current_snapshot_id: None,
            format_version: 2,
            table_uuid: "test-uuid".into(),
            last_updated_ms: 0,
        })
    }

    #[test]
    fn schema_panel_metadata_ready() {
        let mut panel = SchemaPanel::new();
        panel.handle_message(&AppMessage::MetadataReady(make_metadata()));
        assert_eq!(panel.schemas.len(), 2);
        assert_eq!(panel.flat_fields.len(), 3);
        assert_eq!(panel.head_schema_id, 0);
        assert_eq!(panel.current_schema_id, 0);
    }

    #[test]
    fn schema_panel_navigation() {
        let mut panel = SchemaPanel::new();
        panel.handle_message(&AppMessage::MetadataReady(make_metadata()));

        assert_eq!(panel.list_state.selected(), Some(0));

        use crossterm::event::KeyModifiers;
        let down = KeyEvent::new(KeyCode::Down, KeyModifiers::NONE);
        panel.handle_key(down);
        assert_eq!(panel.list_state.selected(), Some(1));
    }

    #[test]
    fn set_viewed_schema_switches_to_snapshot_schema() {
        let mut panel = SchemaPanel::new();
        panel.handle_message(&AppMessage::MetadataReady(make_metadata()));
        assert_eq!(panel.current_schema_id, 0);
        assert_eq!(panel.flat_fields.len(), 3);

        panel.set_viewed_schema(Some(1));
        assert_eq!(panel.current_schema_id, 1);
        assert_eq!(panel.flat_fields.len(), 1);
    }

    #[test]
    fn set_viewed_schema_none_restores_head() {
        let mut panel = SchemaPanel::new();
        panel.handle_message(&AppMessage::MetadataReady(make_metadata()));

        panel.set_viewed_schema(Some(1));
        assert_eq!(panel.current_schema_id, 1);

        panel.set_viewed_schema(None);
        assert_eq!(panel.current_schema_id, 0);
        assert_eq!(panel.flat_fields.len(), 3);
    }

    #[test]
    fn set_viewed_schema_same_id_is_noop() {
        let mut panel = SchemaPanel::new();
        panel.handle_message(&AppMessage::MetadataReady(make_metadata()));

        panel.set_viewed_schema(Some(0));
        assert_eq!(panel.current_schema_id, 0);
        assert_eq!(panel.flat_fields.len(), 3);
    }
}
