use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::Rect;
use ratatui::text::Text;
use ratatui::widgets::{Block, Borders, Cell, Row, Table, TableState};
use ratatui::Frame;

use crate::event::{Action, AppMessage};
use crate::loader::arrow_convert;
use crate::ui::theme::Theme;

use super::Component;
use arrow_array::RecordBatch;

const DEFAULT_MAX_VISIBLE_COLS: usize = 20;
const PAGE_SCROLL_SIZE: usize = 20;
const WIDTH_SAMPLE_ROWS: usize = 100;
const MIN_COLUMN_WIDTH: usize = 4;
const MAX_COLUMN_WIDTH: usize = 40;
const ROW_NUMBER_WIDTH: u16 = 5;
const COLUMN_PADDING: u16 = 2;

pub struct DataView {
    batches: Vec<RecordBatch>,
    all_columns: Vec<String>,
    visible_columns: Vec<String>,
    display_rows: Vec<Vec<String>>,
    display_columns: Vec<String>,
    table_state: TableState,
    pub total_rows: usize,
    h_scroll: usize,
    max_visible_cols: usize,
    has_more: bool,
}

impl DataView {
    pub fn new() -> Self {
        Self {
            batches: vec![],
            all_columns: vec![],
            visible_columns: vec![],
            display_rows: vec![],
            display_columns: vec![],
            table_state: TableState::default(),
            total_rows: 0,
            h_scroll: 0,
            max_visible_cols: DEFAULT_MAX_VISIBLE_COLS,
            has_more: false,
        }
    }

    pub fn all_columns(&self) -> &[String] {
        &self.all_columns
    }

    pub fn visible_columns(&self) -> &[String] {
        &self.visible_columns
    }

    pub fn set_visible_columns(&mut self, columns: Vec<String>) {
        self.visible_columns = columns;
        self.refresh_display();
    }

    fn refresh_display(&mut self) {
        let cols = if self.visible_columns.is_empty() {
            &self.all_columns
        } else {
            &self.visible_columns
        };

        let Ok((display_cols, rows)) =
            arrow_convert::batches_to_string_rows(&self.batches, 0, self.total_rows.max(1))
        else {
            return;
        };

        if self.visible_columns.is_empty() {
            self.display_columns = display_cols;
            self.display_rows = rows;
            return;
        }

        let col_indices: Vec<usize> = cols
            .iter()
            .filter_map(|c| display_cols.iter().position(|dc| dc == c))
            .collect();

        self.display_columns = col_indices
            .iter()
            .map(|&i| display_cols[i].clone())
            .collect();
        self.display_rows = rows
            .into_iter()
            .map(|row| col_indices.iter().map(|&i| row[i].clone()).collect())
            .collect();
    }

    fn move_up(&mut self) {
        let i = self.table_state.selected().unwrap_or(0);
        if i > 0 {
            self.table_state.select(Some(i - 1));
        }
    }

    fn move_down(&mut self) {
        let i = self.table_state.selected().unwrap_or(0);
        if i + 1 < self.display_rows.len() {
            self.table_state.select(Some(i + 1));
        }
    }

    fn page_up(&mut self) {
        let i = self.table_state.selected().unwrap_or(0);
        self.table_state
            .select(Some(i.saturating_sub(PAGE_SCROLL_SIZE)));
    }

    fn page_down(&mut self) {
        let i = self.table_state.selected().unwrap_or(0);
        let max = self.display_rows.len().saturating_sub(1);
        self.table_state
            .select(Some((i + PAGE_SCROLL_SIZE).min(max)));
    }

    fn scroll_left(&mut self) {
        if self.h_scroll > 0 {
            self.h_scroll -= 1;
        }
    }

    fn scroll_right(&mut self) {
        let total = self.display_columns.len();
        if self.h_scroll + self.max_visible_cols < total {
            self.h_scroll += 1;
        }
    }

    fn jump_top(&mut self) {
        self.table_state.select(Some(0));
    }

    fn jump_bottom(&mut self) {
        if !self.display_rows.is_empty() {
            self.table_state.select(Some(self.display_rows.len() - 1));
        }
    }
}

impl Component for DataView {
    fn handle_key(&mut self, key: KeyEvent) -> Option<Action> {
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                self.move_up();
                None
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.move_down();
                None
            }
            KeyCode::Left | KeyCode::Char('h') => {
                self.scroll_left();
                None
            }
            KeyCode::Right | KeyCode::Char('l') => {
                self.scroll_right();
                None
            }
            KeyCode::PageUp => {
                self.page_up();
                None
            }
            KeyCode::PageDown => {
                self.page_down();
                None
            }
            KeyCode::Char('g') => {
                self.jump_top();
                None
            }
            KeyCode::Char('G') => {
                self.jump_bottom();
                None
            }
            KeyCode::Char('/') => Some(Action::FocusFilter),
            KeyCode::Char('c') => Some(Action::ToggleColumnSelector),
            _ => None,
        }
    }

    fn handle_message(&mut self, msg: &AppMessage) -> Option<Action> {
        match msg {
            AppMessage::DataReady {
                batches,
                total_rows,
                has_more,
            } => {
                self.batches = batches.clone();
                self.total_rows = *total_rows;
                self.has_more = *has_more;
                let new_cols = arrow_convert::column_names(&self.batches);
                let schema_changed = self.all_columns != new_cols;
                self.all_columns = new_cols;
                if schema_changed || self.visible_columns.is_empty() {
                    self.visible_columns = self.all_columns.clone();
                }
                self.refresh_display();
                if !self.display_rows.is_empty() {
                    self.table_state.select(Some(0));
                }
                None
            }
            _ => None,
        }
    }

    fn render(&mut self, frame: &mut Frame, area: Rect, focused: bool) {
        if self.display_rows.is_empty() {
            let block = Block::default()
                .borders(Borders::ALL)
                .title(" Data ")
                .border_style(if focused {
                    Theme::border_focused()
                } else {
                    Theme::border_unfocused()
                });
            let empty = ratatui::widgets::Paragraph::new("No data loaded. Press 'r' to reload.")
                .block(block);
            frame.render_widget(empty, area);
            return;
        }

        let total_cols = self.display_columns.len();
        let end_col = (self.h_scroll + self.max_visible_cols).min(total_cols);
        let visible_col_range = self.h_scroll..end_col;

        let col_widths: Vec<u16> = visible_col_range
            .clone()
            .map(|col_idx| {
                let header_width = self.display_columns[col_idx].len();
                let max_data_width = self
                    .display_rows
                    .iter()
                    .take(WIDTH_SAMPLE_ROWS)
                    .map(|row| row.get(col_idx).map_or(0, |cell| cell.len()))
                    .max()
                    .unwrap_or(0);
                let width = header_width
                    .max(max_data_width)
                    .clamp(MIN_COLUMN_WIDTH, MAX_COLUMN_WIDTH);
                width as u16
            })
            .collect();

        let mut header_cells = vec![Cell::from("  #").style(Theme::table_header())];
        for col_idx in visible_col_range.clone() {
            header_cells.push(
                Cell::from(Text::from(self.display_columns[col_idx].clone()))
                    .style(Theme::table_header()),
            );
        }
        let header = Row::new(header_cells).height(1);

        let rows: Vec<Row> = self
            .display_rows
            .iter()
            .enumerate()
            .map(|(i, row)| {
                let style = if i % 2 == 0 {
                    Theme::table_row_normal()
                } else {
                    Theme::table_row_alt()
                };

                let mut cells = vec![Cell::from(format!("{:>4}", i + 1)).style(style)];
                for col_idx in visible_col_range.clone() {
                    let text = row.get(col_idx).cloned().unwrap_or_default();
                    cells.push(Cell::from(text).style(style));
                }
                Row::new(cells).height(1)
            })
            .collect();

        let mut widths = vec![ratatui::layout::Constraint::Length(ROW_NUMBER_WIDTH)];

        for w in &col_widths {
            widths.push(ratatui::layout::Constraint::Length(*w + COLUMN_PADDING));
        }

        let row_label = if self.has_more {
            format!(" Data ({} rows loaded) ", self.total_rows)
        } else {
            format!(" Data ({} rows) ", self.total_rows)
        };
        let block = Block::default()
            .borders(Borders::ALL)
            .title(row_label)
            .border_style(if focused {
                Theme::border_focused()
            } else {
                Theme::border_unfocused()
            });

        let table = Table::new(rows, &widths)
            .header(header)
            .block(block)
            .row_highlight_style(Theme::table_row_selected());

        frame.render_stateful_widget(table, area, &mut self.table_state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_test_batches() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        vec![RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap()]
    }

    #[test]
    fn data_view_initial_state() {
        let dv = DataView::new();
        assert!(dv.display_rows.is_empty());
        assert_eq!(dv.total_rows, 0);
    }

    #[test]
    fn data_view_handles_data_ready() {
        let mut dv = DataView::new();
        let batches = make_test_batches();
        dv.handle_message(&AppMessage::DataReady {
            batches: batches.clone(),
            total_rows: 3,
            has_more: false,
        });
        assert_eq!(dv.total_rows, 3);
        assert_eq!(dv.display_rows.len(), 3);
        assert_eq!(dv.all_columns, vec!["id", "name"]);
    }

    #[test]
    fn data_view_navigation() {
        let mut dv = DataView::new();
        let batches = make_test_batches();
        dv.handle_message(&AppMessage::DataReady {
            batches,
            total_rows: 3,
            has_more: false,
        });

        // Should start at row 0
        assert_eq!(dv.table_state.selected(), Some(0));

        // Move down
        dv.move_down();
        assert_eq!(dv.table_state.selected(), Some(1));

        // Move up
        dv.move_up();
        assert_eq!(dv.table_state.selected(), Some(0));

        // Can't go above 0
        dv.move_up();
        assert_eq!(dv.table_state.selected(), Some(0));

        // Jump to bottom
        dv.jump_bottom();
        assert_eq!(dv.table_state.selected(), Some(2));

        // Jump to top
        dv.jump_top();
        assert_eq!(dv.table_state.selected(), Some(0));
    }

    #[test]
    fn data_view_column_filtering() {
        let mut dv = DataView::new();
        let batches = make_test_batches();
        dv.handle_message(&AppMessage::DataReady {
            batches,
            total_rows: 3,
            has_more: false,
        });

        // Set only one visible column
        dv.set_visible_columns(vec!["name".to_string()]);
        assert_eq!(dv.display_columns, vec!["name"]);
        assert_eq!(dv.display_rows[0], vec!["Alice"]);
    }

    #[test]
    fn data_view_resets_visible_columns_on_schema_change() {
        let mut dv = DataView::new();
        dv.handle_message(&AppMessage::DataReady {
            batches: make_test_batches(),
            total_rows: 3,
            has_more: false,
        });

        dv.set_visible_columns(vec!["name".to_string()]);
        assert_eq!(dv.visible_columns, vec!["name"]);

        let new_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("email", DataType::Utf8, false),
        ]));
        let new_batches = vec![RecordBatch::try_new(
            new_schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a@b.com"])),
            ],
        )
        .unwrap()];

        dv.handle_message(&AppMessage::DataReady {
            batches: new_batches,
            total_rows: 1,
            has_more: false,
        });

        assert_eq!(dv.visible_columns, vec!["id", "email"]);
    }

    #[test]
    fn data_view_keeps_visible_columns_when_schema_unchanged() {
        let mut dv = DataView::new();
        dv.handle_message(&AppMessage::DataReady {
            batches: make_test_batches(),
            total_rows: 3,
            has_more: false,
        });

        dv.set_visible_columns(vec!["name".to_string()]);

        dv.handle_message(&AppMessage::DataReady {
            batches: make_test_batches(),
            total_rows: 3,
            has_more: false,
        });

        assert_eq!(dv.visible_columns, vec!["name"]);
    }
}
