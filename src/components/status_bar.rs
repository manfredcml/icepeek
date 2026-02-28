use crossterm::event::KeyEvent;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::Frame;

use crate::event::{Action, AppMessage};
use crate::ui::theme::Theme;

use super::Component;

const ERROR_DISPLAY_MAX_LEN: usize = 40;
const ERROR_TRUNCATED_LEN: usize = ERROR_DISPLAY_MAX_LEN - 3; // room for "..."

pub struct StatusBar {
    pub loaded_rows: usize,
    pub table_total_rows: Option<usize>,
    pub filtered_rows: Option<usize>,
    pub visible_columns: usize,
    pub total_columns: usize,
    pub loading_message: Option<String>,
    pub error_message: Option<String>,
    pub filter_active: bool,
    pub has_more: bool,
    selected_snapshot_id: Option<i64>,
    current_snapshot_id: Option<i64>,
    highlighted_snapshot: Option<String>,
}

impl StatusBar {
    pub fn new() -> Self {
        Self {
            loaded_rows: 0,
            table_total_rows: None,
            filtered_rows: None,
            visible_columns: 0,
            total_columns: 0,
            loading_message: None,
            error_message: None,
            filter_active: false,
            has_more: false,
            selected_snapshot_id: None,
            current_snapshot_id: None,
            highlighted_snapshot: None,
        }
    }

    pub fn set_snapshot_view(&mut self, selected: Option<i64>, current: Option<i64>) {
        self.selected_snapshot_id = selected;
        self.current_snapshot_id = current;
    }

    pub fn set_highlighted_snapshot(&mut self, label: Option<String>) {
        self.highlighted_snapshot = label;
    }

    pub fn is_time_traveling(&self) -> bool {
        match (self.selected_snapshot_id, self.current_snapshot_id) {
            (Some(sel), Some(cur)) => sel != cur,
            _ => false,
        }
    }
}

impl Component for StatusBar {
    fn handle_key(&mut self, _key: KeyEvent) -> Option<Action> {
        None
    }

    fn handle_message(&mut self, msg: &AppMessage) -> Option<Action> {
        match msg {
            AppMessage::DataReady {
                total_rows,
                has_more,
                ..
            } => {
                self.has_more = *has_more;
                if self.filter_active {
                    self.filtered_rows = Some(*total_rows);
                } else {
                    self.loaded_rows = *total_rows;
                    self.filtered_rows = None;
                }
                self.loading_message = None;
            }
            AppMessage::TotalRowCount(total) => {
                self.table_total_rows = Some(*total);
            }
            AppMessage::LoadingStarted(msg) => {
                self.loading_message = Some(msg.clone());
                self.error_message = None;
            }
            AppMessage::LoadingFinished => {
                self.loading_message = None;
            }
            AppMessage::Error(err) => {
                self.error_message = Some(err.clone());
                self.loading_message = None;
            }
            _ => {}
        }
        None
    }

    fn render(&mut self, frame: &mut Frame, area: Rect, _focused: bool) {
        let mut spans = Vec::new();

        let total_suffix = self
            .table_total_rows
            .map(|t| format!("/{}", t))
            .unwrap_or_default();
        let more_hint = if self.has_more { " (m:+rows)" } else { "" };
        let row_text = if let Some(filtered) = self.filtered_rows {
            format!(
                " Rows: {}/{}{} (filtered){}",
                filtered, self.loaded_rows, total_suffix, more_hint
            )
        } else if self.loaded_rows > 0 || self.table_total_rows.is_some() {
            format!(" Rows: {}{}{}", self.loaded_rows, total_suffix, more_hint)
        } else {
            " Rows: -".to_string()
        };
        spans.push(Span::styled(row_text, Theme::status_bar()));

        // Column count
        if self.total_columns > 0 {
            spans.push(Span::styled(
                format!(" | Cols: {}/{}", self.visible_columns, self.total_columns),
                Theme::status_bar(),
            ));
        }

        if let Some(snap_id) = self
            .selected_snapshot_id
            .filter(|_| self.is_time_traveling())
        {
            spans.push(Span::styled(
                format!(" | Snapshot: {}", snap_id),
                Theme::status_time_travel(),
            ));
        }

        if let Some(ref label) = self.highlighted_snapshot {
            spans.push(Span::styled(format!(" | {}", label), Theme::status_bar()));
        }

        if let Some(ref err) = self.error_message {
            let err_display = if err.len() > ERROR_DISPLAY_MAX_LEN {
                format!(" | Error: {}...", &err[..ERROR_TRUNCATED_LEN])
            } else {
                format!(" | Error: {}", err)
            };
            spans.push(Span::styled(err_display, Theme::status_error()));
        } else if let Some(ref msg) = self.loading_message {
            spans.push(Span::styled(
                format!(" | Loading: {}", msg),
                Theme::status_loading(),
            ));
        }

        // Right-aligned key hints
        let hints = " q:quit ?:help ";
        let used_width: usize = spans.iter().map(|s| s.width()).sum();
        let remaining = area.width as usize - used_width.min(area.width as usize);
        if remaining > hints.len() {
            let padding = " ".repeat(remaining - hints.len());
            spans.push(Span::styled(padding, Theme::status_bar()));
            spans.push(Span::styled(hints, Theme::status_key_hint()));
        }

        let line = Line::from(spans);
        // Fill entire status bar background
        let bar = ratatui::widgets::Paragraph::new(line).style(Theme::status_bar());
        frame.render_widget(bar, area);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_status_bar_defaults() {
        let bar = StatusBar::new();
        assert_eq!(bar.loaded_rows, 0);
        assert!(bar.loading_message.is_none());
        assert!(bar.error_message.is_none());
    }

    #[test]
    fn handle_loading_messages() {
        let mut bar = StatusBar::new();
        bar.handle_message(&AppMessage::LoadingStarted("scanning...".into()));
        assert_eq!(bar.loading_message.as_deref(), Some("scanning..."));
        assert!(bar.error_message.is_none());

        bar.handle_message(&AppMessage::LoadingFinished);
        assert!(bar.loading_message.is_none());
    }

    #[test]
    fn set_snapshot_view_enables_time_travel() {
        let mut bar = StatusBar::new();
        assert!(!bar.is_time_traveling());

        bar.set_snapshot_view(Some(100), Some(200));
        assert!(bar.is_time_traveling());
    }

    #[test]
    fn same_snapshot_is_not_time_traveling() {
        let mut bar = StatusBar::new();
        bar.set_snapshot_view(Some(100), Some(100));
        assert!(!bar.is_time_traveling());
    }

    #[test]
    fn clear_snapshot_view_stops_time_travel() {
        let mut bar = StatusBar::new();
        bar.set_snapshot_view(Some(100), Some(200));
        assert!(bar.is_time_traveling());

        bar.set_snapshot_view(None, Some(200));
        assert!(!bar.is_time_traveling());
    }

    #[test]
    fn set_highlighted_snapshot() {
        let mut bar = StatusBar::new();
        assert!(bar.highlighted_snapshot.is_none());

        bar.set_highlighted_snapshot(Some("Snap: 1 (2026-02-23 07:15:08 UTC)".into()));
        assert_eq!(
            bar.highlighted_snapshot.as_deref(),
            Some("Snap: 1 (2026-02-23 07:15:08 UTC)")
        );

        bar.set_highlighted_snapshot(None);
        assert!(bar.highlighted_snapshot.is_none());
    }

    #[test]
    fn handle_error_clears_loading() {
        let mut bar = StatusBar::new();
        bar.handle_message(&AppMessage::LoadingStarted("loading".into()));
        bar.handle_message(&AppMessage::Error("table not found".into()));
        assert!(bar.loading_message.is_none());
        assert_eq!(bar.error_message.as_deref(), Some("table not found"));
    }

    #[test]
    fn handle_total_row_count() {
        let mut bar = StatusBar::new();
        assert!(bar.table_total_rows.is_none());

        bar.handle_message(&AppMessage::TotalRowCount(50000));
        assert_eq!(bar.table_total_rows, Some(50000));
    }
}
