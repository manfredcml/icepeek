use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState};
use ratatui::Frame;

use crate::event::{Action, AppMessage};
use crate::ui::theme::Theme;

use super::Component;

const POPUP_WIDTH: u16 = 50;
const POPUP_HEIGHT: u16 = 20;
const POPUP_MARGIN: u16 = 4;

pub struct ColumnSelector {
    /// All available column names.
    columns: Vec<String>,
    /// Which columns are currently enabled (by index).
    enabled: Vec<bool>,
    /// List navigation state.
    list_state: ListState,
    /// Whether the popup is visible.
    pub visible: bool,
}

impl ColumnSelector {
    pub fn new() -> Self {
        Self {
            columns: vec![],
            enabled: vec![],
            list_state: ListState::default(),
            visible: false,
        }
    }

    pub fn set_columns(&mut self, columns: Vec<String>, visible: &[String]) {
        self.enabled = columns.iter().map(|c| visible.contains(c)).collect();
        self.columns = columns;
        if !self.columns.is_empty() {
            self.list_state.select(Some(0));
        }
    }

    pub fn show(&mut self) {
        self.visible = true;
    }

    pub fn hide(&mut self) {
        self.visible = false;
    }

    /// Get the list of currently enabled column names.
    pub fn enabled_columns(&self) -> Vec<String> {
        self.columns
            .iter()
            .zip(self.enabled.iter())
            .filter(|(_, e)| **e)
            .map(|(c, _)| c.clone())
            .collect()
    }

    /// Calculate a centered popup rect.
    pub fn popup_area(area: Rect) -> Rect {
        let width = POPUP_WIDTH.min(area.width.saturating_sub(POPUP_MARGIN));
        let height = POPUP_HEIGHT.min(area.height.saturating_sub(POPUP_MARGIN));
        let x = (area.width.saturating_sub(width)) / 2;
        let y = (area.height.saturating_sub(height)) / 2;
        Rect::new(area.x + x, area.y + y, width, height)
    }
}

impl Component for ColumnSelector {
    fn handle_key(&mut self, key: KeyEvent) -> Option<Action> {
        if !self.visible {
            return None;
        }

        match key.code {
            KeyCode::Esc | KeyCode::Char('c') => {
                self.visible = false;
                None
            }
            KeyCode::Up | KeyCode::Char('k') => {
                let i = self.list_state.selected().unwrap_or(0);
                if i > 0 {
                    self.list_state.select(Some(i - 1));
                }
                None
            }
            KeyCode::Down | KeyCode::Char('j') => {
                let i = self.list_state.selected().unwrap_or(0);
                if i + 1 < self.columns.len() {
                    self.list_state.select(Some(i + 1));
                }
                None
            }
            KeyCode::Char(' ') | KeyCode::Enter => {
                let i = self.list_state.selected()?;
                if i >= self.enabled.len() {
                    return None;
                }
                self.enabled[i] = !self.enabled[i];
                Some(Action::ToggleColumn(self.columns[i].clone()))
            }
            KeyCode::Char('a') => {
                let all_enabled = self.enabled.iter().all(|e| *e);
                for e in &mut self.enabled {
                    *e = !all_enabled;
                }
                Some(Action::ToggleColumn(String::new())) // empty = refresh all
            }
            _ => None,
        }
    }

    fn handle_message(&mut self, _msg: &AppMessage) -> Option<Action> {
        None
    }

    fn render(&mut self, frame: &mut Frame, area: Rect, _focused: bool) {
        if !self.visible {
            return;
        }

        let popup = Self::popup_area(area);

        frame.render_widget(Clear, popup);

        let items: Vec<ListItem> = self
            .columns
            .iter()
            .zip(self.enabled.iter())
            .map(|(name, enabled)| {
                let checkbox = if *enabled { "[x]" } else { "[ ]" };
                let line = Line::from(vec![
                    Span::styled(format!("{} ", checkbox), Theme::label()),
                    Span::styled(name.clone(), Theme::value()),
                ]);
                ListItem::new(line)
            })
            .collect();

        let block = Block::default()
            .borders(Borders::ALL)
            .title(" Columns (space=toggle, a=all, esc=close) ")
            .border_style(Theme::border_focused());

        let list = List::new(items)
            .block(block)
            .highlight_style(Theme::table_row_selected());

        frame.render_stateful_widget(list, popup, &mut self.list_state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    #[test]
    fn column_selector_initial() {
        let cs = ColumnSelector::new();
        assert!(!cs.visible);
        assert!(cs.columns.is_empty());
    }

    #[test]
    fn set_columns_and_enabled() {
        let mut cs = ColumnSelector::new();
        cs.set_columns(
            vec!["id".into(), "name".into(), "price".into()],
            &["id".into(), "price".into()],
        );
        assert_eq!(cs.enabled, vec![true, false, true]);
        assert_eq!(cs.enabled_columns(), vec!["id", "price"]);
    }

    #[test]
    fn toggle_column() {
        let mut cs = ColumnSelector::new();
        cs.set_columns(vec!["a".into(), "b".into()], &["a".into(), "b".into()]);
        cs.visible = true;
        cs.list_state.select(Some(1));

        cs.handle_key(key(KeyCode::Char(' ')));
        assert!(!cs.enabled[1]);
        assert_eq!(cs.enabled_columns(), vec!["a"]);
    }

    #[test]
    fn escape_closes() {
        let mut cs = ColumnSelector::new();
        cs.visible = true;
        cs.handle_key(key(KeyCode::Esc));
        assert!(!cs.visible);
    }

    #[test]
    fn popup_area_centered() {
        let area = Rect::new(0, 0, 80, 24);
        let popup = ColumnSelector::popup_area(area);
        assert!(popup.x > 0);
        assert!(popup.y > 0);
        assert!(popup.width <= POPUP_WIDTH);
        assert!(popup.height <= POPUP_HEIGHT);
    }
}
