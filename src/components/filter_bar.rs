use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::event::{Action, AppMessage};
use crate::ui::theme::Theme;

use super::Component;

pub struct FilterBar {
    /// Current filter text.
    pub text: String,
    /// Cursor position within the text.
    cursor: usize,
    /// Whether the filter bar is in active editing mode.
    editing: bool,
    /// Last successfully applied filter.
    applied_filter: Option<String>,
}

impl FilterBar {
    pub fn new() -> Self {
        Self {
            text: String::new(),
            cursor: 0,
            editing: false,
            applied_filter: None,
        }
    }

    pub fn start_editing(&mut self) {
        self.editing = true;
        self.cursor = self.text.len();
    }

    pub fn applied_filter(&self) -> Option<&str> {
        self.applied_filter.as_deref()
    }
}

impl Component for FilterBar {
    fn handle_key(&mut self, key: KeyEvent) -> Option<Action> {
        if !self.editing {
            return None;
        }

        match key.code {
            KeyCode::Enter => {
                self.editing = false;
                let filter_text = self.text.trim().to_string();
                if filter_text.is_empty() {
                    self.applied_filter = None;
                    return Some(Action::SubmitFilter(String::new()));
                }
                self.applied_filter = Some(filter_text.clone());
                Some(Action::SubmitFilter(filter_text))
            }
            KeyCode::Esc => {
                self.editing = false;
                self.text = self.applied_filter.clone().unwrap_or_default();
                None
            }
            KeyCode::Backspace => {
                if self.cursor > 0 {
                    self.text.remove(self.cursor - 1);
                    self.cursor -= 1;
                }
                None
            }
            KeyCode::Delete => {
                if self.cursor < self.text.len() {
                    self.text.remove(self.cursor);
                }
                None
            }
            KeyCode::Left => {
                if self.cursor > 0 {
                    self.cursor -= 1;
                }
                None
            }
            KeyCode::Right => {
                if self.cursor < self.text.len() {
                    self.cursor += 1;
                }
                None
            }
            KeyCode::Home => {
                self.cursor = 0;
                None
            }
            KeyCode::End => {
                self.cursor = self.text.len();
                None
            }
            KeyCode::Char(c) => {
                self.text.insert(self.cursor, c);
                self.cursor += 1;
                None
            }
            _ => None,
        }
    }

    fn handle_message(&mut self, _msg: &AppMessage) -> Option<Action> {
        None
    }

    fn render(&mut self, frame: &mut Frame, area: Rect, _focused: bool) {
        let style = if self.editing {
            Theme::filter_active()
        } else {
            Theme::filter_inactive()
        };

        let label = if self.editing {
            " Filter: "
        } else if self.text.is_empty() {
            " Filter: (press / to filter) "
        } else {
            " Filter: "
        };

        let spans = vec![
            Span::styled(label, Theme::label()),
            Span::styled(&self.text, style),
        ];

        if self.editing {
            let cursor_x = area.x + label.len() as u16 + self.cursor as u16;
            frame.set_cursor_position((cursor_x, area.y));
        }

        let line = Line::from(spans);
        let paragraph = Paragraph::new(line);
        frame.render_widget(paragraph, area);
    }

    fn is_input_mode(&self) -> bool {
        self.editing
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
    fn filter_bar_initial_state() {
        let bar = FilterBar::new();
        assert!(!bar.editing);
        assert!(bar.text.is_empty());
        assert!(bar.applied_filter().is_none());
    }

    #[test]
    fn typing_in_filter() {
        let mut bar = FilterBar::new();
        bar.start_editing();

        bar.handle_key(key(KeyCode::Char('p')));
        bar.handle_key(key(KeyCode::Char('r')));
        bar.handle_key(key(KeyCode::Char('i')));
        bar.handle_key(key(KeyCode::Char('c')));
        bar.handle_key(key(KeyCode::Char('e')));

        assert_eq!(bar.text, "price");
    }

    #[test]
    fn submit_filter() {
        let mut bar = FilterBar::new();
        bar.start_editing();
        bar.text = "price > 100".to_string();
        bar.cursor = bar.text.len();

        let action = bar.handle_key(key(KeyCode::Enter));
        assert!(matches!(action, Some(Action::SubmitFilter(ref s)) if s == "price > 100"));
        assert_eq!(bar.applied_filter(), Some("price > 100"));
        assert!(!bar.editing);
    }

    #[test]
    fn escape_reverts() {
        let mut bar = FilterBar::new();
        bar.applied_filter = Some("old filter".to_string());
        bar.text = "old filter".to_string();
        bar.start_editing();
        bar.text = "new text".to_string();

        bar.handle_key(key(KeyCode::Esc));
        assert_eq!(bar.text, "old filter");
        assert!(!bar.editing);
    }

    #[test]
    fn backspace_deletes() {
        let mut bar = FilterBar::new();
        bar.start_editing();
        bar.text = "abc".to_string();
        bar.cursor = 3;

        bar.handle_key(key(KeyCode::Backspace));
        assert_eq!(bar.text, "ab");
        assert_eq!(bar.cursor, 2);
    }

    #[test]
    fn is_input_mode_when_editing() {
        let mut bar = FilterBar::new();
        assert!(!bar.is_input_mode());
        bar.start_editing();
        assert!(bar.is_input_mode());
    }
}
