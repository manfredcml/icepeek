use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;

use crate::event::{Action, AppMessage};
use crate::ui::theme::Theme;

use super::Component;

const POPUP_WIDTH: u16 = 68;
const POPUP_HEIGHT: u16 = 22;
const POPUP_MARGIN: u16 = 4;

pub struct HelpPopup {
    pub visible: bool,
}

impl HelpPopup {
    pub fn new() -> Self {
        Self { visible: false }
    }

    pub fn toggle(&mut self) {
        self.visible = !self.visible;
    }

    fn popup_area(area: Rect) -> Rect {
        let width = POPUP_WIDTH.min(area.width.saturating_sub(POPUP_MARGIN));
        let height = POPUP_HEIGHT.min(area.height.saturating_sub(POPUP_MARGIN));
        let x = (area.width.saturating_sub(width)) / 2;
        let y = (area.height.saturating_sub(height)) / 2;
        Rect::new(area.x + x, area.y + y, width, height)
    }

    fn keybindings() -> Vec<(&'static str, &'static str)> {
        vec![
            ("1-5", "Switch tab (Data/Schema/Files/Props/Snap)"),
            ("q", "Quit"),
            ("?", "Toggle this help"),
            ("Tab / Shift+Tab", "Cycle focus between panels"),
            ("j/k or Up/Down", "Navigate within panel"),
            ("h/l or Left/Right", "Horizontal scroll (data)"),
            ("g / G", "Jump to top / bottom"),
            ("PgUp / PgDn", "Page up / down"),
            ("/", "Focus filter bar (data tab)"),
            ("c", "Open column selector (data tab)"),
            ("Enter", "Expand / select / time-travel (snapshots)"),
            ("Esc", "Cancel / close popup"),
            ("r", "Reload (preserves snapshot selection)"),
            ("m", "Increase row limit"),
        ]
    }
}

impl Component for HelpPopup {
    fn handle_key(&mut self, key: KeyEvent) -> Option<Action> {
        if !self.visible {
            return None;
        }

        match key.code {
            KeyCode::Esc | KeyCode::Char('?') | KeyCode::Char('q') => {
                self.visible = false;
                None
            }
            _ => None, // Consume all keys while help is open
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

        let mut lines: Vec<Line> = Vec::new();
        lines.push(Line::styled(
            " icepeek — Keyboard Shortcuts",
            Theme::title(),
        ));
        lines.push(Line::raw(""));

        for (key, desc) in Self::keybindings() {
            lines.push(Line::from(vec![
                Span::styled(format!("  {:20}", key), Theme::help_key()),
                Span::styled(desc, Theme::help_description()),
            ]));
        }

        lines.push(Line::raw(""));
        lines.push(Line::styled(
            " Press ? or Esc to close",
            Theme::status_key_hint(),
        ));

        let block = Block::default()
            .borders(Borders::ALL)
            .title(" Help ")
            .border_style(Theme::border_focused());

        let paragraph = Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: false });

        frame.render_widget(paragraph, popup);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::KeyModifiers;

    #[test]
    fn help_popup_toggle() {
        let mut popup = HelpPopup::new();
        assert!(!popup.visible);
        popup.toggle();
        assert!(popup.visible);
        popup.toggle();
        assert!(!popup.visible);
    }

    #[test]
    fn help_popup_escape_closes() {
        let mut popup = HelpPopup::new();
        popup.visible = true;
        popup.handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert!(!popup.visible);
    }

    #[test]
    fn keybindings_not_empty() {
        let bindings = HelpPopup::keybindings();
        assert!(bindings.len() > 10);
    }
}
