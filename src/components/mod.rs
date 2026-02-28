pub mod column_selector;
pub mod data_view;
pub mod filter_bar;
pub mod help_popup;
pub mod manifest_panel;
pub mod properties_panel;
pub mod schema_panel;
pub mod snapshot_panel;
pub mod status_bar;

use crossterm::event::KeyEvent;
use ratatui::layout::Rect;
use ratatui::Frame;

use crate::event::{Action, AppMessage};

/// Trait implemented by all TUI components.
pub trait Component {
    /// Handle a key event. Returns an Action if the component wants the app to do something.
    fn handle_key(&mut self, key: KeyEvent) -> Option<Action>;

    /// Handle a message from a background task.
    fn handle_message(&mut self, msg: &AppMessage) -> Option<Action>;

    /// Render the component into the given area.
    fn render(&mut self, frame: &mut Frame, area: Rect, focused: bool);

    /// Whether this component is currently in text input mode (captures all keys).
    fn is_input_mode(&self) -> bool {
        false
    }
}
