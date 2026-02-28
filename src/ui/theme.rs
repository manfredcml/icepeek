use ratatui::style::{Color, Modifier, Style};

/// Color palette and style constants for the TUI.
pub struct Theme;

impl Theme {
    // Tab bar
    pub fn tab_active() -> Style {
        Style::default()
            .fg(Color::White)
            .bg(Color::DarkGray)
            .add_modifier(Modifier::BOLD)
    }

    pub fn tab_inactive() -> Style {
        Style::default().fg(Color::Gray)
    }

    pub fn tab_bar_bg() -> Style {
        Style::default().bg(Color::Black)
    }

    // Data table
    pub fn table_header() -> Style {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    }

    pub fn table_row_normal() -> Style {
        Style::default().fg(Color::White)
    }

    pub fn table_row_selected() -> Style {
        Style::default().fg(Color::Black).bg(Color::LightCyan)
    }

    pub fn table_row_alt() -> Style {
        Style::default().fg(Color::White).bg(Color::Rgb(25, 25, 30))
    }

    // Borders and panels
    pub fn border_focused() -> Style {
        Style::default().fg(Color::Cyan)
    }

    pub fn border_unfocused() -> Style {
        Style::default().fg(Color::DarkGray)
    }

    // Status bar
    pub fn status_bar() -> Style {
        Style::default().fg(Color::White).bg(Color::DarkGray)
    }

    pub fn status_loading() -> Style {
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    }

    pub fn status_error() -> Style {
        Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
    }

    pub fn status_key_hint() -> Style {
        Style::default().fg(Color::DarkGray)
    }

    // Filter bar
    pub fn filter_active() -> Style {
        Style::default().fg(Color::Yellow)
    }

    pub fn filter_inactive() -> Style {
        Style::default().fg(Color::DarkGray)
    }

    // Help popup
    pub fn help_key() -> Style {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    }

    pub fn help_description() -> Style {
        Style::default().fg(Color::White)
    }

    // Tree / metadata views
    pub fn field_name() -> Style {
        Style::default()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD)
    }

    pub fn field_type() -> Style {
        Style::default().fg(Color::Yellow)
    }

    pub fn field_id() -> Style {
        Style::default().fg(Color::DarkGray)
    }

    pub fn label() -> Style {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    }

    pub fn value() -> Style {
        Style::default().fg(Color::White)
    }

    pub fn title() -> Style {
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD)
    }

    pub fn status_time_travel() -> Style {
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn theme_styles_are_distinct() {
        // Smoke test: ensure various styles can be created without panic
        let _ = Theme::tab_active();
        let _ = Theme::tab_inactive();
        let _ = Theme::table_header();
        let _ = Theme::table_row_selected();
        let _ = Theme::border_focused();
        let _ = Theme::status_bar();
        let _ = Theme::filter_active();
        let _ = Theme::help_key();
        let _ = Theme::field_name();
        let _ = Theme::status_time_travel();
    }
}
