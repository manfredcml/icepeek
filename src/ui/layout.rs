use ratatui::layout::{Constraint, Direction, Layout, Rect};

/// Top-level layout splits the terminal into: tab bar (top), content area (middle), status bar (bottom).
pub struct AppLayout {
    pub tab_bar: Rect,
    pub content: Rect,
    pub status_bar: Rect,
}

impl AppLayout {
    pub fn new(area: Rect) -> Self {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // tab bar
                Constraint::Min(3),    // content area
                Constraint::Length(1), // status bar
            ])
            .split(area);

        Self {
            tab_bar: chunks[0],
            content: chunks[1],
            status_bar: chunks[2],
        }
    }
}

/// Two-panel horizontal split for metadata tabs.
pub struct SplitLayout {
    pub left: Rect,
    pub right: Rect,
}

impl SplitLayout {
    pub fn new(area: Rect, left_percent: u16) -> Self {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(left_percent),
                Constraint::Percentage(100 - left_percent),
            ])
            .split(area);

        Self {
            left: chunks[0],
            right: chunks[1],
        }
    }
}

/// Filter bar + data content split (for data tab).
pub struct DataTabLayout {
    pub filter_bar: Rect,
    pub table: Rect,
}

impl DataTabLayout {
    pub fn new(area: Rect) -> Self {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // filter bar
                Constraint::Min(3),    // data table
            ])
            .split(area);

        Self {
            filter_bar: chunks[0],
            table: chunks[1],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rect(w: u16, h: u16) -> Rect {
        Rect::new(0, 0, w, h)
    }

    #[test]
    fn app_layout_splits_correctly() {
        let layout = AppLayout::new(rect(80, 24));
        assert_eq!(layout.tab_bar.height, 1);
        assert_eq!(layout.status_bar.height, 1);
        assert_eq!(layout.content.height, 22);
    }

    #[test]
    fn split_layout_divides_horizontally() {
        let split = SplitLayout::new(rect(100, 20), 40);
        assert_eq!(split.left.width, 40);
        assert_eq!(split.right.width, 60);
    }

    #[test]
    fn data_tab_layout_has_filter_and_table() {
        let layout = DataTabLayout::new(rect(80, 20));
        assert_eq!(layout.filter_bar.height, 1);
        assert!(layout.table.height >= 3);
    }
}
