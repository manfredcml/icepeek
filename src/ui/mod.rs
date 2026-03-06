pub mod layout;
pub mod theme;

/// Which tab is currently active.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Data,
    Schema,
    Snapshots,
    Files,
    Properties,
    Stats,
}

impl Tab {
    pub const ALL: [Tab; 6] = [
        Tab::Data,
        Tab::Schema,
        Tab::Files,
        Tab::Properties,
        Tab::Stats,
        Tab::Snapshots,
    ];

    pub fn label(&self) -> &'static str {
        match self {
            Tab::Data => "1:Data",
            Tab::Schema => "2:Schema",
            Tab::Files => "3:Files",
            Tab::Properties => "4:Props",
            Tab::Stats => "5:Stats",
            Tab::Snapshots => "6:Snapshots",
        }
    }

    pub fn from_index(i: usize) -> Option<Tab> {
        Tab::ALL.get(i).copied()
    }

    pub fn index(&self) -> usize {
        Tab::ALL.iter().position(|t| t == self).unwrap()
    }
}

/// Which panel within a tab currently has focus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    Left,
    Right,
    FilterBar,
    ColumnSelector,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tab_round_trip() {
        for (i, tab) in Tab::ALL.iter().enumerate() {
            assert_eq!(Tab::from_index(i), Some(*tab));
            assert_eq!(tab.index(), i);
        }
        assert_eq!(Tab::from_index(99), None);
    }

    #[test]
    fn tab_labels() {
        assert_eq!(Tab::Data.label(), "1:Data");
        assert_eq!(Tab::Properties.label(), "4:Props");
        assert_eq!(Tab::Stats.label(), "5:Stats");
        assert_eq!(Tab::Snapshots.label(), "6:Snapshots");
    }
}
