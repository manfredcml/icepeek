use arrow_array::RecordBatch;
use crossterm::event::{self, Event, KeyEvent};
use std::time::Duration;
use tokio::sync::mpsc;

use crate::model::table_info::{DataFileInfo, ManifestInfo, TableMetadata};

#[derive(Debug, PartialEq)]
pub enum Action {
    Quit,
    SwitchTab(usize),
    FocusNext,
    FocusPrev,
    ToggleHelp,
    ToggleColumnSelector,
    FocusFilter,
    Reload,
    IncreaseLimit,
    SubmitFilter(String),
    ToggleColumn(String),
    SelectSnapshot(i64),
}

/// Messages sent from background loader tasks back to the main UI thread.
#[derive(Debug)]
pub enum AppMessage {
    DataReady {
        batches: Vec<RecordBatch>,
        total_rows: usize,
        has_more: bool,
    },
    MetadataReady(Box<TableMetadata>),
    ManifestsReady(Vec<ManifestInfo>),
    DataFileStatsReady(Vec<Vec<DataFileInfo>>),
    TotalRowCount(usize),
    LoadingStarted(String),
    LoadingFinished,
    Error(String),
}

pub fn spawn_event_reader(tx: mpsc::UnboundedSender<Event>) {
    tokio::task::spawn_blocking(move || loop {
        if event::poll(Duration::from_millis(50)).unwrap_or(false) {
            if let Ok(ev) = event::read() {
                if tx.send(ev).is_err() {
                    break;
                }
            }
        }
    });
}

pub fn to_key_event(ev: &Event) -> Option<KeyEvent> {
    match ev {
        Event::Key(key) => Some(*key),
        _ => None,
    }
}
