use std::io;
use std::sync::Mutex;

use anyhow::{Context, Result};
use crossterm::event::{Event, KeyCode, KeyEvent};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use crossterm::ExecutableCommand;
use ratatui::prelude::*;
use ratatui::widgets::Tabs;
use tokio::sync::mpsc;

use crate::cli::{self, Cli, Command};
use crate::components::column_selector::ColumnSelector;
use crate::components::data_view::DataView;
use crate::components::filter_bar::FilterBar;
use crate::components::help_popup::HelpPopup;
use crate::components::manifest_panel::ManifestPanel;
use crate::components::properties_panel::PropertiesPanel;
use crate::components::schema_panel::SchemaPanel;
use crate::components::snapshot_panel::SnapshotPanel;
use crate::components::status_bar::StatusBar;
use crate::components::Component;
use crate::event::{spawn_event_reader, to_key_event, Action, AppMessage};
use crate::loader::arrow_convert::total_row_count;
use crate::loader::catalog_loader::load_from_catalog;
use crate::loader::direct_loader::load_direct;
use crate::loader::scan::{execute_scan, ScanRequest};
use crate::loader::TableHandle;
use crate::model::filter;
use crate::model::table_info::{DataFileInfo, ManifestInfo};
use crate::ui::layout::{AppLayout, DataTabLayout};
use crate::ui::theme::Theme;
use crate::ui::{Focus, Tab};

static TABLE_HANDLE: Mutex<Option<TableHandle>> = Mutex::new(None);

struct App {
    data_view: DataView,
    filter_bar: FilterBar,
    column_selector: ColumnSelector,
    schema_panel: SchemaPanel,
    snapshot_panel: SnapshotPanel,
    manifest_panel: ManifestPanel,
    properties_panel: PropertiesPanel,
    status_bar: StatusBar,
    help_popup: HelpPopup,
    active_tab: Tab,
    focus: Focus,
    initial_columns: Option<Vec<String>>,
    limit: Option<usize>,
    page_size: usize,
    has_more: bool,
    selected_snapshot_id: Option<i64>,
    current_snapshot_id: Option<i64>,
}

impl App {
    fn new(initial_columns: Option<Vec<String>>, limit: Option<usize>, page_size: usize) -> Self {
        Self {
            data_view: DataView::new(),
            filter_bar: FilterBar::new(),
            column_selector: ColumnSelector::new(),
            schema_panel: SchemaPanel::new(),
            snapshot_panel: SnapshotPanel::new(),
            manifest_panel: ManifestPanel::new(),
            properties_panel: PropertiesPanel::new(),
            status_bar: StatusBar::new(),
            help_popup: HelpPopup::new(),
            active_tab: Tab::Data,
            focus: Focus::Left,
            initial_columns,
            limit,
            page_size,
            has_more: false,
            selected_snapshot_id: None,
            current_snapshot_id: None,
        }
    }

    fn draw(&mut self, frame: &mut Frame) {
        let snap_label = self.snapshot_panel.selected_snapshot().map(|s| {
            format!(
                "Snap: {} ({})",
                s.snapshot_id,
                SnapshotPanel::format_timestamp(s.timestamp_ms)
            )
        });
        self.status_bar.set_highlighted_snapshot(snap_label);

        let layout = AppLayout::new(frame.area());

        let tab_titles: Vec<Line> = Tab::ALL
            .iter()
            .map(|t| {
                if *t == self.active_tab {
                    Line::styled(t.label(), Theme::tab_active())
                } else {
                    Line::styled(t.label(), Theme::tab_inactive())
                }
            })
            .collect();

        let tabs = Tabs::new(tab_titles)
            .select(self.active_tab.index())
            .divider(" │ ")
            .style(Theme::tab_bar_bg());

        frame.render_widget(tabs, layout.tab_bar);

        match self.active_tab {
            Tab::Data => {
                let data_layout = DataTabLayout::new(layout.content);
                self.filter_bar.render(
                    frame,
                    data_layout.filter_bar,
                    self.focus == Focus::FilterBar,
                );
                self.data_view
                    .render(frame, data_layout.table, self.focus == Focus::Left);
            }
            Tab::Schema => self.schema_panel.render(frame, layout.content, true),
            Tab::Snapshots => self.snapshot_panel.render(frame, layout.content, true),
            Tab::Files => self.manifest_panel.render(frame, layout.content, true),
            Tab::Properties => self.properties_panel.render(frame, layout.content, true),
        }

        self.status_bar.render(frame, layout.status_bar, false);

        self.column_selector
            .render(frame, frame.area(), self.focus == Focus::ColumnSelector);
        self.help_popup.render(frame, frame.area(), true);
    }

    fn handle_key(&mut self, key: KeyEvent) -> Option<Action> {
        if self.help_popup.visible {
            return self.help_popup.handle_key(key);
        }

        if self.column_selector.visible {
            return self.column_selector.handle_key(key);
        }

        if self.filter_bar.is_input_mode() {
            return self.filter_bar.handle_key(key);
        }

        match key.code {
            KeyCode::Char('q') => return Some(Action::Quit),
            KeyCode::Char('?') => return Some(Action::ToggleHelp),
            KeyCode::Char('1') => return Some(Action::SwitchTab(0)),
            KeyCode::Char('2') => return Some(Action::SwitchTab(1)),
            KeyCode::Char('3') => return Some(Action::SwitchTab(2)),
            KeyCode::Char('4') => return Some(Action::SwitchTab(3)),
            KeyCode::Char('5') => return Some(Action::SwitchTab(4)),
            KeyCode::Char('r') => return Some(Action::Reload),
            KeyCode::Char('m') => return Some(Action::IncreaseLimit),
            KeyCode::Tab => return Some(Action::FocusNext),
            KeyCode::BackTab => return Some(Action::FocusPrev),
            _ => {}
        }

        match self.active_tab {
            Tab::Data => self.data_view.handle_key(key),
            Tab::Schema => self.schema_panel.handle_key(key),
            Tab::Snapshots => self.snapshot_panel.handle_key(key),
            Tab::Files => self.manifest_panel.handle_key(key),
            Tab::Properties => self.properties_panel.handle_key(key),
        }
    }

    async fn handle_action(
        &mut self,
        action: Action,
        msg_tx: &mpsc::UnboundedSender<AppMessage>,
    ) -> Result<bool> {
        match action {
            Action::Quit => return Ok(true),
            Action::SwitchTab(idx) => {
                let Some(tab) = Tab::from_index(idx) else {
                    return Ok(false);
                };
                self.active_tab = tab;
                self.focus = Focus::Left;

                if tab == Tab::Files && self.manifest_panel.needs_load() {
                    let msg_tx = msg_tx.clone();
                    let snap_id = self.selected_snapshot_id;
                    tokio::spawn(async move {
                        let _ =
                            msg_tx.send(AppMessage::LoadingStarted("Loading manifests...".into()));
                        load_manifests(&msg_tx, snap_id).await;
                        let _ = msg_tx.send(AppMessage::LoadingFinished);
                    });
                }
            }
            Action::FocusNext | Action::FocusPrev => {
                self.focus = match self.focus {
                    Focus::Left => Focus::Right,
                    Focus::Right => Focus::Left,
                    _ => Focus::Left,
                };
            }
            Action::ToggleHelp => {
                self.help_popup.toggle();
            }
            Action::FocusFilter => {
                self.focus = Focus::FilterBar;
                self.filter_bar.start_editing();
            }
            Action::ToggleColumnSelector => {
                if self.column_selector.visible {
                    self.column_selector.hide();
                    self.focus = Focus::Left;
                    let enabled = self.column_selector.enabled_columns();
                    self.data_view.set_visible_columns(enabled.clone());
                    self.status_bar.visible_columns = enabled.len();
                } else {
                    self.column_selector.show();
                    self.focus = Focus::ColumnSelector;
                }
            }
            Action::ToggleColumn(_) => {
                let enabled = self.column_selector.enabled_columns();
                self.data_view.set_visible_columns(enabled.clone());
                self.status_bar.visible_columns = enabled.len();
            }
            Action::SubmitFilter(filter_text) => {
                self.focus = Focus::Left;
                self.limit = Some(self.page_size);

                if filter_text.is_empty() {
                    self.status_bar.filter_active = false;
                    spawn_rescan(
                        msg_tx.clone(),
                        None,
                        self.data_view.visible_columns().to_vec(),
                        self.selected_snapshot_id,
                        self.limit,
                    );
                    return Ok(false);
                }

                let predicate = match filter::parse_filter(&filter_text) {
                    Ok(p) => p,
                    Err(e) => {
                        let _ = msg_tx.send(AppMessage::Error(format!("Filter error: {}", e)));
                        return Ok(false);
                    }
                };
                self.status_bar.filter_active = true;
                spawn_rescan(
                    msg_tx.clone(),
                    Some(predicate),
                    self.data_view.visible_columns().to_vec(),
                    self.selected_snapshot_id,
                    self.limit,
                );
            }
            Action::SelectSnapshot(snapshot_id) => {
                let is_current = self.current_snapshot_id == Some(snapshot_id);
                self.selected_snapshot_id = if is_current { None } else { Some(snapshot_id) };
                self.limit = Some(self.page_size);

                self.snapshot_panel
                    .set_viewed_snapshot(self.selected_snapshot_id);
                self.properties_panel
                    .set_viewed_snapshot(self.selected_snapshot_id);
                self.status_bar
                    .set_snapshot_view(self.selected_snapshot_id, self.current_snapshot_id);

                let schema_id = self
                    .selected_snapshot_id
                    .and_then(|sid| self.snapshot_panel.schema_id_for_snapshot(sid));
                self.schema_panel.set_viewed_schema(schema_id);

                self.manifest_panel.invalidate();
                if self.active_tab == Tab::Files {
                    let msg_tx = msg_tx.clone();
                    let snap_id = self.selected_snapshot_id;
                    tokio::spawn(async move {
                        let _ =
                            msg_tx.send(AppMessage::LoadingStarted("Loading manifests...".into()));
                        load_manifests(&msg_tx, snap_id).await;
                        let _ = msg_tx.send(AppMessage::LoadingFinished);
                    });
                }

                let predicate = self
                    .filter_bar
                    .applied_filter()
                    .and_then(|f| filter::parse_filter(f).ok());
                spawn_rescan(
                    msg_tx.clone(),
                    predicate,
                    vec![],
                    self.selected_snapshot_id,
                    self.limit,
                );
                if let Some(handle) = TABLE_HANDLE.lock().unwrap().clone() {
                    spawn_count_rows(msg_tx.clone(), handle, self.selected_snapshot_id);
                }
            }
            Action::IncreaseLimit => {
                if !self.has_more {
                    return Ok(false);
                }
                let loaded = self.limit.unwrap_or(0);
                self.limit = Some(loaded + self.page_size);
                let predicate = self
                    .filter_bar
                    .applied_filter()
                    .and_then(|f| filter::parse_filter(f).ok());
                spawn_rescan(
                    msg_tx.clone(),
                    predicate,
                    self.data_view.visible_columns().to_vec(),
                    self.selected_snapshot_id,
                    self.limit,
                );
            }
            Action::Reload => {
                let predicate = self
                    .filter_bar
                    .applied_filter()
                    .and_then(|f| filter::parse_filter(f).ok());
                spawn_rescan(
                    msg_tx.clone(),
                    predicate,
                    self.data_view.visible_columns().to_vec(),
                    self.selected_snapshot_id,
                    self.limit,
                );
            }
        }
        Ok(false)
    }

    fn handle_message(&mut self, msg: &AppMessage) {
        self.data_view.handle_message(msg);
        self.schema_panel.handle_message(msg);
        self.snapshot_panel.handle_message(msg);
        self.manifest_panel.handle_message(msg);
        self.properties_panel.handle_message(msg);
        self.status_bar.handle_message(msg);

        if let AppMessage::MetadataReady(metadata) = msg {
            self.current_snapshot_id = metadata.current_snapshot_id;
        }

        if let AppMessage::DataReady {
            has_more,
            total_rows,
            ..
        } = msg
        {
            self.has_more = *has_more;
            self.limit = Some(*total_rows);

            let all_cols = self.data_view.all_columns().to_vec();
            let vis_cols = if let Some(ref cols) = self.initial_columns {
                cols.clone()
            } else {
                all_cols.clone()
            };
            self.column_selector
                .set_columns(all_cols.clone(), &vis_cols);
            self.status_bar.visible_columns = vis_cols.len();
            self.status_bar.total_columns = all_cols.len();

            if self.initial_columns.is_some() {
                self.data_view.set_visible_columns(vis_cols);
            }
        }
    }
}

// --- Terminal setup ---

pub async fn run(cli: Cli) -> Result<()> {
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = io::stdout().execute(LeaveAlternateScreen);
        original_hook(info);
    }));

    enable_raw_mode().context("failed to enable raw mode")?;
    io::stdout()
        .execute(EnterAlternateScreen)
        .context("failed to enter alternate screen")?;
    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend)?;

    let result = run_app(&mut terminal, cli).await;

    disable_raw_mode()?;
    io::stdout().execute(LeaveAlternateScreen)?;

    result
}

// --- Event loop ---

async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    cli: Cli,
) -> Result<()> {
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<AppMessage>();

    let (initial_columns, limit, no_limit) = match &cli.command {
        Command::Open {
            columns,
            limit,
            no_limit,
            ..
        } => (columns.clone(), *limit, *no_limit),
        Command::Catalog {
            columns,
            limit,
            no_limit,
            ..
        } => (columns.clone(), *limit, *no_limit),
    };

    let effective = cli::effective_limit(limit, no_limit);
    let page_size = limit.unwrap_or(cli::DEFAULT_PAGE_SIZE);
    let mut app = App::new(initial_columns, effective, page_size);

    spawn_initial_load(msg_tx.clone(), cli.command, effective);

    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<Event>();
    spawn_event_reader(event_tx);

    loop {
        terminal.draw(|frame| app.draw(frame))?;

        tokio::select! {
            Some(event) = event_rx.recv() => {
                let Some(key) = to_key_event(&event) else { continue };
                let Some(action) = app.handle_key(key) else { continue };
                if app.handle_action(action, &msg_tx).await? {
                    return Ok(());
                }
            }
            Some(msg) = msg_rx.recv() => {
                app.handle_message(&msg);
            }
        }
    }
}

// --- Background tasks ---

fn spawn_initial_load(
    msg_tx: mpsc::UnboundedSender<AppMessage>,
    command: Command,
    limit: Option<usize>,
) {
    tokio::spawn(async move {
        let _ = msg_tx.send(AppMessage::LoadingStarted("Loading table...".into()));

        let result = match command {
            Command::Open {
                ref path,
                ref storage,
                ..
            } => load_direct(path, storage).await,
            Command::Catalog {
                ref uri,
                ref table,
                ref storage,
                ..
            } => load_from_catalog(uri, table, storage).await,
        };

        let handle = match result {
            Ok(h) => h,
            Err(e) => {
                let _ = msg_tx.send(AppMessage::Error(format!("Load error: {}", e)));
                let _ = msg_tx.send(AppMessage::LoadingFinished);
                return;
            }
        };

        match handle.extract_metadata() {
            Ok(metadata) => {
                let _ = msg_tx.send(AppMessage::MetadataReady(Box::new(metadata)));
            }
            Err(e) => {
                let _ = msg_tx.send(AppMessage::Error(format!("Metadata error: {}", e)));
            }
        }

        let _ = msg_tx.send(AppMessage::LoadingStarted("Scanning data...".into()));
        let scan_request = ScanRequest {
            limit,
            ..Default::default()
        };
        match execute_scan(&handle, &scan_request).await {
            Ok(result) => {
                let total_rows = total_row_count(&result.batches);
                let _ = msg_tx.send(AppMessage::DataReady {
                    batches: result.batches,
                    total_rows,
                    has_more: result.has_more,
                });
            }
            Err(e) => {
                let _ = msg_tx.send(AppMessage::Error(format!("Scan error: {}", e)));
            }
        }
        let _ = msg_tx.send(AppMessage::LoadingFinished);

        TABLE_HANDLE.lock().unwrap().replace(handle.clone());

        spawn_count_rows(msg_tx.clone(), handle, None);
    });
}

fn spawn_rescan(
    msg_tx: mpsc::UnboundedSender<AppMessage>,
    predicate: Option<iceberg::expr::Predicate>,
    columns: Vec<String>,
    snapshot_id: Option<i64>,
    limit: Option<usize>,
) {
    tokio::spawn(async move {
        let _ = msg_tx.send(AppMessage::LoadingStarted("Scanning...".into()));

        let Some(handle) = TABLE_HANDLE.lock().unwrap().clone() else {
            let _ = msg_tx.send(AppMessage::Error("No table loaded".into()));
            let _ = msg_tx.send(AppMessage::LoadingFinished);
            return;
        };

        let request = ScanRequest {
            columns: if columns.is_empty() {
                None
            } else {
                Some(columns)
            },
            filter: predicate,
            snapshot_id,
            limit,
        };

        match execute_scan(&handle, &request).await {
            Ok(result) => {
                let total_rows = total_row_count(&result.batches);
                let _ = msg_tx.send(AppMessage::DataReady {
                    batches: result.batches,
                    total_rows,
                    has_more: result.has_more,
                });
            }
            Err(e) => {
                let _ = msg_tx.send(AppMessage::Error(format!("Scan error: {}", e)));
            }
        }

        let _ = msg_tx.send(AppMessage::LoadingFinished);
    });
}

fn spawn_count_rows(
    msg_tx: mpsc::UnboundedSender<AppMessage>,
    handle: TableHandle,
    snapshot_id: Option<i64>,
) {
    tokio::spawn(async move {
        if let Ok(total) = handle.count_total_rows(snapshot_id).await {
            let _ = msg_tx.send(AppMessage::TotalRowCount(total));
        }
    });
}

async fn load_manifests(msg_tx: &mpsc::UnboundedSender<AppMessage>, snapshot_id: Option<i64>) {
    let handle = TABLE_HANDLE.lock().unwrap().clone();
    let Some(handle) = handle else {
        let _ = msg_tx.send(AppMessage::Error("No table loaded".into()));
        return;
    };

    let metadata = handle.table.metadata();
    let snapshot = match snapshot_id {
        Some(id) => metadata.snapshot_by_id(id),
        None => metadata.current_snapshot(),
    };
    let Some(snapshot) = snapshot else {
        let _ = msg_tx.send(AppMessage::ManifestsReady(vec![]));
        let _ = msg_tx.send(AppMessage::DataFileStatsReady(vec![]));
        return;
    };

    let file_io = handle.table.file_io().clone();

    let manifest_list = match snapshot.load_manifest_list(&file_io, metadata).await {
        Ok(list) => list,
        Err(e) => {
            let _ = msg_tx.send(AppMessage::Error(format!(
                "Failed to load manifest list: {}",
                e
            )));
            return;
        }
    };

    let mut manifest_infos = Vec::new();
    let mut grouped_files: Vec<Vec<DataFileInfo>> = Vec::new();

    for mf in manifest_list.entries() {
        manifest_infos.push(ManifestInfo {
            path: mf.manifest_path.clone(),
            content_type: mf.content.to_string(),
            added_data_files_count: mf.added_files_count.map(|v| v as i32),
            added_rows_count: mf.added_rows_count.map(|v| v as i64),
            existing_data_files_count: mf.existing_files_count.map(|v| v as i32),
            existing_rows_count: mf.existing_rows_count.map(|v| v as i64),
            deleted_data_files_count: mf.deleted_files_count.map(|v| v as i32),
            deleted_rows_count: mf.deleted_rows_count.map(|v| v as i64),
            sequence_number: mf.sequence_number,
            partition_spec_id: mf.partition_spec_id,
        });

        let manifest = match mf.load_manifest(&file_io).await {
            Ok(m) => m,
            Err(e) => {
                let _ = msg_tx.send(AppMessage::Error(format!("Failed to load manifest: {}", e)));
                grouped_files.push(vec![]);
                continue;
            }
        };

        let mut files_for_manifest = Vec::new();
        for entry in manifest.entries() {
            if !entry.is_alive() {
                continue;
            }
            let df = entry.data_file();
            files_for_manifest.push(DataFileInfo {
                file_path: df.file_path().to_string(),
                file_format: format!("{:?}", df.file_format()),
                record_count: df.record_count() as i64,
                file_size_bytes: df.file_size_in_bytes() as i64,
                null_value_counts: df
                    .null_value_counts()
                    .iter()
                    .map(|(&k, &v)| (k, v as i64))
                    .collect(),
                lower_bounds: df
                    .lower_bounds()
                    .iter()
                    .map(|(&k, v)| (k, v.to_string()))
                    .collect(),
                upper_bounds: df
                    .upper_bounds()
                    .iter()
                    .map(|(&k, v)| (k, v.to_string()))
                    .collect(),
                partition_data: std::collections::HashMap::new(),
            });
        }
        grouped_files.push(files_for_manifest);
    }

    let _ = msg_tx.send(AppMessage::ManifestsReady(manifest_infos));
    let _ = msg_tx.send(AppMessage::DataFileStatsReady(grouped_files));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::DEFAULT_PAGE_SIZE;

    #[test]
    fn app_new_default_state() {
        let app = App::new(None, None, DEFAULT_PAGE_SIZE);
        assert_eq!(app.active_tab, Tab::Data);
        assert_eq!(app.focus, Focus::Left);
        assert!(app.initial_columns.is_none());
        assert!(app.limit.is_none());
        assert_eq!(app.page_size, DEFAULT_PAGE_SIZE);
        assert!(!app.has_more);
        assert!(app.selected_snapshot_id.is_none());
        assert!(app.current_snapshot_id.is_none());
    }

    #[test]
    fn app_new_with_columns() {
        let cols = vec!["a".into(), "b".into()];
        let app = App::new(Some(cols.clone()), None, DEFAULT_PAGE_SIZE);
        assert_eq!(app.initial_columns, Some(cols));
    }

    #[test]
    fn app_new_with_limit() {
        let app = App::new(None, Some(500), 500);
        assert_eq!(app.limit, Some(500));
    }

    #[test]
    fn handle_key_quit() {
        let mut app = App::new(None, None, DEFAULT_PAGE_SIZE);
        let key = KeyEvent::from(KeyCode::Char('q'));
        assert_eq!(app.handle_key(key), Some(Action::Quit));
    }

    #[test]
    fn handle_key_help() {
        let mut app = App::new(None, None, DEFAULT_PAGE_SIZE);
        let key = KeyEvent::from(KeyCode::Char('?'));
        assert_eq!(app.handle_key(key), Some(Action::ToggleHelp));
    }

    #[test]
    fn handle_key_tab_switch() {
        let mut app = App::new(None, None, DEFAULT_PAGE_SIZE);
        for (ch, idx) in [('1', 0), ('2', 1), ('3', 2), ('4', 3), ('5', 4)] {
            let key = KeyEvent::from(KeyCode::Char(ch));
            assert_eq!(app.handle_key(key), Some(Action::SwitchTab(idx)));
        }
    }

    #[test]
    fn handle_key_reload() {
        let mut app = App::new(None, None, DEFAULT_PAGE_SIZE);
        let key = KeyEvent::from(KeyCode::Char('r'));
        assert_eq!(app.handle_key(key), Some(Action::Reload));
    }

    #[test]
    fn handle_key_increase_limit() {
        let mut app = App::new(None, None, DEFAULT_PAGE_SIZE);
        let key = KeyEvent::from(KeyCode::Char('m'));
        assert_eq!(app.handle_key(key), Some(Action::IncreaseLimit));
    }

    #[test]
    fn handle_key_focus_next_prev() {
        let mut app = App::new(None, None, DEFAULT_PAGE_SIZE);
        assert_eq!(
            app.handle_key(KeyEvent::from(KeyCode::Tab)),
            Some(Action::FocusNext)
        );
        assert_eq!(
            app.handle_key(KeyEvent::from(KeyCode::BackTab)),
            Some(Action::FocusPrev)
        );
    }

    #[test]
    fn handle_message_data_ready_updates_has_more() {
        let mut app = App::new(None, Some(500), 500);
        app.handle_message(&AppMessage::DataReady {
            batches: vec![],
            total_rows: 500,
            has_more: true,
        });
        assert!(app.has_more);
        assert_eq!(app.limit, Some(500));

        app.handle_message(&AppMessage::DataReady {
            batches: vec![],
            total_rows: 300,
            has_more: false,
        });
        assert!(!app.has_more);
        assert_eq!(app.limit, Some(300));
    }

    #[test]
    fn table_handle_static_starts_none() {
        let _handle = TABLE_HANDLE.lock().unwrap();
    }
}
