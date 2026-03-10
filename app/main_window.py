import json
from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QSplitter,
    QComboBox, QLabel, QPushButton, QListWidget, QListWidgetItem,
    QLineEdit, QGroupBox, QStatusBar, QMessageBox, QProgressBar,
    QApplication, QFormLayout,
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QSize
from PyQt6.QtGui import QColor, QIcon

from .client import ElasticKibanaClient
from .simulator import RuleSimulator
from .models import Rule, SimulationResult, SimulationOverrides, ConnectionConfig
from .connection_dialog import ConnectionDialog
from .widgets import (
    TimeRangeWidget, RuleDetailWidget, SimulationResultWidget,
    DevicePickerWidget, IndicesPickerWidget,
)


class WorkerThread(QThread):
    """Generic worker thread for background operations."""
    finished = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        try:
            result = self.func(*self.args, **self.kwargs)
            self.finished.emit(result)
        except Exception as e:
            self.error.emit(str(e))


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.client = ElasticKibanaClient()
        self.simulator = RuleSimulator(self.client)
        self.rules: list[Rule] = []
        self.filtered_rules: list[Rule] = []
        self._worker: WorkerThread | None = None

        self.setWindowTitle("Kibana Alert Simulator")
        self.setMinimumSize(1100, 700)
        self.resize(1300, 800)

        self._setup_ui()
        self._setup_statusbar()
        self._update_ui_state()

    def _setup_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)

        # ── Top bar: Space selector + Connection ──
        top_bar = QHBoxLayout()

        top_bar.addWidget(QLabel("Space:"))
        self.space_combo = QComboBox()
        self.space_combo.setMinimumWidth(200)
        self.space_combo.currentIndexChanged.connect(self._on_space_changed)
        top_bar.addWidget(self.space_combo)

        self.refresh_btn = QPushButton("Refresh Rules")
        self.refresh_btn.clicked.connect(self._load_rules)
        top_bar.addWidget(self.refresh_btn)

        top_bar.addStretch()

        self.connection_label = QLabel("Not connected")
        top_bar.addWidget(self.connection_label)

        self.connect_btn = QPushButton("Connect...")
        self.connect_btn.clicked.connect(self._show_connect_dialog)
        top_bar.addWidget(self.connect_btn)

        main_layout.addLayout(top_bar)

        # ── Main content: splitter with rule list + detail panel ──
        splitter = QSplitter(Qt.Orientation.Horizontal)

        # Left panel: Rule list + Data source
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)

        # Data source picker
        ds_group = QGroupBox("Data Source")
        ds_layout = QVBoxLayout(ds_group)
        self.indices_picker = IndicesPickerWidget()
        self.indices_picker.selectionChanged.connect(self._on_indices_selection_changed)
        ds_layout.addWidget(self.indices_picker)
        left_layout.addWidget(ds_group)

        rules_group = QGroupBox("Alerting Rules")
        rules_layout = QVBoxLayout(rules_group)

        self.rule_filter = QLineEdit()
        self.rule_filter.setPlaceholderText("Filter rules...")
        self.rule_filter.textChanged.connect(self._filter_rules)
        rules_layout.addWidget(self.rule_filter)

        self.rule_list = QListWidget()
        self.rule_list.currentRowChanged.connect(self._on_rule_selected)
        rules_layout.addWidget(self.rule_list)

        self.rule_count_label = QLabel("0 rules")
        rules_layout.addWidget(self.rule_count_label)

        left_layout.addWidget(rules_group)
        splitter.addWidget(left_panel)

        # Right panel: Rule detail + simulation
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)

        # Rule details
        self.rule_detail = RuleDetailWidget()
        right_layout.addWidget(self.rule_detail)

        # Simulation config
        sim_group = QGroupBox("Simulation")
        sim_layout = QFormLayout(sim_group)

        self.device_picker = DevicePickerWidget()
        sim_layout.addRow("Device (host.name):", self.device_picker)

        self.time_range = TimeRangeWidget()
        sim_layout.addRow("Time Range:", self.time_range)

        # ── What-If Overrides ──
        self.override_toggle = QPushButton("▶ What-If Overrides")
        self.override_toggle.setCheckable(True)
        self.override_toggle.setStyleSheet(
            "QPushButton { text-align: left; border: none; font-weight: bold; "
            "background: transparent; padding: 4px 0; }"
        )
        self.override_toggle.toggled.connect(self._toggle_overrides)
        sim_layout.addRow(self.override_toggle)

        self.override_container = QWidget()
        override_layout = QFormLayout(self.override_container)
        override_layout.setContentsMargins(10, 0, 0, 0)

        self.override_comparator = QComboBox()
        self.override_comparator.addItems(["(use rule default)", ">", ">=", "<", "<=", "between", "notBetween"])
        override_layout.addRow("Comparator:", self.override_comparator)

        self.override_threshold = QLineEdit()
        self.override_threshold.setPlaceholderText("e.g. 100 or 50,200 for between (blank = rule default)")
        override_layout.addRow("Threshold:", self.override_threshold)

        self.override_filter = QLineEdit()
        self.override_filter.setPlaceholderText("KQL filter override (blank = rule default)")
        override_layout.addRow("Filter (KQL):", self.override_filter)

        self.override_reset_btn = QPushButton("Reset Overrides")
        self.override_reset_btn.clicked.connect(self._reset_overrides)
        override_layout.addRow(self.override_reset_btn)

        self.override_container.setVisible(False)
        sim_layout.addRow(self.override_container)

        sim_btn_layout = QHBoxLayout()
        self.simulate_btn = QPushButton("Simulate")
        self.simulate_btn.setMinimumHeight(35)
        self.simulate_btn.setStyleSheet(
            "QPushButton { background-color: #3498db; color: white; font-weight: bold; "
            "border-radius: 4px; padding: 5px 20px; }"
            "QPushButton:hover { background-color: #2980b9; }"
            "QPushButton:disabled { background-color: #bdc3c7; }"
        )
        self.simulate_btn.clicked.connect(self._run_simulation)
        sim_btn_layout.addWidget(self.simulate_btn)

        self.simulate_all_btn = QPushButton("Simulate All Rules")
        self.simulate_all_btn.setMinimumHeight(35)
        self.simulate_all_btn.setStyleSheet(
            "QPushButton { background-color: #8e44ad; color: white; font-weight: bold; "
            "border-radius: 4px; padding: 5px 20px; }"
            "QPushButton:hover { background-color: #7d3c98; }"
            "QPushButton:disabled { background-color: #bdc3c7; }"
        )
        self.simulate_all_btn.clicked.connect(self._run_simulation_all_rules)
        sim_btn_layout.addWidget(self.simulate_all_btn)

        sim_layout.addRow(sim_btn_layout)
        right_layout.addWidget(sim_group)

        # Results
        results_group = QGroupBox("Results")
        results_layout = QVBoxLayout(results_group)
        self.result_widget = SimulationResultWidget()
        results_layout.addWidget(self.result_widget)
        right_layout.addWidget(results_group)

        splitter.addWidget(right_panel)
        splitter.setSizes([350, 750])

        main_layout.addWidget(splitter)

    def _setup_statusbar(self):
        self.statusbar = QStatusBar()
        self.setStatusBar(self.statusbar)
        self.progress_bar = QProgressBar()
        self.progress_bar.setMaximumWidth(200)
        self.progress_bar.setVisible(False)
        self.statusbar.addPermanentWidget(self.progress_bar)

    def _update_ui_state(self):
        connected = self.client.is_connected
        self.space_combo.setEnabled(connected)
        self.refresh_btn.setEnabled(connected)
        self.rule_filter.setEnabled(connected)
        self.simulate_btn.setEnabled(connected and self.rule_list.currentRow() >= 0)
        self.simulate_all_btn.setEnabled(connected and len(self.filtered_rules) > 0)
        self.device_picker.setEnabled(connected)
        self.indices_picker.setEnabled(connected)

        if connected:
            self.connection_label.setText(
                f"Connected: {self.client.cluster_name} (v{self.client.cluster_version})"
            )
            self.connection_label.setStyleSheet("color: green; font-weight: bold;")
            self.connect_btn.setText("Reconnect...")
        else:
            self.connection_label.setText("Not connected")
            self.connection_label.setStyleSheet("color: gray;")
            self.connect_btn.setText("Connect...")

    def _show_busy(self, busy: bool, message: str = ""):
        self.progress_bar.setVisible(busy)
        if busy:
            self.progress_bar.setRange(0, 0)  # indeterminate
            self.statusbar.showMessage(message)
        else:
            self.statusbar.clearMessage()

    # ── Connection ──────────────────────────────────────────────

    def _show_connect_dialog(self):
        if self.client.is_connected:
            self.client.disconnect()

        dialog = ConnectionDialog(self.client, self)
        if dialog.exec():
            self._update_ui_state()
            self._load_spaces()

    # ── Spaces ──────────────────────────────────────────────────

    def _load_spaces(self):
        self._show_busy(True, "Loading spaces...")

        def fetch():
            return self.client.get_spaces()

        self._worker = WorkerThread(fetch)
        self._worker.finished.connect(self._on_spaces_loaded)
        self._worker.error.connect(self._on_error)
        self._worker.start()

    def _on_spaces_loaded(self, spaces):
        self._show_busy(False)
        self.space_combo.blockSignals(True)
        self.space_combo.clear()
        for space in spaces:
            self.space_combo.addItem(
                f"{space['name']} ({space['id']})",
                space["id"],
            )
        self.space_combo.blockSignals(False)

        # Load available indices/data views for the data source picker
        self._load_available_indices()

        if self.space_combo.count() > 0:
            self._on_space_changed(0)

    def _on_space_changed(self, index):
        if index >= 0:
            self._load_rules()

    # ── Data Source (Indices) ───────────────────────────────────

    def _load_available_indices(self):
        """Fetch all indices/data streams for the data source picker."""
        def fetch():
            return self.client.get_indices()

        self._worker = WorkerThread(fetch)
        self._worker.finished.connect(self._on_available_indices_loaded)
        self._worker.error.connect(lambda msg: None)  # silent fail
        self._worker.start()

    def _on_available_indices_loaded(self, indices):
        self.indices_picker.set_indices(indices)

    def _on_indices_selection_changed(self):
        """When the user changes the data source selection, reload devices."""
        selected = self.indices_picker.selected_indices()
        if not selected:
            # No selection — devices will load from the rule's indices when a rule is selected
            rule = self._get_selected_rule()
            if rule:
                self._load_devices_for_rule(rule)
            else:
                self.device_picker.set_hosts([])
            return

        # Load hosts from the selected indices
        time_seconds = self.time_range.get_seconds()
        if time_seconds == 0:
            time_seconds = 86400  # default 24h for data source browsing

        self._show_busy(True, "Loading devices from selected indices...")

        def fetch():
            return self.client.get_hosts(selected, time_seconds)

        self._worker = WorkerThread(fetch)
        self._worker.finished.connect(self._on_devices_loaded)
        self._worker.error.connect(self._on_error)
        self._worker.start()

    def _get_active_indices(self) -> list[str] | None:
        """Return selected indices from the data source picker, or None to use rule defaults."""
        selected = self.indices_picker.selected_indices()
        return selected if selected else None

    # ── Rules ───────────────────────────────────────────────────

    def _load_rules(self):
        space_id = self.space_combo.currentData()
        if not space_id:
            return

        self._show_busy(True, f"Loading rules from space '{space_id}'...")

        def fetch():
            return self.client.get_rules(space_id)

        self._worker = WorkerThread(fetch)
        self._worker.finished.connect(self._on_rules_loaded)
        self._worker.error.connect(self._on_error)
        self._worker.start()

    def _on_rules_loaded(self, rules):
        self._show_busy(False)
        self.rules = rules
        self._filter_rules()
        self.rule_count_label.setText(f"{len(rules)} rules")
        self._update_ui_state()

    def _filter_rules(self):
        text = self.rule_filter.text().lower()
        self.filtered_rules = [
            r for r in self.rules
            if not text or text in r.name.lower() or text in r.rule_type.lower()
            or any(text in t.lower() for t in r.tags)
        ]

        self.rule_list.blockSignals(True)
        self.rule_list.clear()
        for rule in self.filtered_rules:
            if rule.enabled:
                status_text = "ENABLED"
                item = QListWidgetItem(f"[{status_text}] {rule.name} ({rule.display_type})")
                item.setForeground(QColor("#27ae60"))
            else:
                status_text = "DISABLED"
                item = QListWidgetItem(f"[{status_text}] {rule.name} ({rule.display_type})")
                item.setForeground(QColor("#e74c3c"))
            self.rule_list.addItem(item)
        self.rule_list.blockSignals(False)

        self.rule_count_label.setText(
            f"{len(self.filtered_rules)} of {len(self.rules)} rules"
        )
        self._update_ui_state()

    def _on_rule_selected(self, row):
        if 0 <= row < len(self.filtered_rules):
            rule = self.filtered_rules[row]
            self.rule_detail.set_rule(rule)
            self._populate_override_placeholders(rule)
            self._load_devices_for_rule(rule)
        else:
            self.rule_detail.set_rule(None)
        self._update_ui_state()

    def _populate_override_placeholders(self, rule: Rule):
        """Update placeholder text to show the rule's current values."""
        comparator, thresholds = rule.threshold_info
        thresh_str = ", ".join(str(t) for t in thresholds)
        self.override_threshold.setPlaceholderText(
            f"Current: {thresh_str} (blank = rule default)"
        )
        # Show current filter if any
        fq = rule.params.get("filterQueryText", "")
        if rule.rule_type == ".es-query":
            fq = "(see rule query)"
        self.override_filter.setPlaceholderText(
            f"Current: {fq}" if fq else "KQL filter override (blank = rule default)"
        )

    # ── Devices ─────────────────────────────────────────────────

    def _load_devices_for_rule(self, rule: Rule):
        # Use global data source if selected, otherwise fall back to rule's indices
        global_indices = self._get_active_indices()
        indices = global_indices or rule.indices
        if not indices:
            return

        time_seconds = self.time_range.get_seconds()
        if time_seconds == 0:
            time_seconds = rule.time_window_seconds or rule.interval_seconds

        self._show_busy(True, "Loading devices...")

        def fetch():
            return self.client.get_hosts(indices, time_seconds)

        self._worker = WorkerThread(fetch)
        self._worker.finished.connect(self._on_devices_loaded)
        self._worker.error.connect(self._on_error)
        self._worker.start()

    def _on_devices_loaded(self, hosts):
        self._show_busy(False)
        self.device_picker.set_hosts(hosts)
        self.statusbar.showMessage(f"{len(hosts)} devices found", 3000)

    # ── What-If Overrides ───────────────────────────────────────

    def _toggle_overrides(self, checked: bool):
        self.override_container.setVisible(checked)
        self.override_toggle.setText(
            "▼ What-If Overrides" if checked else "▶ What-If Overrides"
        )

    def _reset_overrides(self):
        self.override_comparator.setCurrentIndex(0)
        self.override_threshold.clear()
        self.override_filter.clear()

    def _get_overrides(self) -> SimulationOverrides | None:
        """Build SimulationOverrides from the UI fields, or None if nothing is set."""
        comparator = None
        threshold = None
        filter_query = None

        if self.override_comparator.currentIndex() > 0:
            comparator = self.override_comparator.currentText()

        thresh_text = self.override_threshold.text().strip()
        if thresh_text:
            try:
                parts = [float(v.strip()) for v in thresh_text.split(",")]
                threshold = parts
            except ValueError:
                pass  # ignore bad input, use rule default

        filter_text = self.override_filter.text().strip()
        if filter_text:
            filter_query = filter_text

        if comparator is None and threshold is None and filter_query is None:
            return None

        return SimulationOverrides(
            threshold=threshold,
            comparator=comparator,
            filter_query=filter_query,
        )

    # ── Simulation ──────────────────────────────────────────────

    def _get_selected_rule(self) -> Rule | None:
        row = self.rule_list.currentRow()
        if 0 <= row < len(self.filtered_rules):
            return self.filtered_rules[row]
        return None

    def _run_simulation(self):
        rule = self._get_selected_rule()
        if not rule:
            QMessageBox.warning(self, "No Rule", "Please select a rule first.")
            return

        selected_hosts = self.device_picker.selected_hosts()
        indices_override = self._get_active_indices()

        time_seconds = self.time_range.get_seconds()
        if time_seconds == 0:
            time_seconds = None  # Use rule default

        self._show_busy(True, f"Simulating '{rule.name}'...")
        self.simulate_btn.setEnabled(False)
        self.simulate_all_btn.setEnabled(False)

        overrides = self._get_overrides()

        def run():
            if not selected_hosts:
                return self.simulator.simulate(rule, None, time_seconds, overrides, indices_override)
            elif len(selected_hosts) == 1:
                return self.simulator.simulate(rule, selected_hosts[0], time_seconds, overrides, indices_override)
            else:
                all_device_results = []
                any_fired = False
                total = 0.0
                last_result = None
                for host in selected_hosts:
                    r = self.simulator.simulate(rule, host, time_seconds, overrides, indices_override)
                    all_device_results.extend(r.device_results)
                    if r.fired:
                        any_fired = True
                    total += r.total_match_count
                    last_result = r
                all_device_results.sort(key=lambda d: (-int(d.fired), -d.match_count))
                return SimulationResult(
                    rule=rule, fired=any_fired, total_match_count=total,
                    threshold=last_result.threshold, comparator=last_result.comparator,
                    device_results=all_device_results,
                    time_range_start=last_result.time_range_start,
                    time_range_end=last_result.time_range_end,
                    query_used=last_result.query_used,
                    has_overrides=last_result.has_overrides,
                )

        self._worker = WorkerThread(run)
        self._worker.finished.connect(self._on_simulation_done)
        self._worker.error.connect(self._on_simulation_error)
        self._worker.start()

    def _run_simulation_all_rules(self):
        if not self.filtered_rules:
            return

        selected_hosts = self.device_picker.selected_hosts()
        indices_override = self._get_active_indices()

        time_seconds = self.time_range.get_seconds()
        if time_seconds == 0:
            time_seconds = None

        rules_to_sim = list(self.filtered_rules)
        self._show_busy(True, f"Simulating {len(rules_to_sim)} rules...")
        self.simulate_btn.setEnabled(False)
        self.simulate_all_btn.setEnabled(False)

        overrides = self._get_overrides()

        def run():
            results = []
            for rule in rules_to_sim:
                try:
                    if not selected_hosts:
                        result = self.simulator.simulate(rule, None, time_seconds, overrides, indices_override)
                    elif len(selected_hosts) == 1:
                        result = self.simulator.simulate(rule, selected_hosts[0], time_seconds, overrides, indices_override)
                    else:
                        all_dr = []
                        any_fired = False
                        total = 0.0
                        last = None
                        for host in selected_hosts:
                            r = self.simulator.simulate(rule, host, time_seconds, overrides, indices_override)
                            all_dr.extend(r.device_results)
                            if r.fired:
                                any_fired = True
                            total += r.total_match_count
                            last = r
                        all_dr.sort(key=lambda d: (-int(d.fired), -d.match_count))
                        result = SimulationResult(
                            rule=rule, fired=any_fired, total_match_count=total,
                            threshold=last.threshold, comparator=last.comparator,
                            device_results=all_dr,
                            time_range_start=last.time_range_start,
                            time_range_end=last.time_range_end,
                            query_used=last.query_used,
                            has_overrides=last.has_overrides,
                        )
                    results.append(result)
                except Exception as e:
                    from .models import SimulationResult as SR
                    results.append(SR(
                        rule=rule, fired=False, total_match_count=0,
                        threshold=[0], comparator=">", error=str(e),
                    ))
            return results

        self._worker = WorkerThread(run)
        self._worker.finished.connect(self._on_simulation_all_done)
        self._worker.error.connect(self._on_simulation_error)
        self._worker.start()

    def _on_simulation_done(self, result: SimulationResult):
        self._show_busy(False)
        self.simulate_btn.setEnabled(True)
        self.simulate_all_btn.setEnabled(True)
        self.result_widget.set_result(result)

        overrides = self._get_overrides()
        override_tag = " [WHAT-IF]" if overrides else ""
        status = "WOULD FIRE" if result.fired else "OK"
        self.statusbar.showMessage(
            f"Simulation complete{override_tag}: {result.rule.name} — {status}", 5000
        )

    def _on_simulation_all_done(self, results: list[SimulationResult]):
        self._show_busy(False)
        self.simulate_btn.setEnabled(True)
        self.simulate_all_btn.setEnabled(True)

        # Build a combined result showing all rules
        fired_count = sum(1 for r in results if r.fired)
        total = len(results)

        # Create paired list of (DeviceResult, SimulationResult) for sorting
        from .models import DeviceResult
        paired = []
        for r in results:
            dr = DeviceResult(
                host_name=f"{r.rule.name} ({r.rule.display_type})",
                match_count=r.total_match_count,
                fired=r.fired,
            )
            paired.append((dr, r))

        # Sort: fired first, then by match count descending
        paired.sort(key=lambda p: (-int(p[0].fired), -p[0].match_count))

        device_results = [p[0] for p in paired]
        sorted_results = [p[1] for p in paired]

        # Create a summary result
        summary = SimulationResult(
            rule=results[0].rule if results else self.filtered_rules[0],
            fired=fired_count > 0,
            total_match_count=sum(r.total_match_count for r in results),
            threshold=[0],
            comparator=">",
            device_results=device_results,
            time_range_start=results[0].time_range_start if results else "",
            time_range_end=results[0].time_range_end if results else "",
        )

        self.result_widget.set_result(summary)
        self.result_widget.set_all_rules_results(sorted_results)
        self.result_widget.summary_label.setText(
            f"<b>All Rules Simulation:</b> "
            f"<span style='color: #e74c3c;'>{fired_count} WOULD FIRE</span> / "
            f"<span style='color: #27ae60;'>{total - fired_count} OK</span> "
            f"out of {total} rules &nbsp; "
            f"<small>(double-click a row for details)</small>"
        )

        self.statusbar.showMessage(
            f"Simulated {total} rules: {fired_count} would fire", 5000
        )

    def _on_simulation_error(self, msg):
        self._show_busy(False)
        self.simulate_btn.setEnabled(True)
        self.simulate_all_btn.setEnabled(True)
        QMessageBox.critical(self, "Simulation Error", msg)

    # ── Error handling ──────────────────────────────────────────

    def _on_error(self, msg):
        self._show_busy(False)
        QMessageBox.critical(self, "Error", msg)

    def closeEvent(self, event):
        self.client.disconnect()
        super().closeEvent(event)
