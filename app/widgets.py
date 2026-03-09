import json
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QComboBox, QLabel,
    QSpinBox, QGroupBox, QTreeWidget, QTreeWidgetItem,
    QTextEdit, QHeaderView, QSplitter,
)
from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtGui import QColor, QFont

from .models import Rule, SimulationResult


class TimeRangeWidget(QWidget):
    """Time range selector with presets and custom option."""

    changed = pyqtSignal(int)  # emits seconds

    PRESETS = [
        ("Rule Default", 0),
        ("Last 15 minutes", 900),
        ("Last 1 hour", 3600),
        ("Last 4 hours", 14400),
        ("Last 24 hours", 86400),
        ("Last 7 days", 604800),
        ("Last 30 days", 2592000),
        ("Custom", -1),
    ]

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.combo = QComboBox()
        for label, _ in self.PRESETS:
            self.combo.addItem(label)
        self.combo.currentIndexChanged.connect(self._on_preset_changed)
        layout.addWidget(self.combo)

        self.custom_value = QSpinBox()
        self.custom_value.setRange(1, 9999)
        self.custom_value.setValue(60)
        self.custom_value.setVisible(False)
        layout.addWidget(self.custom_value)

        self.custom_unit = QComboBox()
        self.custom_unit.addItems(["minutes", "hours", "days"])
        self.custom_unit.setVisible(False)
        layout.addWidget(self.custom_unit)

        self.custom_value.valueChanged.connect(self._emit_changed)
        self.custom_unit.currentIndexChanged.connect(self._emit_changed)

    def _on_preset_changed(self, index):
        is_custom = self.PRESETS[index][1] == -1
        self.custom_value.setVisible(is_custom)
        self.custom_unit.setVisible(is_custom)
        self._emit_changed()

    def _emit_changed(self):
        self.changed.emit(self.get_seconds())

    def get_seconds(self) -> int:
        """Returns selected time range in seconds. 0 means 'use rule default'."""
        index = self.combo.currentIndex()
        _, value = self.PRESETS[index]
        if value == -1:
            # Custom
            v = self.custom_value.value()
            unit = self.custom_unit.currentText()
            multipliers = {"minutes": 60, "hours": 3600, "days": 86400}
            return v * multipliers.get(unit, 60)
        return value


class RuleDetailWidget(QWidget):
    """Displays details about a selected rule."""

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.name_label = QLabel()
        self.name_label.setWordWrap(True)
        font = self.name_label.font()
        font.setPointSize(font.pointSize() + 2)
        font.setBold(True)
        self.name_label.setFont(font)
        layout.addWidget(self.name_label)

        self.info_text = QTextEdit()
        self.info_text.setReadOnly(True)
        self.info_text.setMaximumHeight(200)
        layout.addWidget(self.info_text)

    def set_rule(self, rule: Rule | None):
        if rule is None:
            self.name_label.setText("No rule selected")
            self.info_text.clear()
            return

        self.name_label.setText(rule.name)

        comparator, thresholds = rule.threshold_info
        threshold_str = (
            f"{thresholds[0]} and {thresholds[1]}"
            if len(thresholds) > 1
            else str(thresholds[0]) if thresholds else "N/A"
        )

        lines = [
            f"<b>Type:</b> {rule.display_type}",
            f"<b>Enabled:</b> {'Yes' if rule.enabled else 'No'}",
            f"<b>Schedule:</b> {rule.schedule.get('interval', 'N/A')}",
            f"<b>Threshold:</b> {comparator} {threshold_str}",
            f"<b>Indices:</b> {', '.join(rule.indices) if rule.indices else 'N/A'}",
            f"<b>Tags:</b> {', '.join(rule.tags) if rule.tags else 'None'}",
        ]

        # Show query/params summary
        params = rule.params
        if rule.rule_type == ".es-query":
            query = params.get("esQuery", "")
            if isinstance(query, str) and len(query) > 200:
                query = query[:200] + "..."
            elif isinstance(query, dict):
                query = json.dumps(query, indent=2)[:200] + "..."
            lines.append(f"<b>Query:</b><pre>{query}</pre>")
        elif rule.rule_type == ".index-threshold":
            agg = params.get("aggType", "count")
            agg_field = params.get("aggField", "")
            lines.append(f"<b>Aggregation:</b> {agg}({agg_field or '*'})")

        tw = params.get("timeWindowSize")
        tu = params.get("timeWindowUnit")
        if tw and tu:
            lines.append(f"<b>Time Window:</b> {tw} {tu}")

        self.info_text.setHtml("<br>".join(lines))


class SimulationResultWidget(QWidget):
    """Displays simulation results in a table."""

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.summary_label = QLabel("Run a simulation to see results.")
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)

        self.result_tree = QTreeWidget()
        self.result_tree.setHeaderLabels(["Host", "Matches", "Status"])
        self.result_tree.setAlternatingRowColors(True)
        self.result_tree.setRootIsDecorated(False)
        header = self.result_tree.header()
        header.setStretchLastSection(True)
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        layout.addWidget(self.result_tree)

        self.query_text = QTextEdit()
        self.query_text.setReadOnly(True)
        self.query_text.setMaximumHeight(120)
        self.query_text.setVisible(False)
        layout.addWidget(self.query_text)

    def set_result(self, result: SimulationResult):
        self.result_tree.clear()

        comparator, thresholds = result.comparator, result.threshold
        threshold_str = (
            f"{thresholds[0]} and {thresholds[1]}"
            if len(thresholds) > 1
            else str(thresholds[0]) if thresholds else "N/A"
        )

        if result.error:
            self.summary_label.setText(
                f"<b style='color: orange;'>Warning:</b> {result.error}"
            )
            if not result.device_results:
                return

        # Summary
        if result.fired:
            fired_count = sum(1 for d in result.device_results if d.fired)
            total_count = len(result.device_results)
            self.summary_label.setText(
                f"<b style='color: #e74c3c;'>WOULD FIRE</b> — "
                f"{fired_count} of {total_count} device(s) exceed threshold "
                f"({comparator} {threshold_str})<br>"
                f"<small>Time range: {result.time_range_start} to {result.time_range_end}</small>"
            )
        else:
            self.summary_label.setText(
                f"<b style='color: #27ae60;'>WOULD NOT FIRE</b> — "
                f"No devices exceed threshold ({comparator} {threshold_str})<br>"
                f"<small>Time range: {result.time_range_start} to {result.time_range_end}</small>"
            )

        # Device results
        for dr in result.device_results:
            item = QTreeWidgetItem([
                dr.host_name,
                f"{dr.match_count:,.0f}" if dr.match_count == int(dr.match_count) else f"{dr.match_count:,.2f}",
                "FIRE" if dr.fired else "OK",
            ])
            if dr.fired:
                for col in range(3):
                    item.setForeground(col, QColor("#e74c3c"))
                item.setFont(2, QFont("", -1, QFont.Weight.Bold))
            else:
                item.setForeground(2, QColor("#27ae60"))
            self.result_tree.addTopLevelItem(item)

        # Query used
        if result.query_used:
            self.query_text.setVisible(True)
            self.query_text.setPlainText(json.dumps(result.query_used, indent=2))
        else:
            self.query_text.setVisible(False)

    def clear_results(self):
        self.result_tree.clear()
        self.summary_label.setText("Run a simulation to see results.")
        self.query_text.setVisible(False)
