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
        self.info_text.setMaximumHeight(300)
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

        params = rule.params

        lines = [
            f"<b>Type:</b> {rule.display_type}",
            f"<b>Enabled:</b> {'Yes' if rule.enabled else 'No'}",
            f"<b>Schedule:</b> {rule.schedule.get('interval', 'N/A')}",
            f"<b>Threshold:</b> {comparator} {threshold_str}",
            f"<b>Indices:</b> {', '.join(rule.indices) if rule.indices else 'N/A'}",
            f"<b>Tags:</b> {', '.join(rule.tags) if rule.tags else 'None'}",
        ]

        # ── Type-specific details ──

        if rule.rule_type == ".es-query":
            search_type = params.get("searchType", "esQuery")
            lines.append(f"<b>Search Type:</b> {search_type}")
            query = params.get("esQuery", "")
            if isinstance(query, str) and len(query) > 300:
                query = query[:300] + "..."
            elif isinstance(query, dict):
                query = json.dumps(query, indent=2)[:300] + "..."
            lines.append(f"<b>Query:</b><pre>{query}</pre>")
            size = params.get("size", "")
            if size:
                lines.append(f"<b>Size:</b> {size}")

        elif rule.rule_type == ".index-threshold":
            agg = params.get("aggType", "count")
            agg_field = params.get("aggField", "")
            group_by = params.get("groupBy", "all")
            term_field = params.get("termField", "")
            term_size = params.get("termSize", "")
            lines.append(f"<b>Aggregation:</b> {agg}({agg_field or '*'})")
            lines.append(f"<b>Group By:</b> {group_by}")
            if term_field:
                lines.append(f"<b>Term Field:</b> {term_field} (top {term_size})")

        elif rule.rule_type == "metrics.alert.threshold":
            criteria = rule.criteria
            for i, c in enumerate(criteria):
                metric = c.get("metric", "custom")
                agg = c.get("aggType", "avg")
                cmp = c.get("comparator", ">")
                thresh = c.get("threshold", [])
                thresh_s = ", ".join(str(t) for t in thresh) if isinstance(thresh, list) else str(thresh)
                ts = c.get("timeSize", "")
                tu = c.get("timeUnit", "")
                time_s = f" over {ts}{tu}" if ts else ""
                lines.append(
                    f"<b>Criterion {i+1}:</b> {agg}({metric}) {cmp} {thresh_s}{time_s}"
                )
                if metric == "custom":
                    custom = c.get("customMetrics", [])
                    for cm in custom:
                        lines.append(
                            f"&nbsp;&nbsp;Custom: {cm.get('aggType', '?')}({cm.get('field', '?')})"
                        )
            filter_text = params.get("filterQueryText", "")
            if filter_text:
                lines.append(f"<b>KQL Filter:</b> <code>{filter_text}</code>")
            else:
                lines.append(f"<b>KQL Filter:</b> <i>(none)</i>")
            filter_kql = params.get("filterQuery", "")
            if filter_kql and filter_kql != filter_text:
                lines.append(f"<b>Filter Query:</b> <code>{filter_kql}</code>")
            group_by = params.get("groupBy", [])
            if group_by:
                lines.append(f"<b>Group By:</b> {', '.join(group_by)}")
            alert_on_no_data = params.get("alertOnNoData", False)
            alert_on_group_disappear = params.get("alertOnGroupDisappear", False)
            if alert_on_no_data:
                lines.append("<b>Alert on no data:</b> Yes")
            if alert_on_group_disappear:
                lines.append("<b>Alert on group disappear:</b> Yes")

        elif rule.rule_type == "logs.alert.document.count":
            criteria = rule.criteria
            for i, c in enumerate(criteria):
                field = c.get("field", "")
                cmp = c.get("comparator", "")
                val = c.get("value", "")
                lines.append(f"<b>Criterion {i+1}:</b> {field} {cmp} {val}")
            count_params = params.get("count", {})
            if count_params:
                cmp = count_params.get("comparator", "")
                val = count_params.get("value", "")
                lines.append(f"<b>Count:</b> {cmp} {val}")

        elif rule.rule_type == "metrics.alert.inventory.threshold":
            criteria = rule.criteria
            for i, c in enumerate(criteria):
                metric = c.get("metric", "")
                cmp = c.get("comparator", "")
                thresh = c.get("threshold", "")
                lines.append(f"<b>Criterion {i+1}:</b> {metric} {cmp} {thresh}")
            node_type = params.get("nodeType", "")
            if node_type:
                lines.append(f"<b>Node Type:</b> {node_type}")

        else:
            # Generic: dump key params
            skip_keys = {"index", "threshold", "thresholdComparator"}
            for k, v in params.items():
                if k in skip_keys:
                    continue
                val_str = str(v)
                if len(val_str) > 120:
                    val_str = val_str[:120] + "..."
                lines.append(f"<b>{k}:</b> {val_str}")

        # ── Time window ──
        tw = params.get("timeWindowSize")
        tu = params.get("timeWindowUnit")
        if tw and tu:
            lines.append(f"<b>Time Window:</b> {tw} {tu}")
        elif rule.time_window_seconds:
            mins = rule.time_window_seconds // 60
            lines.append(f"<b>Time Window:</b> {mins} minutes")

        # ── Actions summary ──
        if rule.actions:
            action_types = [a.get("actionTypeId", a.get("group", "?")) for a in rule.actions]
            lines.append(f"<b>Actions:</b> {len(rule.actions)} ({', '.join(action_types)})")

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
        override_banner = (
            "<b style='color: #f39c12;'>⚡ WHAT-IF MODE</b> — "
            "using overridden threshold/filter<br>"
        ) if result.has_overrides else ""

        if result.fired:
            fired_count = sum(1 for d in result.device_results if d.fired)
            total_count = len(result.device_results)
            self.summary_label.setText(
                f"{override_banner}"
                f"<b style='color: #e74c3c;'>WOULD FIRE</b> — "
                f"{fired_count} of {total_count} device(s) exceed threshold "
                f"({comparator} {threshold_str})<br>"
                f"<small>Time range: {result.time_range_start} to {result.time_range_end}</small>"
            )
        else:
            self.summary_label.setText(
                f"{override_banner}"
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
