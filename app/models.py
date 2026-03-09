from dataclasses import dataclass, field


@dataclass
class ConnectionConfig:
    method: str  # "cloud_id", "url_basic", "url_apikey"
    cloud_id: str | None = None
    url: str | None = None
    api_key: str | None = None
    username: str | None = None
    password: str | None = None
    kibana_url_override: str | None = None

    @property
    def kibana_url(self) -> str | None:
        if self.kibana_url_override:
            return self.kibana_url_override.rstrip("/")
        if self.url:
            return self.url.rstrip("/")
        return None


@dataclass
class Rule:
    id: str
    name: str
    rule_type: str
    enabled: bool
    schedule: dict
    params: dict
    tags: list[str] = field(default_factory=list)
    consumer: str = ""
    actions: list[dict] = field(default_factory=list)

    @property
    def interval_seconds(self) -> int:
        interval = self.schedule.get("interval", "1m")
        value = int(interval[:-1])
        unit = interval[-1]
        multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
        return value * multipliers.get(unit, 60)

    @property
    def display_type(self) -> str:
        type_map = {
            ".es-query": "ES Query",
            ".index-threshold": "Index Threshold",
            "xpack.ml.anomaly_detection_alert": "ML Anomaly",
            "metrics.alert.threshold": "Metrics Threshold",
            "metrics.alert.inventory.threshold": "Inventory Threshold",
            "logs.alert.document.count": "Log Document Count",
            "siem.queryRule": "SIEM Query",
            "siem.eqlRule": "SIEM EQL",
        }
        return type_map.get(self.rule_type, self.rule_type)

    # Default indices for rule types that don't store index in params
    DEFAULT_INDICES = {
        "metrics.alert.threshold": ["metrics-*", "metricbeat-*"],
        "metrics.alert.inventory.threshold": ["metrics-*", "metricbeat-*"],
        "logs.alert.document.count": ["logs-*", "filebeat-*"],
    }

    @property
    def indices(self) -> list[str]:
        params = self.params
        if "index" in params:
            idx = params["index"]
            return idx if isinstance(idx, list) else [idx]
        if "searchConfiguration" in params:
            sc = params["searchConfiguration"]
            if "index" in sc:
                return [sc["index"]] if isinstance(sc["index"], str) else [sc["index"]]
        # Fallback defaults for rule types that use source config instead of explicit index
        return self.DEFAULT_INDICES.get(self.rule_type, [])

    @property
    def criteria(self) -> list[dict]:
        """Extract criteria array for metrics/logs rules."""
        return self.params.get("criteria", [])

    @property
    def threshold_info(self) -> tuple[str, list[float]]:
        # For metrics.alert.threshold, threshold lives inside criteria
        if self.rule_type in ("metrics.alert.threshold", "logs.alert.document.count"):
            criteria = self.criteria
            if criteria:
                c = criteria[0]
                comparator = c.get("comparator", ">")
                threshold = c.get("threshold", [0])
                if not isinstance(threshold, list):
                    threshold = [threshold]
                return comparator, threshold

        comparator = self.params.get("thresholdComparator", ">")
        threshold = self.params.get("threshold", [0])
        if not isinstance(threshold, list):
            threshold = [threshold]
        return comparator, threshold

    @property
    def time_window_seconds(self) -> int | None:
        # Standard fields
        size = self.params.get("timeWindowSize")
        unit = self.params.get("timeWindowUnit")
        if size is not None and unit is not None:
            size = int(size)
            multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
            return size * multipliers.get(unit, 60)

        # For metrics.alert.threshold, time window is in criteria
        if self.rule_type == "metrics.alert.threshold":
            criteria = self.criteria
            if criteria:
                c = criteria[0]
                ts = c.get("timeSize")
                tu = c.get("timeUnit")
                if ts is not None and tu is not None:
                    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
                    return int(ts) * multipliers.get(tu, 60)

        return None


@dataclass
class DeviceResult:
    host_name: str
    match_count: float
    fired: bool


@dataclass
class SimulationResult:
    rule: Rule
    fired: bool
    total_match_count: float
    threshold: list[float]
    comparator: str
    device_results: list[DeviceResult] = field(default_factory=list)
    time_range_start: str = ""
    time_range_end: str = ""
    query_used: dict = field(default_factory=dict)
    error: str | None = None
