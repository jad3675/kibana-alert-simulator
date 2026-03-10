import json
from .models import Rule, SimulationResult, SimulationOverrides, DeviceResult
from .client import ElasticKibanaClient


COMPARATORS = {
    ">": lambda v, t: v > t,
    ">=": lambda v, t: v >= t,
    "<": lambda v, t: v < t,
    "<=": lambda v, t: v <= t,
    "between": lambda v, t: len(t) >= 2 and t[0] <= v <= t[1],
    "notBetween": lambda v, t: len(t) >= 2 and (v < t[0] or v > t[1]),
}


class RuleSimulator:
    """Simulates Kibana alerting rules against live Elasticsearch data."""

    def __init__(self, client: ElasticKibanaClient):
        self.client = client

    def _resolve_overrides(self, rule: Rule, overrides: SimulationOverrides | None):
        """Return (comparator, thresholds, filter_query) with overrides applied."""
        comparator, thresholds = rule.threshold_info
        filter_query = None

        if overrides:
            if overrides.comparator is not None:
                comparator = overrides.comparator
            if overrides.threshold is not None:
                thresholds = overrides.threshold
            if overrides.filter_query is not None:
                filter_query = overrides.filter_query

        return comparator, thresholds, filter_query

    def _build_filter_query(self, filter_text: str) -> dict:
        """Convert a KQL/query_string filter into an ES query clause."""
        if filter_text.strip():
            return {"query_string": {"query": filter_text}}
        return {}

    def _merge_filter(self, base_query: dict, filter_override: str | None) -> dict:
        """If a filter override is set, combine it with the base query."""
        if filter_override is None:
            return base_query
        override_clause = self._build_filter_query(filter_override)
        if not override_clause:
            return base_query
        if not base_query:
            return override_clause
        # Combine both with bool/must
        return {"bool": {"must": [base_query, override_clause]}}

    def simulate(
        self,
        rule: Rule,
        host_name: str | None = None,
        time_range_seconds: int | None = None,
        overrides: SimulationOverrides | None = None,
        indices_override: list[str] | None = None,
    ) -> SimulationResult:
        """
        Simulate a rule. If host_name is None, simulates against all devices.
        Overrides allow what-if testing without modifying the real rule.
        indices_override replaces the rule's indices with a global data source.
        """
        if time_range_seconds is None:
            time_range_seconds = rule.time_window_seconds or rule.interval_seconds

        rule_type = rule.rule_type
        idx = indices_override  # shorthand, passed through to sub-methods

        if rule_type == ".es-query":
            result = self._simulate_es_query(rule, host_name, time_range_seconds, overrides, idx)
        elif rule_type == ".index-threshold":
            result = self._simulate_index_threshold(rule, host_name, time_range_seconds, overrides, idx)
        elif rule_type == "metrics.alert.threshold":
            result = self._simulate_metrics_threshold(rule, host_name, time_range_seconds, overrides, idx)
        elif rule_type == "logs.alert.document.count":
            result = self._simulate_logs_document_count(rule, host_name, time_range_seconds, overrides, idx)
        else:
            result = self._simulate_generic(rule, host_name, time_range_seconds, overrides, idx)

        if overrides is not None:
            result.has_overrides = True
        return result

    def _simulate_es_query(
        self, rule: Rule, host_name: str | None, time_range_seconds: int,
        overrides: SimulationOverrides | None = None,
        indices_override: list[str] | None = None,
    ) -> SimulationResult:
        params = rule.params
        comparator, thresholds, filter_override = self._resolve_overrides(rule, overrides)
        indices = indices_override or rule.indices

        # Extract the ES query from the rule
        es_query = {}
        search_type = params.get("searchType", "esQuery")

        if search_type == "esQuery":
            raw_query = params.get("esQuery", "{}")
            if isinstance(raw_query, str):
                try:
                    parsed = json.loads(raw_query)
                    es_query = parsed.get("query", parsed)
                except json.JSONDecodeError:
                    es_query = {"query_string": {"query": raw_query}}
            elif isinstance(raw_query, dict):
                es_query = raw_query.get("query", raw_query)
        elif search_type == "searchSource":
            search_source = params.get("searchConfiguration", {})
            if isinstance(search_source, str):
                try:
                    search_source = json.loads(search_source)
                except json.JSONDecodeError:
                    search_source = {}
            es_query = search_source.get("query", {})

        es_query = self._merge_filter(es_query, filter_override)

        if not indices:
            return SimulationResult(
                rule=rule, fired=False, total_match_count=0,
                threshold=thresholds, comparator=comparator,
                error="No indices configured for this rule",
            )

        if host_name:
            return self._simulate_single_host(
                rule, indices, es_query, comparator, thresholds,
                host_name, time_range_seconds,
            )
        else:
            return self._simulate_all_hosts(
                rule, indices, es_query, comparator, thresholds,
                time_range_seconds,
            )

    def _simulate_index_threshold(
        self, rule: Rule, host_name: str | None, time_range_seconds: int,
        overrides: SimulationOverrides | None = None,
        indices_override: list[str] | None = None,
    ) -> SimulationResult:
        params = rule.params
        comparator, thresholds, filter_override = self._resolve_overrides(rule, overrides)
        indices = indices_override or rule.indices
        agg_type = params.get("aggType", "count")
        agg_field = params.get("aggField")
        term_field = params.get("termField")
        term_size = params.get("termSize", 5)
        group_by = params.get("groupBy", "all")

        base_query = self._build_filter_query(filter_override) if filter_override else {}

        if not indices:
            return SimulationResult(
                rule=rule, fired=False, total_match_count=0,
                threshold=thresholds, comparator=comparator,
                error="No indices configured for this rule",
            )

        try:
            if host_name:
                result = self.client.execute_agg_query(
                    indices=indices,
                    query=base_query,
                    agg_type=agg_type,
                    agg_field=agg_field,
                    time_range_seconds=time_range_seconds,
                    host_filter=host_name,
                    group_by_host=False,
                )
                value = self._extract_metric_value(result, agg_type)
                fired = self._check_threshold(value, comparator, thresholds)
                return SimulationResult(
                    rule=rule, fired=fired, total_match_count=value,
                    threshold=thresholds, comparator=comparator,
                    device_results=[DeviceResult(host_name, value, fired)],
                    time_range_start=result["time_start"],
                    time_range_end=result["time_end"],
                    query_used=result["query"],
                )
            else:
                result = self.client.execute_agg_query(
                    indices=indices,
                    query=base_query,
                    agg_type=agg_type,
                    agg_field=agg_field,
                    time_range_seconds=time_range_seconds,
                    group_by_host=True,
                )
                return self._build_host_results(
                    rule, result, agg_type, comparator, thresholds,
                )
        except Exception as e:
            return SimulationResult(
                rule=rule, fired=False, total_match_count=0,
                threshold=thresholds, comparator=comparator,
                error=str(e),
            )

    def _simulate_metrics_threshold(
        self, rule: Rule, host_name: str | None, time_range_seconds: int,
        overrides: SimulationOverrides | None = None,
        indices_override: list[str] | None = None,
    ) -> SimulationResult:
        """Simulate metrics.alert.threshold rules."""
        params = rule.params
        comparator, thresholds, filter_override = self._resolve_overrides(rule, overrides)
        indices = indices_override or rule.indices
        criteria = rule.criteria
        filter_query_text = params.get("filterQueryText", "")

        if not indices:
            return SimulationResult(
                rule=rule, fired=False, total_match_count=0,
                threshold=thresholds, comparator=comparator,
                error="No indices found for metrics threshold rule.",
            )

        if not criteria:
            return SimulationResult(
                rule=rule, fired=False, total_match_count=0,
                threshold=thresholds, comparator=comparator,
                error="No criteria defined in this rule.",
            )

        criterion = criteria[0]
        metric_field = criterion.get("metric", "")
        agg_type = criterion.get("aggType", "avg")
        if metric_field == "custom":
            agg_field = criterion.get("customMetrics", [{}])[0].get("field", "")
            agg_type = criterion.get("customMetrics", [{}])[0].get("aggType", "avg")
        else:
            agg_field = metric_field

        if filter_override is not None:
            base_query = self._build_filter_query(filter_override)
        elif filter_query_text:
            base_query = {"query_string": {"query": filter_query_text}}
        else:
            base_query = {}

        try:
            if host_name:
                result = self.client.execute_agg_query(
                    indices=indices,
                    query=base_query,
                    agg_type=agg_type,
                    agg_field=agg_field,
                    time_range_seconds=time_range_seconds,
                    host_filter=host_name,
                    group_by_host=False,
                )
                value = self._extract_metric_value(result, agg_type)
                fired = self._check_threshold(value, comparator, thresholds)
                return SimulationResult(
                    rule=rule, fired=fired, total_match_count=value,
                    threshold=thresholds, comparator=comparator,
                    device_results=[DeviceResult(host_name, value, fired)],
                    time_range_start=result["time_start"],
                    time_range_end=result["time_end"],
                    query_used=result["query"],
                )
            else:
                result = self.client.execute_agg_query(
                    indices=indices,
                    query=base_query,
                    agg_type=agg_type,
                    agg_field=agg_field,
                    time_range_seconds=time_range_seconds,
                    group_by_host=True,
                )
                return self._build_host_results(
                    rule, result, agg_type, comparator, thresholds,
                )
        except Exception as e:
            return SimulationResult(
                rule=rule, fired=False, total_match_count=0,
                threshold=thresholds, comparator=comparator,
                error=str(e),
            )

    def _simulate_logs_document_count(
        self, rule: Rule, host_name: str | None, time_range_seconds: int,
        overrides: SimulationOverrides | None = None,
        indices_override: list[str] | None = None,
    ) -> SimulationResult:
        """Simulate logs.alert.document.count rules."""
        comparator, thresholds, filter_override = self._resolve_overrides(rule, overrides)
        indices = indices_override or rule.indices
        criteria = rule.criteria

        if not indices:
            return SimulationResult(
                rule=rule, fired=False, total_match_count=0,
                threshold=thresholds, comparator=comparator,
                error="No indices found for log document count rule.",
            )

        must_clauses = []
        for c in criteria:
            field = c.get("field", "")
            value = c.get("value")
            cmp = c.get("comparator", "more than")
            if field and value is not None:
                if cmp in ("equals", "does not equal"):
                    clause = {"term": {field: value}}
                    if cmp == "does not equal":
                        clause = {"bool": {"must_not": [{"term": {field: value}}]}}
                    must_clauses.append(clause)
                elif cmp in ("matches", "does not match"):
                    clause = {"match": {field: value}}
                    if cmp == "does not match":
                        clause = {"bool": {"must_not": [{"match": {field: value}}]}}
                    must_clauses.append(clause)

        base_query = {"bool": {"must": must_clauses}} if must_clauses else {}
        base_query = self._merge_filter(base_query, filter_override)

        if host_name:
            return self._simulate_single_host(
                rule, indices, base_query, comparator, thresholds,
                host_name, time_range_seconds,
            )
        else:
            return self._simulate_all_hosts(
                rule, indices, base_query, comparator, thresholds,
                time_range_seconds,
            )

    def _simulate_generic(
        self, rule: Rule, host_name: str | None, time_range_seconds: int,
        overrides: SimulationOverrides | None = None,
        indices_override: list[str] | None = None,
    ) -> SimulationResult:
        """Fallback simulation for unsupported rule types — tries count-based."""
        comparator, thresholds, filter_override = self._resolve_overrides(rule, overrides)
        indices = indices_override or rule.indices

        base_query = self._build_filter_query(filter_override) if filter_override else {}

        if not indices:
            return SimulationResult(
                rule=rule, fired=False, total_match_count=0,
                threshold=thresholds, comparator=comparator,
                error=f"Unsupported rule type '{rule.rule_type}' and no indices found. "
                      f"Cannot simulate.",
            )

        if host_name:
            return self._simulate_single_host(
                rule, indices, base_query, comparator, thresholds,
                host_name, time_range_seconds,
            )
        else:
            return self._simulate_all_hosts(
                rule, indices, base_query, comparator, thresholds,
                time_range_seconds,
            )

    # ── Helpers ──────────────────────────────────────────────────

    def _simulate_single_host(
        self, rule, indices, es_query, comparator, thresholds,
        host_name, time_range_seconds,
    ) -> SimulationResult:
        try:
            result = self.client.execute_query(
                indices=indices,
                query=es_query if es_query else None,
                time_range_seconds=time_range_seconds,
                host_filter=host_name,
            )
            count = result["total"]
            fired = self._check_threshold(count, comparator, thresholds)
            return SimulationResult(
                rule=rule, fired=fired, total_match_count=count,
                threshold=thresholds, comparator=comparator,
                device_results=[DeviceResult(host_name, count, fired)],
                time_range_start=result["time_start"],
                time_range_end=result["time_end"],
                query_used=result["query"],
            )
        except Exception as e:
            return SimulationResult(
                rule=rule, fired=False, total_match_count=0,
                threshold=thresholds, comparator=comparator,
                error=str(e),
            )

    def _simulate_all_hosts(
        self, rule, indices, es_query, comparator, thresholds,
        time_range_seconds,
    ) -> SimulationResult:
        try:
            # Get hosts and simulate per-host
            hosts = self.client.get_hosts(indices, time_range_seconds)
            if not hosts:
                # No hosts, just run global query
                result = self.client.execute_query(
                    indices=indices,
                    query=es_query if es_query else None,
                    time_range_seconds=time_range_seconds,
                )
                count = result["total"]
                fired = self._check_threshold(count, comparator, thresholds)
                return SimulationResult(
                    rule=rule, fired=fired, total_match_count=count,
                    threshold=thresholds, comparator=comparator,
                    time_range_start=result["time_start"],
                    time_range_end=result["time_end"],
                    query_used=result["query"],
                    error="No host.name values found in data — showing global result.",
                )

            # Use a single aggregation query grouped by host
            result = self.client.execute_agg_query(
                indices=indices,
                query=es_query if es_query else {},
                agg_type="count",
                agg_field=None,
                time_range_seconds=time_range_seconds,
                group_by_host=True,
            )
            return self._build_host_results(
                rule, result, "count", comparator, thresholds,
            )
        except Exception as e:
            return SimulationResult(
                rule=rule, fired=False, total_match_count=0,
                threshold=thresholds, comparator=comparator,
                error=str(e),
            )

    def _build_host_results(
        self, rule, result, agg_type, comparator, thresholds,
    ) -> SimulationResult:
        buckets = result.get("aggregations", {}).get("by_host", {}).get("buckets", [])
        device_results = []
        any_fired = False
        total = 0.0

        for bucket in buckets:
            host = bucket["key"]
            value = self._extract_bucket_value(bucket, agg_type)
            fired = self._check_threshold(value, comparator, thresholds)
            if fired:
                any_fired = True
            total += value
            device_results.append(DeviceResult(host, value, fired))

        # Sort: fired first, then by match count descending
        device_results.sort(key=lambda d: (-int(d.fired), -d.match_count))

        return SimulationResult(
            rule=rule, fired=any_fired, total_match_count=total,
            threshold=thresholds, comparator=comparator,
            device_results=device_results,
            time_range_start=result["time_start"],
            time_range_end=result["time_end"],
            query_used=result["query"],
        )

    @staticmethod
    def _extract_metric_value(result: dict, agg_type: str) -> float:
        if agg_type == "count":
            return float(result.get("total", 0) or 0)
        metric = result.get("aggregations", {}).get("metric", {})
        val = metric.get("value")
        return float(val) if val is not None else 0.0

    @staticmethod
    def _extract_bucket_value(bucket: dict, agg_type: str) -> float:
        if agg_type == "count":
            return float(bucket.get("doc_count", 0) or 0)
        metric = bucket.get("metric", {})
        val = metric.get("value")
        return float(val) if val is not None else 0.0

    @staticmethod
    def _check_threshold(value: float, comparator: str, thresholds: list[float]) -> bool:
        if comparator in ("between", "notBetween"):
            fn = COMPARATORS.get(comparator)
            return fn(value, thresholds) if fn else False
        threshold = thresholds[0] if thresholds else 0
        fn = COMPARATORS.get(comparator, COMPARATORS[">"])
        return fn(value, threshold)
