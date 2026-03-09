import json
import base64
from datetime import datetime, timedelta, timezone

import requests
from elasticsearch import Elasticsearch

from .models import ConnectionConfig, Rule


class ElasticKibanaClient:
    """Handles connections to Elasticsearch and Kibana APIs."""

    def __init__(self):
        self.es: Elasticsearch | None = None
        self.kibana_session: requests.Session | None = None
        self.kibana_url: str = ""
        self.config: ConnectionConfig | None = None
        self.cluster_name: str = ""
        self.cluster_version: str = ""

    def connect(self, config: ConnectionConfig) -> str:
        """Connect to ES and Kibana. Returns cluster info string on success."""
        self.config = config
        self._connect_elasticsearch(config)
        self._setup_kibana_session(config)

        info = self.es.info()
        self.cluster_name = info["cluster_name"]
        self.cluster_version = info["version"]["number"]
        return f"{self.cluster_name} (v{self.cluster_version})"

    def _connect_elasticsearch(self, config: ConnectionConfig):
        if config.method == "cloud_id":
            self.es = Elasticsearch(
                cloud_id=config.cloud_id,
                api_key=config.api_key,
            )
        elif config.method == "url_basic":
            self.es = Elasticsearch(
                config.url,
                basic_auth=(config.username, config.password),
                verify_certs=False,
            )
        elif config.method == "url_apikey":
            self.es = Elasticsearch(
                config.url,
                api_key=config.api_key,
                verify_certs=False,
            )

    def _setup_kibana_session(self, config: ConnectionConfig):
        self.kibana_session = requests.Session()
        self.kibana_session.verify = False
        self.kibana_session.headers["kbn-xsrf"] = "true"
        self.kibana_session.headers["Content-Type"] = "application/json"

        if config.method == "cloud_id":
            # Parse cloud_id to extract Kibana URL
            decoded = base64.b64decode(config.cloud_id.split(":")[1]).decode()
            parts = decoded.split("$")
            host = parts[0]
            kibana_id = parts[2] if len(parts) > 2 else parts[1]
            self.kibana_url = f"https://{kibana_id}.{host}"
            self.kibana_session.headers["Authorization"] = f"ApiKey {config.api_key}"
        elif config.method == "url_basic":
            # Use explicit Kibana URL or derive from ES URL
            if config.kibana_url_override:
                self.kibana_url = config.kibana_url_override.rstrip("/")
            else:
                self.kibana_url = config.url.rstrip("/")
                if ":9200" in self.kibana_url:
                    self.kibana_url = self.kibana_url.replace(":9200", ":5601")
            self.kibana_session.auth = (config.username, config.password)
        elif config.method == "url_apikey":
            if config.kibana_url_override:
                self.kibana_url = config.kibana_url_override.rstrip("/")
            else:
                self.kibana_url = config.url.rstrip("/")
                if ":9200" in self.kibana_url:
                    self.kibana_url = self.kibana_url.replace(":9200", ":5601")
            self.kibana_session.headers["Authorization"] = f"ApiKey {config.api_key}"

    def disconnect(self):
        if self.es:
            self.es.close()
            self.es = None
        if self.kibana_session:
            self.kibana_session.close()
            self.kibana_session = None
        self.cluster_name = ""
        self.cluster_version = ""

    @property
    def is_connected(self) -> bool:
        return self.es is not None

    # ── Kibana API ──────────────────────────────────────────────

    def _kibana_get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self.kibana_url}{path}"
        resp = self.kibana_session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_spaces(self) -> list[dict]:
        """Fetch all Kibana spaces."""
        return self._kibana_get("/api/spaces/space")

    def get_rules(self, space_id: str = "default") -> list[Rule]:
        """Fetch all alerting rules from a Kibana space (paginated)."""
        rules = []
        page = 1
        per_page = 100

        while True:
            prefix = f"/s/{space_id}" if space_id != "default" else ""
            data = self._kibana_get(
                f"{prefix}/api/alerting/rules/_find",
                params={
                    "page": page,
                    "per_page": per_page,
                    "sort_field": "name",
                    "sort_order": "asc",
                },
            )

            for r in data.get("data", []):
                rules.append(Rule(
                    id=r["id"],
                    name=r["name"],
                    rule_type=r["rule_type_id"],
                    enabled=r["enabled"],
                    schedule=r.get("schedule", {}),
                    params=r.get("params", {}),
                    tags=r.get("tags", []),
                    consumer=r.get("consumer", ""),
                    actions=r.get("actions", []),
                ))

            total = data.get("total", 0)
            if page * per_page >= total:
                break
            page += 1

        return rules

    # ── Elasticsearch Queries ───────────────────────────────────

    def get_indices(self) -> list[str]:
        """Get list of data stream and index names."""
        indices = []
        # Get indices
        idx_result = self.es.cat.indices(format="json", h="index")
        for item in idx_result:
            name = item["index"]
            if not name.startswith("."):
                indices.append(name)
        # Get data streams
        try:
            ds_result = self.es.indices.get_data_stream(name="*")
            for ds in ds_result.get("data_streams", []):
                indices.append(ds["name"])
        except Exception:
            pass
        return sorted(set(indices))

    def get_hosts(self, indices: list[str], time_range_seconds: int = 86400) -> list[str]:
        """Get unique host.name values from given indices in the time range."""
        now = datetime.now(timezone.utc)
        start = now - timedelta(seconds=time_range_seconds)

        try:
            result = self.es.search(
                index=",".join(indices),
                body={
                    "size": 0,
                    "query": {
                        "range": {
                            "@timestamp": {
                                "gte": start.isoformat(),
                                "lte": now.isoformat(),
                            }
                        }
                    },
                    "aggs": {
                        "hosts": {
                            "terms": {
                                "field": "host.name",
                                "size": 10000,
                            }
                        }
                    },
                },
                ignore_unavailable=True,
            )
            buckets = result.get("aggregations", {}).get("hosts", {}).get("buckets", [])
            return sorted([b["key"] for b in buckets])
        except Exception:
            return []

    def execute_query(
        self,
        indices: list[str],
        query: dict,
        time_range_seconds: int,
        host_filter: str | None = None,
    ) -> dict:
        """Execute an ES query with time range and optional host filter."""
        now = datetime.now(timezone.utc)
        start = now - timedelta(seconds=time_range_seconds)

        must_clauses = [
            {
                "range": {
                    "@timestamp": {
                        "gte": start.isoformat(),
                        "lte": now.isoformat(),
                    }
                }
            }
        ]

        # Add the rule's query
        if query:
            must_clauses.append(query)

        # Add host filter
        if host_filter:
            must_clauses.append({"term": {"host.name": host_filter}})

        full_query = {"bool": {"must": must_clauses}}

        result = self.es.search(
            index=",".join(indices),
            body={"size": 0, "query": full_query, "track_total_hits": True},
            ignore_unavailable=True,
        )

        return {
            "total": result["hits"]["total"]["value"],
            "time_start": start.isoformat(),
            "time_end": now.isoformat(),
            "query": full_query,
        }

    def execute_agg_query(
        self,
        indices: list[str],
        query: dict,
        agg_type: str,
        agg_field: str | None,
        time_range_seconds: int,
        host_filter: str | None = None,
        group_by_host: bool = False,
    ) -> dict:
        """Execute an ES aggregation query."""
        now = datetime.now(timezone.utc)
        start = now - timedelta(seconds=time_range_seconds)

        must_clauses = [
            {
                "range": {
                    "@timestamp": {
                        "gte": start.isoformat(),
                        "lte": now.isoformat(),
                    }
                }
            }
        ]

        if query:
            must_clauses.append(query)
        if host_filter:
            must_clauses.append({"term": {"host.name": host_filter}})

        full_query = {"bool": {"must": must_clauses}}

        # Build aggregation
        aggs = {}
        metric_agg = self._build_metric_agg(agg_type, agg_field)

        if group_by_host:
            aggs["by_host"] = {
                "terms": {"field": "host.name", "size": 10000},
                "aggs": {"metric": metric_agg} if metric_agg else {},
            }
        else:
            if metric_agg:
                aggs["metric"] = metric_agg

        body = {"size": 0, "query": full_query, "track_total_hits": True}
        if aggs:
            body["aggs"] = aggs

        result = self.es.search(
            index=",".join(indices),
            body=body,
            ignore_unavailable=True,
        )

        return {
            "total": result["hits"]["total"]["value"],
            "aggregations": result.get("aggregations", {}),
            "time_start": start.isoformat(),
            "time_end": now.isoformat(),
            "query": full_query,
        }

    @staticmethod
    def _build_metric_agg(agg_type: str, agg_field: str | None) -> dict | None:
        if agg_type == "count":
            return None  # Count uses total hits
        if agg_type in ("avg", "sum", "min", "max") and agg_field:
            return {agg_type: {"field": agg_field}}
        if agg_type == "cardinality" and agg_field:
            return {"cardinality": {"field": agg_field}}
        return None
