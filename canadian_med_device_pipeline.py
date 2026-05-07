#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


PROJECT_ROOT = Path(__file__).resolve().parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
SQL_FILE = PROJECT_ROOT / "sql" / "01_init_pipeline.sql"
CONFIG_ENV = PROJECT_ROOT / "config" / ".env"


def log(message: str) -> None:
    timestamp = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    print(f"[{timestamp}] {message}")


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def ensure_dirs() -> None:
    for directory in [RAW_DIR, PROCESSED_DIR, PROJECT_ROOT / "logs"]:
        directory.mkdir(parents=True, exist_ok=True)


@dataclass
class DbConfig:
    host: str
    port: int
    name: str
    user: str
    password: str
    sslmode: str = "prefer"


@dataclass
class RuntimeConfig:
    http_timeout_seconds: int = 60
    api_rate_limit_per_minute: int = 100
    scrape_rate_limit_per_minute: int = 30


def load_configs() -> tuple[DbConfig, RuntimeConfig]:
    load_env_file(CONFIG_ENV)
    db = DbConfig(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        name=os.getenv("DB_NAME", "canadian_med_device_risk"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
        sslmode=os.getenv("DB_SSLMODE", "prefer"),
    )
    runtime = RuntimeConfig(
        http_timeout_seconds=int(os.getenv("HTTP_TIMEOUT_SECONDS", "60")),
        api_rate_limit_per_minute=int(os.getenv("API_RATE_LIMIT_PER_MINUTE", "100")),
        scrape_rate_limit_per_minute=int(os.getenv("SCRAPE_RATE_LIMIT_PER_MINUTE", "30")),
    )
    return db, runtime


class RateLimiter:
    def __init__(self, per_minute: int) -> None:
        self.min_interval = 60.0 / max(per_minute, 1)
        self._last_call = 0.0

    def wait(self) -> None:
        now = time.time()
        elapsed = now - self._last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_call = time.time()


class HttpClient:
    def __init__(self, timeout_seconds: int, rate_limit_per_minute: int) -> None:
        self.timeout_seconds = timeout_seconds
        self.rate_limiter = RateLimiter(rate_limit_per_minute)

    def get_json(self, url: str, params: Optional[Dict[str, Any]] = None, attempts: int = 4) -> Any:
        payload = self.get_text(url, params=params, attempts=attempts, accept="application/json")
        return json.loads(payload)

    def get_text(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        attempts: int = 4,
        accept: str = "text/html,application/json",
    ) -> str:
        full_url = f"{url}?{urlencode(params)}" if params else url
        last_error: Optional[Exception] = None
        for attempt in range(1, attempts + 1):
            self.rate_limiter.wait()
            request = Request(
                full_url,
                headers={
                    "User-Agent": "canadian-med-device-risk-pipeline/1.0",
                    "Accept": accept,
                    "Accept-Language": "en-CA,en;q=0.9",
                },
            )
            try:
                with urlopen(request, timeout=self.timeout_seconds) as response:
                    return response.read().decode("utf-8", errors="replace")
            except (HTTPError, URLError, TimeoutError, ValueError) as exc:
                last_error = exc
                if attempt == attempts:
                    break
                sleep_seconds = 2 ** attempt
                log(f"Retrying {full_url} after error: {exc} (attempt {attempt}/{attempts})")
                time.sleep(sleep_seconds)
        raise RuntimeError(f"GET failed for {full_url}: {last_error}")


class BaseExtractor:
    source_name = "base"

    def __init__(self, client: HttpClient) -> None:
        self.client = client

    def snapshot(self, name: str, payload: Any) -> Path:
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        path = RAW_DIR / f"{self.source_name}-{name}-{stamp}.json"
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        return path


class MdallExtractor(BaseExtractor):
    source_name = "mdall"
    base_url = "https://health-products.canada.ca/api/medical-devices"

    def fetch_licences(self, state: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        params = {"type": "json"}
        if state:
            params["state"] = state
        data = self.client.get_json(f"{self.base_url}/licence/", params=params)
        sliced = data[:limit] if isinstance(data, list) else data
        path = self.snapshot("licences", sliced)
        return {"records": sliced, "snapshot_path": str(path)}

    def fetch_companies(self, status: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        params = {"type": "json"}
        if status:
            params["status"] = status
        data = self.client.get_json(f"{self.base_url}/company/", params=params)
        sliced = data[:limit] if isinstance(data, list) else data
        path = self.snapshot("companies", sliced)
        return {"records": sliced, "snapshot_path": str(path)}


class CanadaVigilanceExtractor(BaseExtractor):
    source_name = "canada-vigilance"
    base_url = "https://health-products.canada.ca/api/canada-vigilance"

    def fetch_report(self, report_id: str) -> Dict[str, Any]:
        data = self.client.get_json(f"{self.base_url}/report/", params={"id": report_id, "type": "json"})
        path = self.snapshot(f"report-{report_id}", data)
        return {"report_id": report_id, "record": data, "snapshot_path": str(path)}


class RecallScraper(BaseExtractor):
    source_name = "recalls"
    base_url = "https://recalls-rappels.canada.ca/en/search/site"

    def fetch_search_results(self, query: str = "medical device", pages: int = 1) -> Dict[str, Any]:
        pages = max(1, pages)
        results = []
        for page in range(pages):
            html = self.client.get_text(
                self.base_url,
                params={"search_api_fulltext": query, "page": page},
                accept="text/html",
            )
            cards = re.findall(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', html, flags=re.I | re.S)
            normalized = [
                {"href": href, "text": re.sub(r"\s+", " ", text).strip()}
                for href, text in cards
                if "recall" in href.lower() or "safety" in href.lower()
            ]
            results.extend(normalized)
        path = self.snapshot("search-results", results)
        return {"records": results, "snapshot_path": str(path)}


class StatisticsCanadaExtractor(BaseExtractor):
    source_name = "statcan"
    base_url = "https://www150.statcan.gc.ca/t1/wds/rest"

    def fetch_cube_metadata(self, product_id: str) -> Dict[str, Any]:
        data = self.client.get_json(f"{self.base_url}/getCubeMetadata", params={"productId": product_id})
        path = self.snapshot(f"cube-metadata-{product_id}", data)
        return {"product_id": product_id, "record": data, "snapshot_path": str(path)}


class HealthInfobaseExtractor(BaseExtractor):
    source_name = "health-infobase"
    base_url = "https://health-infobase.canada.ca/api"

    def fetch_table(self, database_name: str, table_name: str) -> Dict[str, Any]:
        data = self.client.get_json(f"{self.base_url}/{database_name}/table/{table_name}")
        path = self.snapshot(f"{database_name}-{table_name}", data)
        return {"database": database_name, "table": table_name, "record": data, "snapshot_path": str(path)}


def run_psql(db: DbConfig, sql_path: Path) -> None:
    env = os.environ.copy()
    env["PGPASSWORD"] = db.password
    cmd = [
        "psql",
        "-h", db.host,
        "-p", str(db.port),
        "-U", db.user,
        "-d", db.name,
        "-v", "ON_ERROR_STOP=1",
        "-f", str(sql_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "psql failed")


def init_db(db: DbConfig) -> None:
    if not SQL_FILE.exists():
        raise FileNotFoundError(f"Missing SQL bootstrap file: {SQL_FILE}")
    run_psql(db, SQL_FILE)
    log(f"Initialized PostgreSQL schemas and tables in {db.name} using {SQL_FILE.name}")


def source_catalog() -> list[dict[str, str]]:
    return [
        {"source": "MDALL", "method": "REST API", "base_url": "https://health-products.canada.ca/api/medical-devices/"},
        {"source": "Canada Vigilance", "method": "REST API", "base_url": "https://health-products.canada.ca/api/canada-vigilance/"},
        {"source": "Recalls", "method": "Web scraping", "base_url": "https://recalls-rappels.canada.ca/en"},
        {"source": "CIHI", "method": "Formal extract", "base_url": "Secure request only"},
        {"source": "Statistics Canada", "method": "REST API", "base_url": "https://www150.statcan.gc.ca/t1/wds/rest/"},
        {"source": "Health Infobase", "method": "REST API", "base_url": "https://health-infobase.canada.ca/api/"},
        {"source": "CADTH", "method": "Web scraping / manual", "base_url": "https://www.cda-amc.ca/"},
    ]


def write_processed(name: str, payload: Any) -> Path:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = PROCESSED_DIR / f"{name}-{stamp}.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def cli() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Health Canada medical device risk analysis ETL scaffold")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("sources", help="Print supported data sources and endpoints")
    sub.add_parser("init-db", help="Create schemas, tables, indexes, and analytics views")
    sub.add_parser("plan-run", help="Print a recommended extract cadence")

    mdall_lic = sub.add_parser("extract-mdall-licences", help="Extract MDALL licence records")
    mdall_lic.add_argument("--state")
    mdall_lic.add_argument("--limit", type=int, default=100)

    mdall_co = sub.add_parser("extract-mdall-companies", help="Extract MDALL company records")
    mdall_co.add_argument("--status")
    mdall_co.add_argument("--limit", type=int, default=100)

    vigilance = sub.add_parser("extract-vigilance-report", help="Extract a Canada Vigilance report by ID")
    vigilance.add_argument("--report-id", required=True)

    recalls = sub.add_parser("extract-recalls", help="Scrape Health Canada recall search results")
    recalls.add_argument("--query", default="medical device")
    recalls.add_argument("--pages", type=int, default=1)

    statcan = sub.add_parser("extract-statcan-metadata", help="Extract Statistics Canada cube metadata")
    statcan.add_argument("--product-id", required=True)

    infobase = sub.add_parser("extract-health-infobase", help="Extract a Health Infobase table")
    infobase.add_argument("--database", required=True)
    infobase.add_argument("--table", required=True)
    return parser.parse_args()


def main() -> int:
    ensure_dirs()
    args = cli()
    db, runtime = load_configs()
    client = HttpClient(
        timeout_seconds=runtime.http_timeout_seconds,
        rate_limit_per_minute=runtime.api_rate_limit_per_minute,
    )

    if args.command == "sources":
        print(json.dumps(source_catalog(), indent=2))
        return 0

    if args.command == "init-db":
        init_db(db)
        return 0

    if args.command == "plan-run":
        cadence = {
            "daily": ["canada_vigilance_reports", "medical_device_recalls"],
            "weekly": ["mdall_licences", "mdall_companies", "health_infobase"],
            "monthly": ["cihi_secure_extracts", "risk_score_refresh"],
            "quarterly": ["cadth_reviews", "materialized_view_refresh"],
        }
        print(json.dumps(cadence, indent=2))
        return 0

    if args.command == "extract-mdall-licences":
        result = MdallExtractor(client).fetch_licences(state=args.state, limit=args.limit)
    elif args.command == "extract-mdall-companies":
        result = MdallExtractor(client).fetch_companies(status=args.status, limit=args.limit)
    elif args.command == "extract-vigilance-report":
        result = CanadaVigilanceExtractor(client).fetch_report(report_id=args.report_id)
    elif args.command == "extract-recalls":
        scrape_client = HttpClient(
            timeout_seconds=runtime.http_timeout_seconds,
            rate_limit_per_minute=runtime.scrape_rate_limit_per_minute,
        )
        result = RecallScraper(scrape_client).fetch_search_results(query=args.query, pages=args.pages)
    elif args.command == "extract-statcan-metadata":
        result = StatisticsCanadaExtractor(client).fetch_cube_metadata(product_id=args.product_id)
    elif args.command == "extract-health-infobase":
        result = HealthInfobaseExtractor(client).fetch_table(database_name=args.database, table_name=args.table)
    else:
        raise ValueError(f"Unsupported command: {args.command}")

    output = write_processed(args.command, result)
    log(f"Wrote processed output: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
