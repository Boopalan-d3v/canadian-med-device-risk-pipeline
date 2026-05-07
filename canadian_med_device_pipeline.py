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
from typing import Any, Dict, Iterable, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    import psycopg
except ImportError:  # pragma: no cover - optional until dependency is installed
    psycopg = None


PROJECT_ROOT = Path(__file__).resolve().parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
SQL_FILE = PROJECT_ROOT / "sql" / "01_init_pipeline.sql"
ROOT_ENV = PROJECT_ROOT / ".env"
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


@dataclass
class LoadStats:
    records_seen: int = 0
    unique_records: int = 0
    inserted_or_updated: int = 0
    unchanged_skipped: int = 0
    batch_duplicates_skipped: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "records_seen": self.records_seen,
            "unique_records": self.unique_records,
            "inserted_or_updated": self.inserted_or_updated,
            "unchanged_skipped": self.unchanged_skipped,
            "batch_duplicates_skipped": self.batch_duplicates_skipped,
        }


def load_configs() -> tuple[DbConfig, RuntimeConfig]:
    load_env_file(ROOT_ENV)
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


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def first_non_empty(record: Dict[str, Any], *keys: str) -> Optional[str]:
    for key in keys:
        value = clean_text(record.get(key))
        if value:
            return value
    return None


def parse_date(value: Any) -> Optional[datetime.date]:
    text = clean_text(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%b-%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt).date()
        except ValueError:
            continue
    match = re.search(r"(\d{4})[-/](\d{2})[-/](\d{2})", text)
    if match:
        return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3))).date()
    return None


def date_key(value: Any) -> Optional[int]:
    parsed = parse_date(value)
    return int(parsed.strftime("%Y%m%d")) if parsed else None


def parse_risk_class(value: Any) -> Optional[int]:
    text = clean_text(value)
    if not text:
        return None
    match = re.search(r"([1-4])", text)
    return int(match.group(1)) if match else None


def as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def normalize_bool(value: Any) -> bool:
    text = clean_text(value)
    if text is None:
        return False
    return text.lower() in {"1", "true", "yes", "y", "serious", "death", "fatal"}


def normalize_href(href: str) -> str:
    href = href.strip()
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return f"https://recalls-rappels.canada.ca{href}"
    return f"https://recalls-rappels.canada.ca/{href.lstrip('/')}"


def extract_recall_id(href: str) -> str:
    href = normalize_href(href)
    parts = [part for part in href.rstrip("/").split("/") if part]
    slug = parts[-1] if parts else href
    return slug[:100]


def stable_term_code(term_name: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", term_name.lower()).strip("-")
    return (normalized or "unknown-term")[:20]


def dedupe_records(records: Iterable[Dict[str, Any]], key_builder) -> tuple[list[Dict[str, Any]], int]:
    unique: dict[str, Dict[str, Any]] = {}
    duplicates_skipped = 0
    for record in records:
        key = key_builder(record)
        if key in unique:
            duplicates_skipped += 1
            continue
        unique[key] = record
    return list(unique.values()), duplicates_skipped


class DatabaseLoader:
    def __init__(self, db: DbConfig) -> None:
        self.db = db

    def connect(self):
        if psycopg is None:
            raise RuntimeError(
                "psycopg is not installed. Run `pip install -r requirements.txt` before using database loads."
            )
        return psycopg.connect(
            host=self.db.host,
            port=self.db.port,
            dbname=self.db.name,
            user=self.db.user,
            password=self.db.password,
            sslmode=self.db.sslmode,
        )

    def ensure_ready(self) -> None:
        init_db(self.db)

    def start_run(self, pipeline_name: str, task_id: str, metadata: Optional[Dict[str, Any]] = None) -> int:
        with self.connect() as conn:
            with conn.cursor() as cur:
                now = datetime.now(UTC)
                cur.execute(
                    """
                    INSERT INTO audit.etl_run_log (
                        pipeline_name, task_id, execution_date, start_time, status, metadata
                    )
                    VALUES (%s, %s, %s, %s, 'RUNNING', %s::jsonb)
                    RETURNING run_id
                    """,
                    (pipeline_name, task_id, now, now, json.dumps(metadata or {}, ensure_ascii=False)),
                )
                run_id = int(cur.fetchone()[0])
            conn.commit()
        return run_id

    def finish_run(
        self,
        run_id: int,
        *,
        status: str,
        records_extracted: int = 0,
        records_loaded: int = 0,
        records_failed: int = 0,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE audit.etl_run_log
                    SET end_time = %s,
                        status = %s,
                        records_extracted = %s,
                        records_loaded = %s,
                        records_failed = %s,
                        error_message = %s,
                        metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb
                    WHERE run_id = %s
                    """,
                    (
                        datetime.now(UTC),
                        status,
                        records_extracted,
                        records_loaded,
                        records_failed,
                        error_message,
                        json.dumps(metadata or {}, ensure_ascii=False),
                        run_id,
                    ),
                )
            conn.commit()

    def validate_no_duplicates(self) -> Dict[str, Any]:
        checks = [
            (
                "raw.raw_mdall_licence",
                "SELECT source_system, source_record_id, COUNT(*) AS cnt FROM raw.raw_mdall_licence GROUP BY 1,2 HAVING COUNT(*) > 1",
            ),
            (
                "raw.raw_mdall_company",
                "SELECT source_system, source_record_id, COUNT(*) AS cnt FROM raw.raw_mdall_company GROUP BY 1,2 HAVING COUNT(*) > 1",
            ),
            (
                "raw.raw_recall_listing",
                "SELECT source_system, source_record_id, COUNT(*) AS cnt FROM raw.raw_recall_listing GROUP BY 1,2 HAVING COUNT(*) > 1",
            ),
            (
                "raw.raw_canada_vigilance_report",
                "SELECT source_system, source_record_id, COUNT(*) AS cnt FROM raw.raw_canada_vigilance_report GROUP BY 1,2 HAVING COUNT(*) > 1",
            ),
            (
                "dim.dim_manufacturer",
                "SELECT source_system, company_id, COUNT(*) AS cnt FROM dim.dim_manufacturer GROUP BY 1,2 HAVING COUNT(*) > 1",
            ),
            (
                "dim.dim_device",
                "SELECT source_system, device_id, COUNT(*) AS cnt FROM dim.dim_device GROUP BY 1,2 HAVING COUNT(*) > 1",
            ),
            (
                "fact.fact_recall",
                "SELECT source_system, recall_id, COUNT(*) AS cnt FROM fact.fact_recall GROUP BY 1,2 HAVING COUNT(*) > 1",
            ),
            (
                "fact.fact_adverse_event",
                "SELECT source_system, report_id, COUNT(*) AS cnt FROM fact.fact_adverse_event GROUP BY 1,2 HAVING COUNT(*) > 1",
            ),
        ]
        summary: Dict[str, Any] = {"duplicate_tables": 0, "tables_checked": len(checks), "results": []}
        with self.connect() as conn:
            with conn.cursor() as cur:
                for table_name, sql in checks:
                    cur.execute(sql)
                    rows = cur.fetchall()
                    summary["results"].append(
                        {
                            "table": table_name,
                            "duplicate_groups": len(rows),
                            "sample": rows[:10],
                        }
                    )
                    if rows:
                        summary["duplicate_tables"] += 1
        return summary

    def _is_unchanged_raw_payload(
        self, cur, table_name: str, source_system: str, source_record_id: str, payload_json: str
    ) -> bool:
        cur.execute(
            f"SELECT 1 FROM {table_name} WHERE source_system = %s AND source_record_id = %s AND payload = %s::jsonb",
            (source_system, source_record_id, payload_json),
        )
        return cur.fetchone() is not None

    def upsert_mdall_licences(self, records: Iterable[Dict[str, Any]]) -> LoadStats:
        records = list(records)
        stats = LoadStats(records_seen=len(records))
        unique_records, batch_duplicates = dedupe_records(
            records,
            lambda record: first_non_empty(
                record, "device_id", "device_identifier", "licence_number", "licence_no", "id"
            ) or stable_json(record),
        )
        stats.unique_records = len(unique_records)
        stats.batch_duplicates_skipped = batch_duplicates
        with self.connect() as conn:
            with conn.cursor() as cur:
                for index, record in enumerate(unique_records, start=1):
                    payload = stable_json(record)
                    source_record_id = first_non_empty(
                        record, "device_id", "device_identifier", "licence_number", "licence_no", "id"
                    ) or f"mdall-licence-{index}"
                    if self._is_unchanged_raw_payload(cur, "raw.raw_mdall_licence", "MDALL", source_record_id, payload):
                        stats.unchanged_skipped += 1
                        continue
                    cur.execute(
                        """
                        INSERT INTO raw.raw_mdall_licence (source_record_id, source_system, payload)
                        VALUES (%s, 'MDALL', %s::jsonb)
                        ON CONFLICT (source_system, source_record_id)
                        DO UPDATE SET payload = EXCLUDED.payload, loaded_at = CURRENT_TIMESTAMP
                        """,
                        (source_record_id, payload),
                    )
                    manufacturer_key = self._upsert_manufacturer_from_mdall(cur, record)
                    device_id = first_non_empty(record, "device_id", "device_identifier", "licence_number", "id")
                    licence_number = first_non_empty(record, "licence_number", "licence_no", "licence")
                    trade_name = first_non_empty(record, "licence_name", "trade_name", "device_name", "name")
                    generic_name = first_non_empty(record, "generic_name", "device_desc", "description")
                    risk_class = parse_risk_class(
                        first_non_empty(record, "risk_class", "class", "device_class", "licence_class")
                    )
                    licence_status = first_non_empty(record, "status", "licence_status", "state")
                    first_issued = parse_date(first_non_empty(record, "first_issue_date", "issue_date", "first_licence_date"))
                    cancellation = parse_date(first_non_empty(record, "cancellation_date", "removal_date"))
                    source_id = first_non_empty(record, "id", "licence_number", "device_id", "device_identifier")
                    device_id = device_id or licence_number or source_record_id
                    cur.execute(
                        """
                        INSERT INTO dim.dim_device (
                            device_id, licence_number, trade_name, generic_name, risk_class,
                            risk_class_description, device_type, manufacturer_key, licence_status,
                            first_issued_date, cancellation_date, is_active, source_system, source_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'MDALL', %s)
                        ON CONFLICT (device_id, source_system)
                        DO UPDATE SET
                            licence_number = EXCLUDED.licence_number,
                            trade_name = EXCLUDED.trade_name,
                            generic_name = EXCLUDED.generic_name,
                            risk_class = EXCLUDED.risk_class,
                            risk_class_description = EXCLUDED.risk_class_description,
                            device_type = EXCLUDED.device_type,
                            manufacturer_key = COALESCE(EXCLUDED.manufacturer_key, dim.dim_device.manufacturer_key),
                            licence_status = EXCLUDED.licence_status,
                            first_issued_date = COALESCE(EXCLUDED.first_issued_date, dim.dim_device.first_issued_date),
                            cancellation_date = EXCLUDED.cancellation_date,
                            is_active = EXCLUDED.is_active,
                            source_id = EXCLUDED.source_id,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        (
                            device_id,
                            licence_number,
                            trade_name,
                            generic_name,
                            risk_class,
                            first_non_empty(record, "risk_class_description", "class_description"),
                            first_non_empty(record, "device_type", "licence_type", "type"),
                            manufacturer_key,
                            licence_status,
                            first_issued,
                            cancellation,
                            cancellation is None,
                            source_id,
                        ),
                    )
                    stats.inserted_or_updated += 1
            conn.commit()
        return stats

    def upsert_mdall_companies(self, records: Iterable[Dict[str, Any]]) -> LoadStats:
        records = list(records)
        stats = LoadStats(records_seen=len(records))
        unique_records, batch_duplicates = dedupe_records(
            records,
            lambda record: first_non_empty(record, "company_id", "id", "company_name") or stable_json(record),
        )
        stats.unique_records = len(unique_records)
        stats.batch_duplicates_skipped = batch_duplicates
        with self.connect() as conn:
            with conn.cursor() as cur:
                for index, record in enumerate(unique_records, start=1):
                    payload = stable_json(record)
                    source_record_id = first_non_empty(record, "company_id", "id", "company_name") or f"mdall-company-{index}"
                    if self._is_unchanged_raw_payload(cur, "raw.raw_mdall_company", "MDALL", source_record_id, payload):
                        stats.unchanged_skipped += 1
                        continue
                    cur.execute(
                        """
                        INSERT INTO raw.raw_mdall_company (source_record_id, source_system, payload)
                        VALUES (%s, 'MDALL', %s::jsonb)
                        ON CONFLICT (source_system, source_record_id)
                        DO UPDATE SET payload = EXCLUDED.payload, loaded_at = CURRENT_TIMESTAMP
                        """,
                        (source_record_id, payload),
                    )
                    self._upsert_manufacturer_from_company(cur, record)
                    stats.inserted_or_updated += 1
            conn.commit()
        return stats

    def upsert_recalls(self, records: Iterable[Dict[str, Any]]) -> LoadStats:
        records = list(records)
        stats = LoadStats(records_seen=len(records))
        unique_records, batch_duplicates = dedupe_records(
            records,
            lambda record: extract_recall_id(record.get("href", "") or stable_json(record)),
        )
        stats.unique_records = len(unique_records)
        stats.batch_duplicates_skipped = batch_duplicates
        extracted_at = datetime.now(UTC)
        with self.connect() as conn:
            with conn.cursor() as cur:
                for record in unique_records:
                    href = normalize_href(record.get("href", ""))
                    recall_id = extract_recall_id(href or f"recall-{stats.inserted_or_updated + 1}")
                    payload = stable_json(record)
                    if self._is_unchanged_raw_payload(cur, "raw.raw_recall_listing", "HC_RECALLS", recall_id, payload):
                        stats.unchanged_skipped += 1
                        continue
                    cur.execute(
                        """
                        INSERT INTO raw.raw_recall_listing (source_record_id, source_system, payload)
                        VALUES (%s, 'HC_RECALLS', %s::jsonb)
                        ON CONFLICT (source_system, source_record_id)
                        DO UPDATE SET payload = EXCLUDED.payload, loaded_at = CURRENT_TIMESTAMP
                        """,
                        (recall_id, payload),
                    )
                    title = clean_text(record.get("text")) or "Medical device recall listing"
                    recall_class = first_non_empty(record, "recall_class")
                    class_numeric = None
                    if recall_class:
                        match = re.search(r"([1-3])", recall_class)
                        class_numeric = int(match.group(1)) if match else None
                    cur.execute(
                        """
                        INSERT INTO fact.fact_recall (
                            recall_id, recall_number, recall_date_key, recall_class, recall_class_numeric,
                            recall_reason, recall_type, recall_status, product_description, source_system,
                            source_id, source_url, raw_data, extracted_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'HC_RECALLS', %s, %s, %s::jsonb, %s)
                        ON CONFLICT (recall_id, source_system)
                        DO UPDATE SET
                            recall_number = EXCLUDED.recall_number,
                            recall_date_key = COALESCE(EXCLUDED.recall_date_key, fact.fact_recall.recall_date_key),
                            recall_class = COALESCE(EXCLUDED.recall_class, fact.fact_recall.recall_class),
                            recall_class_numeric = COALESCE(EXCLUDED.recall_class_numeric, fact.fact_recall.recall_class_numeric),
                            recall_reason = COALESCE(EXCLUDED.recall_reason, fact.fact_recall.recall_reason),
                            recall_type = COALESCE(EXCLUDED.recall_type, fact.fact_recall.recall_type),
                            recall_status = COALESCE(EXCLUDED.recall_status, fact.fact_recall.recall_status),
                            product_description = EXCLUDED.product_description,
                            source_url = EXCLUDED.source_url,
                            raw_data = EXCLUDED.raw_data,
                            extracted_at = EXCLUDED.extracted_at,
                            last_updated = CURRENT_TIMESTAMP
                        """,
                        (
                            recall_id,
                            recall_id,
                            date_key(first_non_empty(record, "recall_date", "posted_date", "date")),
                            recall_class,
                            class_numeric,
                            first_non_empty(record, "reason", "recall_reason"),
                            first_non_empty(record, "recall_type") or "Recall Listing",
                            first_non_empty(record, "status"),
                            title,
                            recall_id,
                            href,
                            payload,
                            extracted_at,
                        ),
                    )
                    stats.inserted_or_updated += 1
            conn.commit()
        return stats

    def upsert_vigilance_report(self, report: Dict[str, Any]) -> LoadStats:
        stats = LoadStats(records_seen=1, unique_records=1)
        report_record = report
        if isinstance(report, list):
            report_record = report[0] if report else {}
        if not isinstance(report_record, dict):
            raise RuntimeError("Canada Vigilance report payload is not a dictionary record.")
        extracted_at = datetime.now(UTC)
        report_id = first_non_empty(report_record, "report_id", "report_number", "id", "aer_id")
        if not report_id:
            raise RuntimeError("Canada Vigilance report payload does not include a report identifier.")
        payload = stable_json(report_record)
        with self.connect() as conn:
            with conn.cursor() as cur:
                if self._is_unchanged_raw_payload(
                    cur, "raw.raw_canada_vigilance_report", "CANADA_VIGILANCE", report_id, payload
                ):
                    stats.unchanged_skipped = 1
                    conn.commit()
                    return stats
                cur.execute(
                    """
                    INSERT INTO raw.raw_canada_vigilance_report (source_record_id, source_system, payload, extracted_at)
                    VALUES (%s, 'CANADA_VIGILANCE', %s::jsonb, %s)
                    ON CONFLICT (source_system, source_record_id)
                    DO UPDATE SET payload = EXCLUDED.payload, extracted_at = EXCLUDED.extracted_at, loaded_at = CURRENT_TIMESTAMP
                    """,
                    (report_id, payload, extracted_at),
                )
                outcome_key = self._upsert_outcome(cur, report_record)
                cur.execute(
                    """
                    INSERT INTO fact.fact_adverse_event (
                        report_id, report_number, date_received_key, date_occurred_key, outcome_key,
                        patient_age, patient_age_unit, patient_gender,
                        is_serious, is_death, is_life_threatening, is_hospitalization, is_disability,
                        report_type, reporter_type, source_country, source_system, source_id, raw_data, extracted_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'CANADA_VIGILANCE', %s, %s::jsonb, %s)
                    ON CONFLICT (report_id, source_system)
                    DO UPDATE SET
                        report_number = EXCLUDED.report_number,
                        date_received_key = COALESCE(EXCLUDED.date_received_key, fact.fact_adverse_event.date_received_key),
                        date_occurred_key = COALESCE(EXCLUDED.date_occurred_key, fact.fact_adverse_event.date_occurred_key),
                        outcome_key = COALESCE(EXCLUDED.outcome_key, fact.fact_adverse_event.outcome_key),
                        patient_age = COALESCE(EXCLUDED.patient_age, fact.fact_adverse_event.patient_age),
                        patient_age_unit = COALESCE(EXCLUDED.patient_age_unit, fact.fact_adverse_event.patient_age_unit),
                        patient_gender = COALESCE(EXCLUDED.patient_gender, fact.fact_adverse_event.patient_gender),
                        is_serious = EXCLUDED.is_serious,
                        is_death = EXCLUDED.is_death,
                        is_life_threatening = EXCLUDED.is_life_threatening,
                        is_hospitalization = EXCLUDED.is_hospitalization,
                        is_disability = EXCLUDED.is_disability,
                        report_type = COALESCE(EXCLUDED.report_type, fact.fact_adverse_event.report_type),
                        reporter_type = COALESCE(EXCLUDED.reporter_type, fact.fact_adverse_event.reporter_type),
                        source_country = COALESCE(EXCLUDED.source_country, fact.fact_adverse_event.source_country),
                        source_id = EXCLUDED.source_id,
                        raw_data = EXCLUDED.raw_data,
                        extracted_at = EXCLUDED.extracted_at,
                        last_updated = CURRENT_TIMESTAMP
                    RETURNING adverse_event_key
                    """,
                    (
                        report_id,
                        first_non_empty(report_record, "report_number", "report_id", "id"),
                        date_key(first_non_empty(report_record, "date_received", "received_date")),
                        date_key(first_non_empty(report_record, "reaction_date", "date_of_reaction", "date_occurred")),
                        outcome_key,
                        self._parse_smallint(first_non_empty(report_record, "age", "patient_age")),
                        first_non_empty(report_record, "age_unit", "patient_age_unit"),
                        first_non_empty(report_record, "gender", "patient_gender"),
                        normalize_bool(first_non_empty(report_record, "serious", "is_serious")),
                        normalize_bool(first_non_empty(report_record, "death", "is_death")),
                        normalize_bool(first_non_empty(report_record, "life_threatening", "is_life_threatening")),
                        normalize_bool(first_non_empty(report_record, "hospitalization", "is_hospitalization")),
                        normalize_bool(first_non_empty(report_record, "disability", "is_disability")),
                        first_non_empty(report_record, "report_type"),
                        first_non_empty(report_record, "reporter_type"),
                        first_non_empty(report_record, "source_country", "country"),
                        first_non_empty(report_record, "id", "report_id"),
                        payload,
                        extracted_at,
                    ),
                )
                adverse_event_key = int(cur.fetchone()[0])
                self._replace_vigilance_reactions(cur, adverse_event_key, report_record)
            conn.commit()
        stats.inserted_or_updated = 1
        return stats

    def _upsert_manufacturer_from_mdall(self, cur, record: Dict[str, Any]) -> Optional[int]:
        company_id = first_non_empty(record, "company_id", "manufacturer_id", "licence_holder_id")
        company_name = first_non_empty(record, "company_name", "manufacturer_name", "licence_holder_name")
        if not company_id and not company_name:
            return None
        if not company_id:
            company_id = company_name.lower().replace(" ", "-")[:100]
        return self._upsert_manufacturer(
            cur,
            company_id=company_id,
            company_name=company_name or company_id,
            address_line1=first_non_empty(record, "address_line1", "address"),
            address_line2=first_non_empty(record, "address_line2"),
            city=first_non_empty(record, "city"),
            province_state=first_non_empty(record, "province", "region", "state"),
            postal_code=first_non_empty(record, "postal_code", "postal"),
            country=first_non_empty(record, "country"),
            regulatory_status=first_non_empty(record, "status", "company_status"),
            licence_holder_type=first_non_empty(record, "licence_holder_type", "type"),
            source_id=first_non_empty(record, "id", "company_id"),
        )

    def _upsert_manufacturer_from_company(self, cur, record: Dict[str, Any]) -> Optional[int]:
        company_id = first_non_empty(record, "company_id", "id")
        company_name = first_non_empty(record, "company_name", "name")
        if not company_id and not company_name:
            return None
        return self._upsert_manufacturer(
            cur,
            company_id=company_id or company_name.lower().replace(" ", "-")[:100],
            company_name=company_name or company_id,
            address_line1=first_non_empty(record, "address_line_1", "address_line1", "address"),
            address_line2=first_non_empty(record, "address_line_2", "address_line2"),
            city=first_non_empty(record, "city"),
            province_state=first_non_empty(record, "province", "region", "state"),
            postal_code=first_non_empty(record, "postal_code"),
            country=first_non_empty(record, "country"),
            phone=first_non_empty(record, "phone"),
            regulatory_status=first_non_empty(record, "status"),
            licence_holder_type=first_non_empty(record, "type"),
            source_id=first_non_empty(record, "id", "company_id"),
        )

    def _upsert_manufacturer(
        self,
        cur,
        *,
        company_id: str,
        company_name: str,
        address_line1: Optional[str] = None,
        address_line2: Optional[str] = None,
        city: Optional[str] = None,
        province_state: Optional[str] = None,
        postal_code: Optional[str] = None,
        country: Optional[str] = None,
        phone: Optional[str] = None,
        regulatory_status: Optional[str] = None,
        licence_holder_type: Optional[str] = None,
        source_id: Optional[str] = None,
    ) -> int:
        cur.execute(
            """
            INSERT INTO dim.dim_manufacturer (
                company_id, company_name, address_line1, address_line2, city, province_state,
                postal_code, country, phone, regulatory_status, licence_holder_type, source_system, source_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'MDALL', %s)
            ON CONFLICT (company_id, source_system)
            DO UPDATE SET
                company_name = EXCLUDED.company_name,
                address_line1 = COALESCE(EXCLUDED.address_line1, dim.dim_manufacturer.address_line1),
                address_line2 = COALESCE(EXCLUDED.address_line2, dim.dim_manufacturer.address_line2),
                city = COALESCE(EXCLUDED.city, dim.dim_manufacturer.city),
                province_state = COALESCE(EXCLUDED.province_state, dim.dim_manufacturer.province_state),
                postal_code = COALESCE(EXCLUDED.postal_code, dim.dim_manufacturer.postal_code),
                country = COALESCE(EXCLUDED.country, dim.dim_manufacturer.country),
                phone = COALESCE(EXCLUDED.phone, dim.dim_manufacturer.phone),
                regulatory_status = COALESCE(EXCLUDED.regulatory_status, dim.dim_manufacturer.regulatory_status),
                licence_holder_type = COALESCE(EXCLUDED.licence_holder_type, dim.dim_manufacturer.licence_holder_type),
                source_id = COALESCE(EXCLUDED.source_id, dim.dim_manufacturer.source_id),
                updated_at = CURRENT_TIMESTAMP
            RETURNING manufacturer_key
            """,
            (
                company_id,
                company_name,
                address_line1,
                address_line2,
                city,
                province_state,
                postal_code,
                country,
                phone,
                regulatory_status,
                licence_holder_type,
                source_id,
            ),
        )
        manufacturer_key = cur.fetchone()
        return int(manufacturer_key[0])

    def _upsert_outcome(self, cur, record: Dict[str, Any]) -> Optional[int]:
        outcome_code = first_non_empty(record, "outcome_code", "outcome")
        outcome_description = first_non_empty(record, "outcome_description", "outcome_name", "outcome")
        if not outcome_code and not outcome_description:
            return None
        cur.execute(
            """
            INSERT INTO dim.dim_outcome (
                outcome_code, outcome_description, severity_level, is_serious, is_fatal, source_system
            )
            VALUES (%s, %s, %s, %s, %s, 'CANADA_VIGILANCE')
            ON CONFLICT (outcome_code, source_system)
            DO UPDATE SET
                outcome_description = EXCLUDED.outcome_description,
                severity_level = EXCLUDED.severity_level,
                is_serious = EXCLUDED.is_serious,
                is_fatal = EXCLUDED.is_fatal
            RETURNING outcome_key
            """,
            (
                outcome_code or outcome_description,
                outcome_description or outcome_code,
                5 if normalize_bool(first_non_empty(record, "death", "is_death")) else 3 if normalize_bool(first_non_empty(record, "serious", "is_serious")) else 1,
                normalize_bool(first_non_empty(record, "serious", "is_serious")),
                normalize_bool(first_non_empty(record, "death", "is_death")),
            ),
        )
        return int(cur.fetchone()[0])

    def _replace_vigilance_reactions(self, cur, adverse_event_key: int, report_record: Dict[str, Any]) -> None:
        reactions = as_list(report_record.get("reaction")) or as_list(report_record.get("reactions"))
        if not reactions:
            term = first_non_empty(report_record, "reaction_name", "reaction_term", "meddra_pt_name")
            if term:
                reactions = [{"term_name": term}]
        cur.execute("DELETE FROM fact.fact_adverse_event_reaction WHERE adverse_event_key = %s", (adverse_event_key,))
        for index, reaction in enumerate(reactions, start=1):
            if not isinstance(reaction, dict):
                reaction = {"term_name": clean_text(reaction)}
            term_name = first_non_empty(reaction, "term_name", "reaction_name", "meddra_pt_name", "pt_name")
            if not term_name:
                continue
            cur.execute(
                """
                INSERT INTO dim.dim_adverse_event_type (
                    meddra_code, meddra_level, term_name, is_standardized, source_system
                )
                VALUES (%s, %s, %s, %s, 'CANADA_VIGILANCE')
                ON CONFLICT (meddra_code, meddra_level)
                DO UPDATE SET term_name = EXCLUDED.term_name
                RETURNING ae_type_key
                """,
                (
                    first_non_empty(reaction, "meddra_code", "pt_code", "reaction_code") or stable_term_code(term_name),
                    first_non_empty(reaction, "meddra_level", "level") or "PT",
                    term_name,
                    True,
                ),
            )
            ae_type_key = int(cur.fetchone()[0])
            cur.execute(
                """
                INSERT INTO fact.fact_adverse_event_reaction (
                    adverse_event_key, ae_type_key, reaction_sequence, reaction_outcome, seriousness
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (adverse_event_key, ae_type_key, reaction_sequence)
                DO UPDATE SET
                    reaction_outcome = EXCLUDED.reaction_outcome,
                    seriousness = EXCLUDED.seriousness
                """,
                (
                    adverse_event_key,
                    ae_type_key,
                    index,
                    first_non_empty(reaction, "reaction_outcome", "outcome"),
                    first_non_empty(reaction, "seriousness"),
                ),
            )

    def _parse_smallint(self, value: Optional[str]) -> Optional[int]:
        if not value:
            return None
        match = re.search(r"(-?\d+)", value)
        return int(match.group(1)) if match else None


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
    sub.add_parser("validate-no-duplicates", help="Report duplicate key groups across core raw/dim/fact tables")

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
    loader = DatabaseLoader(db)
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

    if args.command == "validate-no-duplicates":
        loader.ensure_ready()
        summary = loader.validate_no_duplicates()
        print(json.dumps(summary, indent=2, default=str))
        return 0

    loader.ensure_ready()
    run_id = loader.start_run(
        pipeline_name="canadian_med_device_pipeline",
        task_id=args.command,
        metadata={"argv": sys.argv[1:]},
    )
    records_extracted = 0
    records_loaded = 0
    try:
        if args.command == "extract-mdall-licences":
            result = MdallExtractor(client).fetch_licences(state=args.state, limit=args.limit)
            records_extracted = len(result["records"])
            load_stats = loader.upsert_mdall_licences(result["records"])
            records_loaded = load_stats.inserted_or_updated
            result["postgres"] = {
                "database": db.name,
                "schema": "dim/raw",
                "tables": ["raw.raw_mdall_licence", "dim.dim_device", "dim.dim_manufacturer"],
                "stored_records": records_loaded,
                "load_stats": load_stats.to_dict(),
            }
        elif args.command == "extract-mdall-companies":
            result = MdallExtractor(client).fetch_companies(status=args.status, limit=args.limit)
            records_extracted = len(result["records"])
            load_stats = loader.upsert_mdall_companies(result["records"])
            records_loaded = load_stats.inserted_or_updated
            result["postgres"] = {
                "database": db.name,
                "schema": "dim/raw",
                "tables": ["raw.raw_mdall_company", "dim.dim_manufacturer"],
                "stored_records": records_loaded,
                "load_stats": load_stats.to_dict(),
            }
        elif args.command == "extract-vigilance-report":
            result = CanadaVigilanceExtractor(client).fetch_report(report_id=args.report_id)
            records_extracted = 1 if result.get("record") else 0
            load_stats = loader.upsert_vigilance_report(result["record"])
            records_loaded = load_stats.inserted_or_updated
            result["postgres"] = {
                "database": db.name,
                "schema": "fact/raw/dim",
                "tables": [
                    "raw.raw_canada_vigilance_report",
                    "fact.fact_adverse_event",
                    "fact.fact_adverse_event_reaction",
                    "dim.dim_outcome",
                    "dim.dim_adverse_event_type",
                ],
                "stored_records": records_loaded,
                "load_stats": load_stats.to_dict(),
            }
        elif args.command == "extract-recalls":
            scrape_client = HttpClient(
                timeout_seconds=runtime.http_timeout_seconds,
                rate_limit_per_minute=runtime.scrape_rate_limit_per_minute,
            )
            result = RecallScraper(scrape_client).fetch_search_results(query=args.query, pages=args.pages)
            records_extracted = len(result["records"])
            load_stats = loader.upsert_recalls(result["records"])
            records_loaded = load_stats.inserted_or_updated
            result["postgres"] = {
                "database": db.name,
                "schema": "fact/raw",
                "tables": ["raw.raw_recall_listing", "fact.fact_recall"],
                "stored_records": records_loaded,
                "load_stats": load_stats.to_dict(),
            }
        elif args.command == "extract-statcan-metadata":
            result = StatisticsCanadaExtractor(client).fetch_cube_metadata(product_id=args.product_id)
        elif args.command == "extract-health-infobase":
            result = HealthInfobaseExtractor(client).fetch_table(database_name=args.database, table_name=args.table)
        else:
            raise ValueError(f"Unsupported command: {args.command}")

        output = write_processed(args.command, result)
        log(f"Wrote processed output: {output}")
        if isinstance(result, dict) and "postgres" in result:
            log(f"Stored {result['postgres']['stored_records']} records in PostgreSQL database {result['postgres']['database']}")
            load_stats = result["postgres"].get("load_stats", {})
            if load_stats:
                log(
                    "Duplicate handling summary: "
                    f"seen={load_stats.get('records_seen', 0)}, "
                    f"unique={load_stats.get('unique_records', 0)}, "
                    f"batch_duplicates_skipped={load_stats.get('batch_duplicates_skipped', 0)}, "
                    f"unchanged_skipped={load_stats.get('unchanged_skipped', 0)}, "
                    f"inserted_or_updated={load_stats.get('inserted_or_updated', 0)}"
                )
        loader.finish_run(
            run_id,
            status="SUCCESS",
            records_extracted=records_extracted,
            records_loaded=records_loaded,
            metadata={
                "output_path": str(output),
                "load_stats": result.get("postgres", {}).get("load_stats", {}) if isinstance(result, dict) else {},
            },
        )
        return 0
    except Exception as exc:
        loader.finish_run(
            run_id,
            status="FAILED",
            records_extracted=records_extracted,
            records_loaded=records_loaded,
            records_failed=max(records_extracted - records_loaded, 1),
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    raise SystemExit(main())
