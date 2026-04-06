#!/usr/bin/env python3
"""
Incremental OANDA EUR_USD M1 candles sync into BigQuery.

Flow:
1) Ensure dataset/table/staging exist.
2) Start from max stored candle timestamp + 1 minute, or 2023-01-01T00:00:00Z if empty.
3) Pull 1000 candles from OANDA.
4) TRUNCATE staging, load rows into staging, MERGE into production on candle timestamp.
5) Repeat until no newer candles are returned.
"""
from __future__ import annotations

import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

PROJECT_ID = "bold-artifact-312304"
DATASET_ID = "oanda"
TABLE_ID = "m1HistoricalCandles"
STAGING_ID = "m1HistoricalCandlesStaging"
INSTRUMENT = "EUR_USD"
GRANULARITY = "M1"
BATCH_SIZE = 1000
START_UTC = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

SCRIPT_DIR = Path(__file__).resolve().parent


def _clean_env_value(name: str, required: bool = True) -> str:
    raw = os.environ.get(name)
    if raw is None:
        if required:
            raise RuntimeError(f"Missing required environment variable: {name}")
        return ""

    value = raw.strip()
    if (
        len(value) >= 2
        and ((value[0] == "'" and value[-1] == "'") or (value[0] == '"' and value[-1] == '"'))
    ):
        value = value[1:-1].strip()

    if required and not value:
        raise RuntimeError(f"Environment variable {name} is empty after trimming.")
    return value


def _credentials_path() -> str:
    return os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", str(SCRIPT_DIR / "ba.json"))


def bq_client() -> bigquery.Client:
    return bigquery.Client.from_service_account_json(_credentials_path())


def candles_schema() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("instrument", "STRING"),
        bigquery.SchemaField("timestamp", "DATETIME"),
        bigquery.SchemaField("open", "FLOAT"),
        bigquery.SchemaField("high", "FLOAT"),
        bigquery.SchemaField("low", "FLOAT"),
        bigquery.SchemaField("close", "FLOAT"),
        bigquery.SchemaField("volume", "INTEGER"),
        bigquery.SchemaField("_loadedAt", "TIMESTAMP"),
    ]


def ensure_dataset(client: bigquery.Client) -> None:
    ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
    try:
        client.get_dataset(ref)
    except NotFound:
        client.create_dataset(bigquery.Dataset(ref))


def ensure_table(client: bigquery.Client, table_id: str) -> None:
    ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID).table(table_id)
    try:
        client.get_table(ref)
    except NotFound:
        client.create_table(bigquery.Table(ref, schema=candles_schema()))


def _format_oanda_from(dt_utc: datetime) -> str:
    return dt_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_oanda_datetime(s: str | None) -> datetime | None:
    if not s or not isinstance(s, str):
        return None
    s_iso = s.strip()
    if not s_iso:
        return None
    if s_iso.endswith("Z"):
        s_iso = s_iso[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(s_iso)
    except ValueError:
        try:
            dt = datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
            return dt
        except ValueError:
            return None

    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _to_float(x: Any, default: float = 0.0) -> float:
    if x is None or x == "":
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _to_int(x: Any, default: int = 0) -> int:
    if x is None or x == "":
        return default
    try:
        return int(round(float(x)))
    except (TypeError, ValueError):
        return default


def _to_bool(x: Any, default: bool = False) -> bool:
    if isinstance(x, bool):
        return x
    if isinstance(x, str):
        v = x.strip().lower()
        if v in {"true", "1", "yes", "y"}:
            return True
        if v in {"false", "0", "no", "n"}:
            return False
    if isinstance(x, (int, float)):
        return bool(x)
    return default


def fetch_candle_batch(api_base: str, headers: dict[str, str], from_utc: datetime) -> dict[str, Any]:
    params = {
        "granularity": GRANULARITY,
        "price": "M",
        "count": str(BATCH_SIZE),
        "from": _format_oanda_from(from_utc),
    }
    url = f"{api_base.rstrip('/')}/v3/instruments/{INSTRUMENT}/candles"
    r = requests.get(url, headers=headers, params=params, timeout=120)
    r.raise_for_status()
    return r.json()


def candle_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    instrument = str(payload.get("instrument") or INSTRUMENT)
    out: list[dict[str, Any]] = []
    loaded_at = datetime.now(timezone.utc)

    for row in payload.get("candles") or []:
        if not isinstance(row, dict):
            continue

        if not _to_bool(row.get("complete"), False):
            continue

        mid = row.get("mid")
        if not isinstance(mid, dict):
            continue

        ts = _parse_oanda_datetime(row.get("time"))
        if ts is None:
            continue

        out.append(
            {
                "instrument": instrument,
                "timestamp": ts,
                "open": _to_float(mid.get("o"), 0.0),
                "high": _to_float(mid.get("h"), 0.0),
                "low": _to_float(mid.get("l"), 0.0),
                "close": _to_float(mid.get("c"), 0.0),
                "volume": int(row.get("volume", 0)),
                "_loadedAt": loaded_at,
            }
        )

    return out


def _is_missing_json_value(val: object) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return True
    return False


def rows_for_bigquery_json(normalized: list[dict[str, Any]]) -> list[dict[str, object]]:
    type_by_col = {f.name: f.field_type for f in candles_schema()}
    out: list[dict[str, object]] = []

    for row in normalized:
        rec: dict[str, object] = {}
        for name, val in row.items():
            ft = type_by_col.get(name, "STRING")
            if _is_missing_json_value(val):
                rec[name] = None
                continue

            if ft == "TIMESTAMP":
                if not isinstance(val, datetime):
                    rec[name] = None
                    continue
                ts = val
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                else:
                    ts = ts.astimezone(timezone.utc)
                rec[name] = ts.strftime("%Y-%m-%d %H:%M:%S.%f") + " UTC"
            elif ft == "DATETIME":
                if not isinstance(val, datetime):
                    rec[name] = None
                else:
                    rec[name] = val.strftime("%Y-%m-%d %H:%M:%S")
            elif ft == "INTEGER":
                rec[name] = _to_int(val, 0)
            elif ft == "FLOAT":
                try:
                    fv = float(val)
                    if math.isnan(fv) or math.isinf(fv):
                        rec[name] = None
                    else:
                        rec[name] = fv
                except (TypeError, ValueError):
                    rec[name] = None
            elif ft == "BOOL":
                rec[name] = _to_bool(val, False)
            else:
                rec[name] = str(val)
        out.append(rec)
    return out


def load_records_to_staging(client: bigquery.Client, records: list[dict[str, Any]]) -> None:
    if not records:
        return
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{STAGING_ID}"
    rows = rows_for_bigquery_json(records)
    job = client.load_table_from_json(
        rows,
        table_ref,
        job_config=bigquery.LoadJobConfig(
            schema=candles_schema(),
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ),
    )
    job.result()


def merge_staging_into_main(client: bigquery.Client) -> None:
    fields = [f.name for f in candles_schema()]
    insert_cols = ", ".join(f"`{c}`" for c in fields)
    values = ", ".join(f"S.`{c}`" for c in fields)
    sql = f"""
    MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` T
    USING `{PROJECT_ID}.{DATASET_ID}.{STAGING_ID}` S
    ON T.instrument = S.instrument
       AND T.timestamp = S.timestamp
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols})
      VALUES ({values})
    """
    client.query(sql).result()


def max_stored_candle_timestamp(client: bigquery.Client) -> datetime | None:
    sql = f"""
    SELECT MAX(timestamp) AS max_ts
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE instrument = @instrument
    """
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("instrument", "STRING", INSTRUMENT),
            ]
        ),
    )
    row = next(iter(job.result()))
    return row.max_ts


def next_fetch_start(client: bigquery.Client) -> datetime:
    max_ts = max_stored_candle_timestamp(client)
    if max_ts is None:
        return START_UTC

    if max_ts.tzinfo is None:
        max_ts_utc = max_ts.replace(tzinfo=timezone.utc)
    else:
        max_ts_utc = max_ts.astimezone(timezone.utc)

    return max_ts_utc + timedelta(minutes=1)


def main() -> None:
    load_dotenv(SCRIPT_DIR / ".env")

    token = _clean_env_value("OANDA_ACCESS_TOKEN")
    _clean_env_value("OANDA_ACCOUNT_ID")  # kept for parity with existing env config
    api_base = _clean_env_value("OANDA_API_BASE", required=False) or "https://api-fxtrade.oanda.com"

    headers = {"Authorization": f"Bearer {token}"}
    client = bq_client()

    ensure_dataset(client)
    ensure_table(client, TABLE_ID)
    ensure_table(client, STAGING_ID)

    staging_ref = f"`{PROJECT_ID}.{DATASET_ID}.{STAGING_ID}`"

    fetch_from = next_fetch_start(client)
    while True:
        payload = fetch_candle_batch(api_base, headers, fetch_from)
        records = candle_records(payload)
        if not records:
            break

        client.query(f"TRUNCATE TABLE {staging_ref}").result()
        load_records_to_staging(client, records)
        merge_staging_into_main(client)

        latest = max_stored_candle_timestamp(client)
        if latest is None:
            break

        if latest.tzinfo is None:
            latest_utc = latest.replace(tzinfo=timezone.utc)
        else:
            latest_utc = latest.astimezone(timezone.utc)

        next_from = latest_utc + timedelta(minutes=1)
        print(
            f"Loaded batch rows={len(records)}. max_timestamp={latest_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )

        if next_from <= fetch_from:
            break

        fetch_from = next_from


if __name__ == "__main__":
    main()
