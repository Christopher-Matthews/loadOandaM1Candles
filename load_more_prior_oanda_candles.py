#!/usr/bin/env python3
"""
Backfill older OANDA EUR_USD M1 candles into the same BigQuery production table as
load_oanda_candles.py, without using the weekly job's staging table.

Manual usage:
- Set PRIOR_START_UTC below to the earliest instant you want to load (UTC).
- Ensure .env / secrets provide OANDA_ACCESS_TOKEN (and optional OANDA_API_BASE),
  and BigQuery auth (GOOGLE_APPLICATION_CREDENTIALS or ba.json next to this file).
- Run: python load_more_prior_oanda_candles.py

Boundary semantics:
- Reads MIN(timestamp) from production m1HistoricalCandles for EUR_USD. That timestamp
  is the exclusive upper bound: only rows with timestamp < MIN are inserted from this script.
- Requires PRIOR_START_UTC < MIN(prod); otherwise the script exits with an error.

Stop conditions:
- OANDA returns no complete candles for a request (empty batch).
- After filtering to [PRIOR_START_UTC, MIN(prod)), the batch has no rows (gap filled up
  to the production floor, or only bars at/after MIN were returned).
- Next fetch start is at or past MIN(prod).

Staging table m1PriorHistoricalCandlesStaging is truncated each batch so it never clashes
with the weekly sync's m1HistoricalCandlesStaging. MERGE into production matches on
(instrument, timestamp) and inserts only when not matched (no duplicates).
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from google.cloud import bigquery

from load_oanda_candles import (
    DATASET_ID,
    INSTRUMENT,
    PROJECT_ID,
    TABLE_ID,
    _clean_env_value,
    bq_client,
    candle_records,
    candles_schema,
    ensure_dataset,
    ensure_table,
    fetch_candle_batch,
    rows_for_bigquery_json,
)

# Earliest candle to request from OANDA (UTC). Must be strictly before MIN(timestamp) in prod.
PRIOR_START_UTC = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

PRIOR_STAGING_ID = "m1PriorHistoricalCandlesStaging"

SCRIPT_DIR = Path(__file__).resolve().parent


def _as_naive_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def min_stored_candle_timestamp(client: bigquery.Client) -> datetime | None:
    sql = f"""
    SELECT MIN(timestamp) AS min_ts
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
    return row.min_ts


def load_records_to_staging_table(
    client: bigquery.Client, staging_table_id: str, records: list[dict[str, Any]]
) -> None:
    if not records:
        return
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{staging_table_id}"
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


def merge_staging_into_main(client: bigquery.Client, staging_table_id: str) -> None:
    fields = [f.name for f in candles_schema()]
    insert_cols = ", ".join(f"`{c}`" for c in fields)
    values = ", ".join(f"S.`{c}`" for c in fields)
    sql = f"""
    MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` T
    USING `{PROJECT_ID}.{DATASET_ID}.{staging_table_id}` S
    ON T.instrument = S.instrument
       AND T.timestamp = S.timestamp
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols})
      VALUES ({values})
    """
    client.query(sql).result()


def filter_prior_window(
    records: list[dict[str, Any]], prior_start_naive: datetime, min_prod_naive: datetime
) -> list[dict[str, Any]]:
    return [
        r
        for r in records
        if prior_start_naive <= r["timestamp"] < min_prod_naive
    ]


def main() -> None:
    load_dotenv(SCRIPT_DIR / ".env")

    token = _clean_env_value("OANDA_ACCESS_TOKEN")
    _clean_env_value("OANDA_ACCOUNT_ID")
    api_base = _clean_env_value("OANDA_API_BASE", required=False) or "https://api-fxtrade.oanda.com"

    headers = {"Authorization": f"Bearer {token}"}
    client = bq_client()

    ensure_dataset(client)
    ensure_table(client, TABLE_ID)
    ensure_table(client, PRIOR_STAGING_ID)

    staging_ref = f"`{PROJECT_ID}.{DATASET_ID}.{PRIOR_STAGING_ID}`"

    min_prod = min_stored_candle_timestamp(client)
    if min_prod is None:
        raise RuntimeError(
            "Production table has no rows for this instrument; run load_oanda_candles.py first."
        )

    min_prod_naive = _as_naive_utc(min_prod)
    prior_start_naive = _as_naive_utc(PRIOR_START_UTC)

    if prior_start_naive >= min_prod_naive:
        raise RuntimeError(
            f"PRIOR_START_UTC ({PRIOR_START_UTC.isoformat()}) must be strictly before "
            f"MIN(timestamp) in production ({min_prod_naive.isoformat()})."
        )

    fetch_from = PRIOR_START_UTC
    while True:
        if _as_naive_utc(fetch_from) >= min_prod_naive:
            break

        payload = fetch_candle_batch(api_base, headers, fetch_from)
        records = candle_records(payload)
        if not records:
            break

        filtered = filter_prior_window(records, prior_start_naive, min_prod_naive)
        if not filtered:
            break

        client.query(f"TRUNCATE TABLE {staging_ref}").result()
        load_records_to_staging_table(client, PRIOR_STAGING_ID, filtered)
        merge_staging_into_main(client, PRIOR_STAGING_ID)

        latest_uploaded = max(r["timestamp"] for r in filtered)
        print(latest_uploaded.strftime("%Y-%m-%d %H:%M:%S UTC"))

        next_naive = latest_uploaded + timedelta(minutes=1)
        next_from = next_naive.replace(tzinfo=timezone.utc)
        if next_from <= fetch_from:
            break

        fetch_from = next_from


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as e:
        print(e, file=sys.stderr)
        sys.exit(1)
