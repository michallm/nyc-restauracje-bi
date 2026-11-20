import os
from datetime import datetime, timedelta, timezone

import psycopg
import requests
from prefect import flow, task
from prefect.runtime import flow_run
from psycopg.types.json import Jsonb


URL = "https://data.cityofnewyork.us/resource/jrsc-cabt.json"
PAGE_SIZE = 50000
OVERLAP = timedelta(days=1)
DATABASE_URL = os.environ["NYC_DATABASE_URL"]
DAGSTER_URL = os.environ["DAGSTER_URL"]

COLUMNS = [
    "unique_key", "created_date", "closed_date", "due_date",
    "resolution_action_updated_date", "agency", "complaint_type", "descriptor",
    "location_type", "status", "incident_zip", "incident_address",
    "street_name", "city", "borough", "community_board",
    "latitude", "longitude", "raw", "source_updated_at",
]
TIMESTAMP_COLUMNS = {
    "created_date", "closed_date", "due_date",
    "resolution_action_updated_date", "source_updated_at",
}
FLOAT_COLUMNS = {"latitude", "longitude"}

CREATE_TABLE_SQL = """
create table if not exists staging.complaints_311(
    unique_key text primary key,
    created_date timestamp,
    closed_date timestamp,
    due_date timestamp,
    resolution_action_updated_date timestamp,
    agency text,
    complaint_type text,
    descriptor text,
    location_type text,
    status text,
    incident_zip text,
    incident_address text,
    street_name text,
    city text,
    borough text,
    community_board text,
    latitude double precision,
    longitude double precision,
    geom geometry(Point,4326),
    raw jsonb,
    source_updated_at timestamptz,
    loaded_at timestamptz,
    deleted_at timestamptz
);
create index if not exists complaints_311_geom_idx
    on staging.complaints_311 using gist (geom);
create index if not exists complaints_311_source_updated_at_idx
    on staging.complaints_311 (source_updated_at);
"""


def parse_timestamp(value):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def parse_float(value):
    if value in (None, ""):
        return None
    return float(value)


@task
def get_watermark() -> datetime | None:
    with psycopg.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            cursor.execute(CREATE_TABLE_SQL)
            cursor.execute(
                "select max(source_updated_at) from staging.complaints_311"
            )
            return cursor.fetchone()[0]


@task(retries=3, retry_delay_seconds=30)
def fetch_311(since: datetime | None) -> list[dict]:
    rows = []
    offset = 0
    where = None

    if since is not None:
        start = (since - OVERLAP).astimezone(timezone.utc)
        timestamp = start.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        where = f":updated_at > '{timestamp}'"

    while True:
        params = {
            "$select": ":*,*",
            "$limit": PAGE_SIZE,
            "$offset": offset,
            "$order": ":id",
        }
        if where:
            params["$where"] = where

        response = requests.get(URL, params=params, timeout=120)
        response.raise_for_status()
        page = response.json()
        rows.extend(page)

        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    print(f"fetched {len(rows)} rows")
    return rows


@task
def upsert_staging(rows: list[dict]) -> dict:
    insert_columns = ", ".join(COLUMNS + ["geom", "loaded_at", "deleted_at"])
    placeholders = ", ".join(["%s"] * len(COLUMNS))
    updates = ", ".join(
        f"{column} = excluded.{column}" for column in COLUMNS[1:] + ["geom"]
    )
    sql = f"""
    insert into staging.complaints_311 ({insert_columns})
    values (
        {placeholders},
        st_setsrid(st_makepoint(%s::float8, %s::float8), 4326),
        now(), null
    )
    on conflict (unique_key) do update set
        {updates}, loaded_at = now(), deleted_at = null
    """

    values = []
    for row in rows:
        row_values = []
        for column in COLUMNS:
            source_column = ":updated_at" if column == "source_updated_at" else column
            value = row[source_column] if column == "unique_key" else row.get(source_column)
            if column in TIMESTAMP_COLUMNS:
                value = parse_timestamp(value)
            elif column in FLOAT_COLUMNS:
                value = parse_float(value)
            elif column == "raw":
                value = Jsonb(row)
            row_values.append(value)
        row_values.extend([parse_float(row.get("longitude")), parse_float(row.get("latitude"))])
        values.append(tuple(row_values))

    with psycopg.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            cursor.execute("select count(*) from staging.complaints_311")
            before = cursor.fetchone()[0]
            cursor.executemany(sql, values)
            cursor.execute("select count(*) from staging.complaints_311")
            after = cursor.fetchone()[0]

    inserted = after - before
    return {"inserted": inserted, "updated": len(rows) - inserted}


@task(retries=3, retry_delay_seconds=30)
def reconcile_deleted() -> int:
    source_keys = set()
    offset = 0

    while True:
        response = requests.get(
            URL,
            params={
                "$select": "unique_key",
                "$limit": PAGE_SIZE,
                "$offset": offset,
                "$order": "unique_key",
            },
            timeout=120,
        )
        response.raise_for_status()
        page = response.json()
        source_keys.update(row["unique_key"] for row in page)

        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    if not source_keys:
        raise RuntimeError("source returned no unique keys")

    with psycopg.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "select unique_key from staging.complaints_311 where deleted_at is null"
            )
            db_keys = {row[0] for row in cursor.fetchall()}
            missing = db_keys - source_keys
            if missing:
                cursor.execute(
                    """
                    update staging.complaints_311
                    set deleted_at = now()
                    where unique_key = any(%s)
                    """,
                    (list(missing),),
                )

    return len(missing)


@task(retries=3, retry_delay_seconds=30)
def report_to_dagster(stats: dict) -> None:
    with psycopg.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select count(*) filter (where deleted_at is null),
                       max(source_updated_at)
                from staging.complaints_311
                """
            )
            total_active, watermark = cursor.fetchone()

    metadata = {
        "fetched": stats["fetched"],
        "inserted": stats["inserted"],
        "updated": stats["updated"],
        "deleted": stats["deleted"],
        "total_active": total_active,
        "watermark": watermark.isoformat() if watermark else None,
        "prefect_flow_run_id": str(flow_run.id),
    }
    description = "NYC 311 restaurant complaints loaded into PostgreSQL staging"
    response = requests.post(
        f"{DAGSTER_URL}/report_asset_materialization/staging/complaints_311",
        json={"metadata": metadata, "description": description},
        timeout=30,
    )
    response.raise_for_status()


@flow(name="complaints-311", log_prints=True)
def complaints_311() -> None:
    since = get_watermark()
    rows = fetch_311(since)
    stats = upsert_staging(rows)
    stats["fetched"] = len(rows)
    stats["deleted"] = reconcile_deleted()
    report_to_dagster(stats)
    print(
        f"loaded {stats['fetched']} rows: {stats['inserted']} inserted, "
        f"{stats['updated']} updated, {stats['deleted']} deleted"
    )


if __name__ == "__main__":
    complaints_311()
