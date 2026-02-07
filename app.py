from __future__ import annotations

from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import psycopg
import json
import csv
import io
import re
import os
import random
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

app = FastAPI()

# ----------------------------
# CORS
# ----------------------------
_cors_origins_raw = os.getenv("CORS_ALLOW_ORIGINS", "*")
_cors_origins = [o.strip() for o in _cors_origins_raw.split(",") if o.strip()] if _cors_origins_raw != "*" else ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------
# Globals
# ----------------------------
TARGET_DB_URL = None
SOURCE_DB_URL = None
TARGET_DB_CONN = None
SOURCE_DB_CONN = None
JOBS = {}

IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# ============================
# Utilities
# ============================

def validate_identifiers(items):
    for i in items:
        if not IDENT_RE.match(i):
            raise ValueError(f"Invalid SQL identifier: {i}")


def parse_list(raw: str):
    return [c.strip() for c in raw.split(",") if c.strip()]


def generate_job_id():
    for _ in range(20):
        job_id = str(random.randint(1000, 9999))
        if job_id not in JOBS:
            return job_id
    raise RuntimeError("Failed to generate unique job id")


def utc_now():
    return datetime.now(timezone.utc)


def to_iso(dt: datetime | None):
    return dt.isoformat() if dt else None


def purge_jobs(retention: timedelta = timedelta(hours=2)):
    now = utc_now()
    to_delete = []
    for job_id, job in JOBS.items():
        status = job.get("status")
        if status == "processing":
            continue
        finished_at_raw = job.get("finished_at")
        try:
            finished_at = datetime.fromisoformat(finished_at_raw) if finished_at_raw else None
        except Exception:
            finished_at = None

        if not finished_at:
            to_delete.append(job_id)
            continue

        if now - finished_at > retention:
            to_delete.append(job_id)

    for job_id in to_delete:
        JOBS.pop(job_id, None)


def job_public_view(job_id: str, job: dict):
    return {
        "job_id": job_id,
        "status": job.get("status"),
        "filename": job.get("filename"),
        "label": job.get("label"),
        "created_at": job.get("created_at"),
        "updated_at": job.get("updated_at"),
        "finished_at": job.get("finished_at"),
        "rows_total": job.get("rows_total"),
        "rows_inserted": job.get("rows_inserted"),
        "rows_skipped": job.get("rows_skipped"),
        "error": job.get("error"),
        "progress": job.get("progress"),
        "rows_processed": job.get("rows_processed"),
    }


def get_csv_header_columns(file_bytes: bytes):
    text = file_bytes.decode("utf-8-sig", errors="strict")
    first_line = text.splitlines()[0] if text else ""
    if not first_line.strip():
        raise ValueError("CSV file is empty")
    reader = csv.reader(io.StringIO(first_line))
    header = next(reader, [])
    cols = [c.strip() for c in header if c and c.strip()]
    if not cols:
        raise ValueError("CSV header is missing or empty")
    return cols


async def get_target_db_conn():
    global TARGET_DB_CONN
    if not TARGET_DB_URL:
        raise HTTPException(status_code=400, detail="Target database not configured")
    if TARGET_DB_CONN is None or TARGET_DB_CONN.closed:
        TARGET_DB_CONN = await psycopg.AsyncConnection.connect(TARGET_DB_URL)
    return TARGET_DB_CONN


async def get_source_db_conn():
    global SOURCE_DB_CONN
    if not SOURCE_DB_URL:
        raise HTTPException(status_code=400, detail="Source database not configured")
    if SOURCE_DB_CONN is None or SOURCE_DB_CONN.closed:
        SOURCE_DB_CONN = await psycopg.AsyncConnection.connect(SOURCE_DB_URL)
    return SOURCE_DB_CONN


async def close_target_db_conn():
    global TARGET_DB_CONN
    if TARGET_DB_CONN and not TARGET_DB_CONN.closed:
        await TARGET_DB_CONN.close()
        TARGET_DB_CONN = None


async def close_source_db_conn():
    global SOURCE_DB_CONN
    if SOURCE_DB_CONN and not SOURCE_DB_CONN.closed:
        await SOURCE_DB_CONN.close()
        SOURCE_DB_CONN = None


# Legacy alias for backward compatibility
async def get_db_conn():
    return await get_target_db_conn()


async def get_identity_always_columns(conn, table, schema="public"):
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
          AND table_schema = %s
          AND is_identity = 'YES'
          AND identity_generation = 'ALWAYS'
    """

    async with conn.cursor() as cur:
        await cur.execute(sql, (table, schema))
        rows = await cur.fetchall()
    return {r[0] for r in rows}

# ============================
# Core DB Logic
# ============================

async def copy_csv_with_pk_dedup(conn, table, columns, pk_columns, file_bytes):
    cols = ", ".join(columns)
    temp_table = f"{table}_staging"

    identity_cols = await get_identity_always_columns(conn, table, "public")
    has_identity_values = any(c in identity_cols for c in columns)

    async with conn.cursor() as cur:
        await cur.execute(f"""
            CREATE TEMP TABLE {temp_table}
            (LIKE {table} INCLUDING ALL)
            ON COMMIT DROP
        """)

        async with cur.copy(
            f"COPY {temp_table} ({cols}) FROM STDIN WITH CSV HEADER"
        ) as copy:
            await copy.write(file_bytes)


        await cur.execute(f"SELECT COUNT(*) FROM {temp_table}")
        total_rows = (await cur.fetchone())[0]

        if pk_columns:
            pk = ", ".join(pk_columns)
            await cur.execute(f"""
                INSERT INTO {table} ({cols}) OVERRIDING SYSTEM VALUE
                SELECT {cols} FROM {temp_table}
                ON CONFLICT ({pk}) DO NOTHING
                RETURNING 1
            """)
        else:
            await cur.execute(f"""
                INSERT INTO {table} ({cols}) OVERRIDING SYSTEM VALUE
                SELECT {cols} FROM {temp_table}
                RETURNING 1
            """)

        inserted = cur.rowcount

    await conn.commit()
    return total_rows, inserted


async def insert_json_with_pk_dedup(conn, table, columns, pk_columns, data):
    cols = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))

    identity_cols = await get_identity_always_columns(conn, table, "public")
    has_identity_values = any(c in identity_cols for c in columns)
    overriding = " OVERRIDING SYSTEM VALUE" if has_identity_values else ""

    if pk_columns:
        pk = ", ".join(pk_columns)
        sql = f"""
            INSERT INTO {table} ({cols}){overriding}
            VALUES ({placeholders})
            ON CONFLICT ({pk}) DO NOTHING
            RETURNING 1
        """
    else:
        sql = f"""
            INSERT INTO {table} ({cols}){overriding}
            VALUES ({placeholders})
            RETURNING 1
        """

    rows = [tuple(obj[col] for col in columns) for obj in data]

    async with conn.cursor() as cur:
        await cur.executemany(sql, rows)
        inserted = cur.rowcount

    await conn.commit()
    return len(rows), inserted


async def get_table_schema(conn, table, schema="public"):
    sql = """
        SELECT
            c.column_name,
            tc.constraint_type
        FROM information_schema.columns c
        LEFT JOIN information_schema.key_column_usage kcu
            ON c.table_name = kcu.table_name
           AND c.column_name = kcu.column_name
           AND c.table_schema = kcu.table_schema
        LEFT JOIN information_schema.table_constraints tc
            ON kcu.constraint_name = tc.constraint_name
           AND tc.constraint_type = 'PRIMARY KEY'
        WHERE c.table_name = %s
          AND c.table_schema = %s
        ORDER BY c.ordinal_position;
    """

    async with conn.cursor() as cur:
        await cur.execute(sql, (table, schema))
        rows = await cur.fetchall()

    attributes, pk = [], []
    for col, constraint in rows:
        attributes.append(col)
        if constraint == "PRIMARY KEY":
            pk.append(col)

    return attributes, pk



async def create_table_from_sql(conn, create_sql: str):
    if not create_sql.strip().lower().startswith("create table"):
        raise ValueError("Only CREATE TABLE statements are allowed")

    async with conn.cursor() as cur:
        await cur.execute(create_sql)

    await conn.commit()

    return {
        "status": "created",
        "message": "Table created successfully"
    }



# ============================
# Background Job
# ============================

async def process_upload(job_id, table, columns, pk_columns, file_bytes, filename):
    conn = None
    try:
        conn = await get_db_conn()

        if filename.endswith(".csv"):
            csv_columns = get_csv_header_columns(file_bytes)
            requested = set(columns)
            effective_columns = [c for c in csv_columns if c in requested]
            if pk_columns:
                missing_pk = [c for c in pk_columns if c not in effective_columns]
                if missing_pk:
                    raise ValueError(
                        "CSV is missing primary key column(s): " + ", ".join(missing_pk)
                    )

            total, inserted = await copy_csv_with_pk_dedup(
                conn, table, effective_columns, pk_columns, file_bytes
            )
        elif filename.endswith(".json"):
            data = json.loads(file_bytes.decode("utf-8"))
            total, inserted = await insert_json_with_pk_dedup(
                conn, table, columns, pk_columns, data
            )
        else:
            raise ValueError("Only CSV or JSON supported")

        job = JOBS.get(job_id, {})
        job.update({
            "status": "completed",
            "rows_total": total,
            "rows_inserted": inserted,
            "rows_skipped": total - inserted,
            "updated_at": to_iso(utc_now()),
            "finished_at": to_iso(utc_now()),
            "progress": 100,
        })
        JOBS[job_id] = job

    except Exception as e:
        job = JOBS.get(job_id, {})
        job.update({
            "status": "failed",
            "error": str(e),
            "updated_at": to_iso(utc_now()),
            "finished_at": to_iso(utc_now()),
        })
        JOBS[job_id] = job

    finally:
        if conn:
            await conn.close()


# ============================
# APIs
# ============================

@app.post("/save-db")
async def save_db(payload: dict):
    global TARGET_DB_URL, TARGET_DB_CONN
    db_url = payload.get("database_url")

    if not db_url:
        raise HTTPException(status_code=400, detail="database_url required")

    # Close existing connection if URL changed
    if TARGET_DB_URL != db_url:
        await close_target_db_conn()

    TARGET_DB_URL = db_url
    return {"status": "saved"}


@app.post("/upload-data")
async def upload_data(
    background_tasks: BackgroundTasks,
    table: str = Form(...),
    columns: str = Form(...),
    primary_key: str = Form(""),
    file: UploadFile = File(...)
):
    columns_list = parse_list(columns)
    pk_list = parse_list(primary_key)

    validate_identifiers([table])
    validate_identifiers(columns_list)
    validate_identifiers(pk_list)

    if not columns_list:
        raise HTTPException(status_code=400, detail="columns is required")

    conn = None
    try:
        conn = await get_db_conn()
        db_attributes, db_pk = await get_table_schema(conn, table, "public")
    finally:
        if conn:
            await conn.close()

    if not db_attributes:
        raise HTTPException(status_code=404, detail="table not found")

    db_attr_set = set(db_attributes)
    bad_cols = [c for c in columns_list if c not in db_attr_set]
    if bad_cols:
        raise HTTPException(
            status_code=400,
            detail="Invalid column(s) for table: " + ", ".join(bad_cols)
        )

    # If primary_key is provided, enforce it matches the table PK.
    # If empty, we allow inserts without deduplication (auto mode).
    if pk_list:
        if set(pk_list) != set(db_pk):
            raise HTTPException(
                status_code=400,
                detail=(
                    "primary_key does not match table primary key. "
                    f"Expected: {', '.join(db_pk) if db_pk else 'None'}"
                ),
            )

        missing_pk_in_cols = [c for c in pk_list if c not in columns_list]
        if missing_pk_in_cols:
            raise HTTPException(
                status_code=400,
                detail="primary_key must be included in columns: " + ", ".join(missing_pk_in_cols)
            )

    # Smart deduplication: if primary_key is not provided, auto-detect from target table.
    if not pk_list:
        conn = await get_db_conn()
        try:
            _, detected_pk = await get_table_schema(conn, table, "public")
        finally:
            await conn.close()
        if detected_pk:
            pk_list = detected_pk

    file_bytes = await file.read()
    filename = file.filename.lower()

    purge_jobs()
    job_id = generate_job_id()
    now = utc_now()
    JOBS[job_id] = {
        "status": "processing",
        "created_at": to_iso(now),
        "updated_at": to_iso(now),
        "finished_at": None,
        "filename": file.filename,
        "label": f"Upload to {table}",
        "progress": 0,
    }

    background_tasks.add_task(
        process_upload,
        job_id,
        table,
        columns_list,
        pk_list,
        file_bytes,
        filename
    )

    return {"status": "accepted", "job_id": job_id}


@app.get("/job-status/{job_id}")
async def job_status(job_id: str):
    purge_jobs()
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return job_public_view(job_id, job)


@app.get("/table-schema")
async def table_schema(table: str, schema: str = "public"):
    validate_identifiers([table])
    conn = await get_db_conn()
    attributes, pk = await get_table_schema(conn, table, schema)
    await conn.close()

    if not attributes:
        raise HTTPException(status_code=404, detail="table not found")

    return {"table": table, "attributes": attributes, "primary_key": pk}


@app.get("/jobs/running")
async def get_running_jobs():
    purge_jobs()
    running_jobs = [
        {"job_id": job_id, "status": job["status"]}
        for job_id, job in JOBS.items()
        if job.get("status") == "processing"
    ]

    return {
        "count": len(running_jobs),
        "jobs": running_jobs
    }


@app.get("/jobs/recent")
async def get_recent_jobs(hours: int = 2):
    # Keep completed/failed for 2 hours after finished, always keep processing.
    purge_jobs(retention=timedelta(hours=hours))
    jobs = [job_public_view(job_id, job) for job_id, job in JOBS.items()]

    def sort_key(j):
        return j.get("updated_at") or j.get("created_at") or ""

    jobs.sort(key=sort_key, reverse=True)
    return {
        "count": len(jobs),
        "jobs": jobs,
    }



@app.post("/create-table")
async def create_table_api(payload: dict):
    create_sql = payload.get("create_sql")

    if not create_sql:
        raise HTTPException(status_code=400, detail="create_sql is required")

    conn = None
    try:
        conn = await get_db_conn()
        result = await create_table_from_sql(conn, create_sql)
        return result

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    finally:
        if conn:
            await conn.close()





@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/transfer/start")
async def start_transfer(background_tasks: BackgroundTasks, req: TransferStartRequest = Body(...)):
    purge_jobs()
    job_id = generate_job_id()
    now = utc_now()

    try:
        validate_identifiers([req.target_table])
        validate_identifiers(req.target_columns)
        validate_identifiers(req.primary_key)
        validate_identifiers(list(req.source_to_target_mapping.keys()))
        validate_identifiers(list(req.source_to_target_mapping.values()))
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))

    JOBS[job_id] = {
        "status": "processing",
        "created_at": to_iso(now),
        "updated_at": to_iso(now),
        "finished_at": None,
        "filename": None,
        "label": f"Transfer to {req.target_table}",
        "progress": 0,
        "rows_processed": 0,
        "rows_inserted": 0,
        "cancel_requested": False,
    }

    background_tasks.add_task(
        process_transfer_job,
        job_id,
        req.query,
        req.target_table,
        req.target_columns,
        req.source_to_target_mapping,
        req.primary_key,
        req.chunk_size,
        req.on_conflict_do_nothing,
    )

    return {"status": "accepted", "job_id": job_id}


@app.post("/transfer/cancel/{job_id}")
async def cancel_transfer(job_id: str):
    purge_jobs()
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    if job.get("status") != "processing":
        return {"status": "ignored", "job_id": job_id, "job_status": job.get("status")}
    job["cancel_requested"] = True
    job["updated_at"] = to_iso(utc_now())
    JOBS[job_id] = job
    return {"status": "ok", "job_id": job_id}


# ============================
# Source Database APIs
# ============================

@app.post("/connect-source-db")
async def connect_source_db(payload: dict):
    global SOURCE_DB_URL, SOURCE_DB_CONN
    db_url = payload.get("database_url")

    if not db_url:
        raise HTTPException(status_code=400, detail="database_url required")

    # Close existing connection if URL changed
    if SOURCE_DB_URL != db_url:
        await close_source_db_conn()

    SOURCE_DB_URL = db_url

    # Test the connection
    try:
        conn = await get_source_db_conn()
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1")
            await cur.fetchone()
        return {"status": "connected", "database_url": mask_url(db_url)}
    except Exception as e:
        SOURCE_DB_URL = None
        await close_source_db_conn()
        raise HTTPException(status_code=400, detail=f"Failed to connect: {str(e)}")


@app.post("/disconnect-source-db")
async def disconnect_source_db():
    global SOURCE_DB_URL
    await close_source_db_conn()
    SOURCE_DB_URL = None
    return {"status": "disconnected"}


@app.post("/execute-query")
async def execute_query(payload: dict):
    query = payload.get("query")
    params = payload.get("params", [])
    limit = payload.get("limit", 1000)

    if not query:
        raise HTTPException(status_code=400, detail="query is required")

    # Security: Only allow SELECT queries
    query_stripped = query.strip().upper()
    if not query_stripped.startswith("SELECT"):
        raise HTTPException(status_code=400, detail="Only SELECT queries are allowed")

    # Prevent dangerous keywords
    forbidden_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE", "EXEC", "EXECUTE"]
    for keyword in forbidden_keywords:
        if keyword in query_stripped:
            raise HTTPException(status_code=400, detail=f"Query contains forbidden keyword: {keyword}")

    try:
        conn = await get_source_db_conn()
        async with conn.cursor() as cur:
            await cur.execute(query, params)
            rows = await cur.fetchall()
            columns = [desc[0] for desc in cur.description] if cur.description else []

            # Convert to list of dicts, handling binary data
            results = []
            for row in rows[:limit]:
                row_dict = {}
                for col, val in zip(columns, row):
                    row_dict[col] = serialize_value(val)
                results.append(row_dict)

            return {
                "status": "success",
                "columns": columns,
                "rows": results,
                "total_count": len(rows),
                "returned_count": len(results)
            }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Query execution failed: {str(e)}")


def serialize_value(val):
    """Convert Python values to JSON-serializable format, handling bytes."""
    if val is None:
        return None
    if isinstance(val, bytes):
        # Convert bytes to base64 string
        import base64
        return f"base64:{base64.b64encode(val).decode('ascii')}"
    if isinstance(val, (list, tuple)):
        return [serialize_value(v) for v in val]
    if isinstance(val, dict):
        return {k: serialize_value(v) for k, v in val.items()}
    return val


@app.get("/source-db-status")
async def source_db_status():
    return {
        "connected": SOURCE_DB_CONN is not None and not SOURCE_DB_CONN.closed,
        "database_url": mask_url(SOURCE_DB_URL) if SOURCE_DB_URL else None
    }


def mask_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.username}:***@{parsed.hostname}:{parsed.port or 5432}{parsed.path}"
    except:
        return url[:30] + "..." if len(url) > 30 else url


class TransferStartRequest(BaseModel):
    query: str = Field(..., min_length=1)
    target_table: str = Field(..., min_length=1)
    target_columns: list[str] = Field(..., min_length=1)
    source_to_target_mapping: dict[str, str] = Field(default_factory=dict)
    primary_key: list[str] = Field(default_factory=list)
    chunk_size: int = Field(default=1000, ge=1, le=100_000)
    on_conflict_do_nothing: bool = True


def _job_mark(job_id: str, **updates):
    job = JOBS.get(job_id, {})
    job.update(updates)
    job["updated_at"] = to_iso(utc_now())
    JOBS[job_id] = job


async def _ensure_db_urls():
    if not SOURCE_DB_URL:
        raise HTTPException(status_code=400, detail="Source database not configured")
    if not TARGET_DB_URL:
        raise HTTPException(status_code=400, detail="Target database not configured")


async def process_transfer_job(
    job_id: str,
    query: str,
    target_table: str,
    target_columns: list[str],
    mapping: dict[str, str],
    pk_columns: list[str],
    chunk_size: int,
    on_conflict_do_nothing: bool,
):
    source_conn = None
    target_conn = None
    try:
        await _ensure_db_urls()

        source_conn = await psycopg.AsyncConnection.connect(SOURCE_DB_URL)
        target_conn = await psycopg.AsyncConnection.connect(TARGET_DB_URL)

        validate_identifiers([target_table])
        validate_identifiers(target_columns)
        validate_identifiers(pk_columns)
        validate_identifiers(list(mapping.keys()))
        validate_identifiers(list(mapping.values()))

        requested_target_cols = list(target_columns)
        effective_mapping = {c: mapping.get(c, c) for c in requested_target_cols}

        placeholders = ", ".join(["%s"] * len(requested_target_cols))
        cols_sql = ", ".join(requested_target_cols)

        conflict_sql = ""
        if on_conflict_do_nothing and pk_columns:
            pk_sql = ", ".join(pk_columns)
            conflict_sql = f" ON CONFLICT ({pk_sql}) DO NOTHING"

        insert_sql = f"INSERT INTO {target_table} ({cols_sql}) VALUES ({placeholders}){conflict_sql}"

        rows_processed = 0
        rows_inserted = 0

        async with target_conn.cursor() as _cur:
            await _cur.execute("BEGIN")

        async with source_conn.cursor() as s_cur:
            await s_cur.execute(query)
            source_cols = [d.name for d in (s_cur.description or [])]
            source_idx = {name: i for i, name in enumerate(source_cols)}

            missing = [
                (t_col, s_col)
                for t_col, s_col in effective_mapping.items()
                if s_col not in source_idx
            ]
            if missing:
                raise ValueError(
                    "Source query is missing required column(s): "
                    + ", ".join([f"{t}->{s}" for t, s in missing])
                )

            async with target_conn.cursor() as t_cur:
                while True:
                    job = JOBS.get(job_id) or {}
                    if job.get("cancel_requested"):
                        raise RuntimeError("cancel_requested")

                    chunk = await s_cur.fetchmany(chunk_size)
                    if not chunk:
                        break

                    payload = [
                        tuple(row[source_idx[effective_mapping[col]]] for col in requested_target_cols)
                        for row in chunk
                    ]

                    await t_cur.executemany(insert_sql, payload)
                    rows_processed += len(payload)

                    try:
                        if t_cur.rowcount and t_cur.rowcount > 0:
                            rows_inserted += t_cur.rowcount
                    except Exception:
                        pass

                    _job_mark(
                        job_id,
                        rows_processed=rows_processed,
                        rows_inserted=rows_inserted,
                        progress=None,
                    )

        await target_conn.commit()

        _job_mark(
            job_id,
            status="completed",
            finished_at=to_iso(utc_now()),
            rows_total=rows_processed,
            rows_inserted=rows_inserted,
            rows_skipped=(rows_processed - rows_inserted) if rows_inserted is not None else None,
            progress=100,
        )

    except Exception as e:
        if target_conn is not None:
            try:
                await target_conn.rollback()
            except Exception:
                pass

        if str(e) == "cancel_requested":
            _job_mark(
                job_id,
                status="canceled",
                error="canceled",
                finished_at=to_iso(utc_now()),
            )
        else:
            _job_mark(
                job_id,
                status="failed",
                error=str(e),
                finished_at=to_iso(utc_now()),
            )
    finally:
        if source_conn is not None and not source_conn.closed:
            await source_conn.close()
        if target_conn is not None and not target_conn.closed:
            await target_conn.close()


# ----------------------------
# Local fallback
# ----------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
