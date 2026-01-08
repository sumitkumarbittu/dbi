from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg
import json
import re
import os
import random

app = FastAPI()

# ----------------------------
# CORS
# ----------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------
# Globals
# ----------------------------
DATABASE_URL = None
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


async def get_db_conn():
    if not DATABASE_URL:
        raise HTTPException(status_code=400, detail="Database not configured")
    return await psycopg.AsyncConnection.connect(DATABASE_URL)

# ============================
# Core DB Logic
# ============================

async def copy_csv_with_pk_dedup(conn, table, columns, pk_columns, file_bytes):
    cols = ", ".join(columns)
    pk = ", ".join(pk_columns)
    temp_table = f"{table}_staging"

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

        await cur.execute(f"""
            INSERT INTO {table} ({cols})
            SELECT {cols} FROM {temp_table}
            ON CONFLICT ({pk}) DO NOTHING
            RETURNING 1
        """)

        inserted = cur.rowcount

    await conn.commit()
    return total_rows, inserted


async def insert_json_with_pk_dedup(conn, table, columns, pk_columns, data):
    cols = ", ".join(columns)
    pk = ", ".join(pk_columns)
    placeholders = ", ".join(["%s"] * len(columns))

    sql = f"""
        INSERT INTO {table} ({cols})
        VALUES ({placeholders})
        ON CONFLICT ({pk}) DO NOTHING
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
            total, inserted = await copy_csv_with_pk_dedup(
                conn, table, columns, pk_columns, file_bytes
            )
        elif filename.endswith(".json"):
            data = json.loads(file_bytes.decode("utf-8"))
            total, inserted = await insert_json_with_pk_dedup(
                conn, table, columns, pk_columns, data
            )
        else:
            raise ValueError("Only CSV or JSON supported")

        JOBS[job_id] = {
            "status": "completed",
            "rows_total": total,
            "rows_inserted": inserted,
            "rows_skipped": total - inserted
        }

    except Exception as e:
        JOBS[job_id] = {"status": "failed", "error": str(e)}

    finally:
        if conn:
            await conn.close()


# ============================
# APIs
# ============================

@app.post("/save-db")
async def save_db(payload: dict):
    global DATABASE_URL
    db_url = payload.get("database_url")

    if not db_url:
        raise HTTPException(status_code=400, detail="database_url required")

    DATABASE_URL = db_url
    return {"status": "saved"}


@app.post("/upload-data")
async def upload_data(
    background_tasks: BackgroundTasks,
    table: str = Form(...),
    columns: str = Form(...),
    primary_key: str = Form(...),
    file: UploadFile = File(...)
):
    columns_list = parse_list(columns)
    pk_list = parse_list(primary_key)

    validate_identifiers([table])
    validate_identifiers(columns_list)
    validate_identifiers(pk_list)

    file_bytes = await file.read()
    filename = file.filename.lower()

    job_id = generate_job_id()
    JOBS[job_id] = {"status": "processing"}

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
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return job


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
    running_jobs = [
        {"job_id": job_id, "status": job["status"]}
        for job_id, job in JOBS.items()
        if job.get("status") == "processing"
    ]

    return {
        "count": len(running_jobs),
        "jobs": running_jobs
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


# ----------------------------
# Local fallback
# ----------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
