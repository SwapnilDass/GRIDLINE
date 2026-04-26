from fastapi import FastAPI, HTTPException
import os
import psycopg2
import psycopg2.extras

# Create the FastAPI app instance
app = FastAPI()


def get_conn():
    """
    Build and return a PostgreSQL connection using DATABASE_URL from env vars.

    Expected format:
    DATABASE_URL=postgresql://user:password@localhost:5432/gridline
    """
    # Read DB connection string from environment
    database_url = os.getenv("DATABASE_URL")

    # Fail fast if env var is missing
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")

    # Open and return a psycopg2 connection
    # A psycopg connection is the active session your Python code uses to communicate with a PostgreSQL database and execute SQL queries.
    return psycopg2.connect(database_url)


@app.get("/health")
def health():
    # Simple health check endpoint
    return {"status": "ok"}


@app.get("/live-timing/{race_id}")
def live_timing(race_id: int):
    """
    Return the latest timing snapshot per driver for a specific race_id.
    """
    # SQL:
    # 1) Find latest captured_at for this race
    # 2) Join that snapshot to entries/drivers/constructors
    # 3) Return ordered by position
    sql = """
    WITH latest_ts AS (
        SELECT race_id, MAX(captured_at) AS max_captured_at
        FROM live_timing_snapshots
        WHERE race_id = %s
        GROUP BY race_id
    )
    SELECT
        lts.position,
        d.code AS driver_code,
        d.full_name AS driver_name,
        c.name AS constructor_name,
        lts.lap,
        lts.gap_to_leader_ms,
        lts.best_lap_ms,
        lts.status,
        lts.captured_at
    FROM live_timing_snapshots lts
    JOIN latest_ts lt
        ON lts.race_id = lt.race_id
        AND lts.captured_at = lt.max_captured_at
    JOIN race_entries re
        ON re.race_entry_id = lts.race_entry_id
    JOIN drivers d
        ON d.driver_id = re.driver_id
    JOIN constructors c
        ON c.constructor_id = re.constructor_id
    WHERE lts.race_id = %s
    ORDER BY lts.position ASC;
    """

    try:
        # Open DB connection and cursor that returns rows as dicts
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # race_id is used twice in the query (%s placeholders)
                cur.execute(sql, (race_id, race_id))
                rows = cur.fetchall()
    except RuntimeError as e:
        # Config/connection setup issue (e.g., missing DATABASE_URL)
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        # Any other database/runtime error
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    # If no data found for this race, return 404
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No live timing found for race_id={race_id}",
        )

    # API response payload
    return {
        "race_id": race_id,
        "drivers": rows,
    }