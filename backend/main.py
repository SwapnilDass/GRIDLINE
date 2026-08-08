from dotenv import load_dotenv
load_dotenv()


import asyncio

from fastapi import FastAPI, HTTPException
import os
import psycopg2
import psycopg2.extras
import httpx
from fastapi.middleware.cors import CORSMiddleware

import time

_cache = {"data": None, "timestamp": 0}
CACHE_TTL = 60  # seconds

_session_cache = {"data": None, "timestamp": 0}
SESSION_CACHE_TTL = 3600  # 1 hour


# Create the FastAPI app instance
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:5174"],
    allow_methods=["*"],
    allow_headers=["*"],
)

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



# Fetches the latest driver positions from the OpenF1 public API.
# Uses session_key=latest to always target the most recent F1 session.
# Returns raw position data as JSON — no database involved.

@app.get("/f1/live")
async def f1_live():
    now = time.time()
    if _cache["data"] and (now - _cache["timestamp"]) < CACHE_TTL:
        return _cache["data"]
    
    async with httpx.AsyncClient() as client:
        pos_res = await client.get("https://api.openf1.org/v1/position?session_key=latest")

        drv_res = await client.get("https://api.openf1.org/v1/drivers?session_key=latest")

        lap_res = await client.get("https://api.openf1.org/v1/laps?session_key=latest")



        now_ses = time.time()
        if _session_cache["data"] and (now_ses - _session_cache["timestamp"]) < SESSION_CACHE_TTL:
            ses_data = _session_cache["data"]
        else:
            ses_res = await client.get("https://api.openf1.org/v1/sessions?session_key=latest")
            if ses_res.status_code == 200:
                ses_data = ses_res.json()
                _session_cache["data"] = ses_data
                _session_cache["timestamp"] = time.time()
            else:
                ses_data = _session_cache["data"] or []



        int_res =  await client.get("https://api.openf1.org/v1/intervals?session_key=latest")

        pit_res = await client.get("https://api.openf1.org/v1/pit?session_key=latest")

        stint_res = await client.get("https://api.openf1.org/v1/stints?session_key=latest")


    # Log status codes so we can see which one fails
    print(f"pos:{pos_res.status_code} drv:{drv_res.status_code} lap:{lap_res.status_code}")


    for res in [pos_res, drv_res, lap_res]:
        if res.status_code != 200:
            raise HTTPException(status_code=res.status_code, detail=f"OpenF1 error: {res.status_code} from {res.url}")

    int_data = int_res.json() if int_res.status_code == 200 else []
    pit_data = pit_res.json() if pit_res.status_code == 200 else []
    stint_data = stint_res.json() if stint_res.status_code == 200 else []

    # Keep only the latest position entry per driver
    latest = {}
    for entry in pos_res.json():
        driver = entry["driver_number"]
        if driver not in latest or entry["date"] > latest[driver]["date"]:
            latest[driver] = entry

    # Build a lookup map: driver_number -> driver info
    driver_info = {d["driver_number"]: d for d in drv_res.json()}

    #Latest Lap per driver (Highest Lap Number)
    latest_lap = {}
    for lap in lap_res.json():
        driver = lap["driver_number"]
        if driver not in latest_lap or lap["lap_number"] > latest_lap[driver]["lap_number"]:
            latest_lap[driver] = lap

    best_lap = {}
    for lap in lap_res.json():
        driver = lap["driver_number"]
        duration = lap.get("lap_duration")
        if duration and (driver not in best_lap or duration < best_lap[driver]):
            best_lap[driver] = duration

    #Latest Interval Per driver
    latest_interval = {}
    for entry in int_data:
        driver = entry["driver_number"]
        if driver not in latest_interval or entry["date"] > latest_interval[driver]["date"]:
            latest_interval[driver] = entry

    #Pit stops
    pit_stops = {}
    for entry in pit_data:
        driver = entry["driver_number"]
        pit_stops[driver] = pit_stops.get(driver, 0) + 1

    # Current tyre compound per driver (highest stint_number = current stint)
    current_tyre = {}
    for entry in stint_data:
        driver = entry["driver_number"]
        if driver not in current_tyre or entry["stint_number"] > current_tyre[driver]["stint_number"]:
            current_tyre[driver] = entry


    # Merge position + driver info + Lap Info
    result = []
    for driver_number, pos in latest.items():
        info = driver_info.get(driver_number, {})
        lap = latest_lap.get(driver_number, {})
        interval = latest_interval.get(driver_number, {})
        tyre = current_tyre.get(driver_number, {})
        result.append({
            "position": pos["position"],
            "driver_number": driver_number,
            "name_acronym": info.get("name_acronym"),
            "full_name": info.get("full_name"),
            "team_name": info.get("team_name"),
            "team_colour": info.get("team_colour"),
            "headshot_url": info.get("headshot_url"),
            "lap_number": lap.get("lap_number"),
            "last_lap": lap.get("lap_duration"),
            "best_lap": best_lap.get(driver_number),
            "gap_to_leader": interval.get("gap_to_leader"),
            "interval": interval.get("interval"),
            "pit_stops": pit_stops.get(driver_number, 0),
            "compound": tyre.get("compound")
        })

    
        session = ses_data[0] if isinstance(ses_data, list) and ses_data else ses_data if isinstance(ses_data, dict) else {}


    response = {
        "session": {
            "name": session.get("session_name"),
            "circuit": session.get("circuit_short_name"),
            "country": session.get("country_name"),
            "year": session.get("year"),
        },
        "drivers": sorted(result, key=lambda x: x["position"])
    }
    _cache["data"] = response
    _cache["timestamp"] = time.time()
    return response

@app.get("/ergast/drivers")
def ergast_drivers():
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT driver_id, forename, surname, code, nationality, dob
                    FROM drivers
                    ORDER BY surname ASC
                """)
                rows = cur.fetchall()
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    return {"drivers": [dict(r) for r in rows]}


#pushed
#pushed