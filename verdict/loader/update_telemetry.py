
import sys
import os
import json
import psycopg2

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "telemetry"))
from snapshot import get_telemetry_snapshot

DB_URL = "postgresql://gridline:gridline@localhost:5433/gridline_db"


def update_decision_telemetry(source_file, race_year, race_event, own_car, other_car, incident_time, utc_offset_hours):
    snapshot = get_telemetry_snapshot(race_year, race_event, own_car, other_car, incident_time, utc_offset_hours)

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("""
        UPDATE penalty_decisions
        SET race_year = %s, race_event = %s, telemetry_snapshot = %s
        WHERE source_file = %s
    """, (race_year, race_event, json.dumps(snapshot), source_file))
    conn.commit()
    cur.close()
    conn.close()
    print(f"Updated {source_file}")


if __name__ == "__main__":
    update_decision_telemetry(
        source_file="test.pdf",
        race_year=2024,
        race_event="Abu Dhabi",
        own_car=77,
        other_car=20,
        incident_time="17:49",
        utc_offset_hours=4,
    )
