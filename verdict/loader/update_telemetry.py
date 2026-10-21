
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
    decisions = [
        dict(source_file="test.pdf", race_year=2024, race_event="Abu Dhabi",
             own_car=77, other_car=20, incident_time="17:49", utc_offset_hours=4),
        dict(source_file="offence.pdf", race_year=2021, race_event="Hungarian",
             own_car=77, other_car=4, incident_time="15:03", utc_offset_hours=2),
        dict(source_file="test-no-collision.pdf", race_year=2026, race_event="Spanish",
             own_car=5, other_car=43, incident_time="16:19", utc_offset_hours=2),
        dict(source_file="test-no-decision.pdf", race_year=2023, race_event="Abu Dhabi",
             own_car=44, other_car=10, incident_time="17:25", utc_offset_hours=4),
    ]

    for d in decisions:
        try:
            update_decision_telemetry(**d)
        except Exception as e:
            print(f"FAILED on {d['source_file']}: {e}")

