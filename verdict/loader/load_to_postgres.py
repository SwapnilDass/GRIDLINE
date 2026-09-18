import json
import glob
import os
import psycopg2

DB_URL = "postgresql://gridline:gridline@localhost:5433/gridline_db"


def load_one(cur, json_path):
    with open(json_path) as f:
        data = json.load(f)

    filename = os.path.basename(json_path)
    source_file = filename.replace(".json", ".pdf")


    cur.execute("""
        INSERT INTO penalty_decisions
            (source_file, driver_text, own_car, other_car, competitor,
             session, incident_time, turn, fact, infringement, decision,
             penalty_points, reason)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_file) DO NOTHING
    """, (
        source_file,
        data.get("Driver"),
        data.get("own_car"),
        data.get("other_car"),
        data.get("Competitor"),
        data.get("Session"),
        data.get("Time"),
        data.get("turn"),
        data.get("Fact"),
        data.get("Infringement"),
        data.get("Decision"),
        data.get("penalty_points"),
        data.get("Reason"),
    ))


if __name__ == "__main__":
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    for json_path in glob.glob("verdict/data/parsed/*.json"):
        load_one(cur, json_path)
        print(f"Loaded {json_path}")

    conn.commit()
    cur.close()
    conn.close()
