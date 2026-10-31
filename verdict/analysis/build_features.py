import sys
import os
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "telemetry"))

from who_was_ahead import who_was_ahead
from closing_speed import closing_speed
from contact_severity import contact_severity
from snapshot import get_telemetry_snapshot

decisions = [
    dict(source_file="test.pdf", race_year=2024, race_event="Abu Dhabi",
         own_car=77, other_car=20, incident_time="17:49", utc_offset_hours=4, penalty_points=3),
    dict(source_file="offence.pdf", race_year=2021, race_event="Hungarian",
         own_car=77, other_car=4, incident_time="15:03", utc_offset_hours=2, penalty_points=2),
    dict(source_file="2024_abudhabi_car1.pdf", race_year=2024, race_event="Abu Dhabi",
         own_car=1, other_car=81, incident_time="17:03", utc_offset_hours=4, penalty_points=2),
    dict(source_file="2024_abudhabi_car81.pdf", race_year=2024, race_event="Abu Dhabi",
         own_car=81, other_car=43, incident_time="17:08", utc_offset_hours=4, penalty_points=2),
    dict(source_file="2021_hungarian_car18.pdf", race_year=2021, race_event="Hungarian",
         own_car=18, other_car=16, incident_time="15:03", utc_offset_hours=2, penalty_points=2),
]

features = []
for d in decisions:
    snapshot = get_telemetry_snapshot(
        d["race_year"], d["race_event"], d["own_car"], d["other_car"],
        d["incident_time"], d["utc_offset_hours"]
    )
    features.append({
        "source_file": d["source_file"],
        "who_was_ahead": who_was_ahead(snapshot),
        "closing_speed": round(closing_speed(snapshot), 1),
        "contact_severity": round(contact_severity(snapshot["own_car_telemetry"]), 1),
        "penalty_points": d["penalty_points"],
    })

for f in features:
    print(f)
