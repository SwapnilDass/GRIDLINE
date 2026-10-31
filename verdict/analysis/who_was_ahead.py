import pandas as pd

def find_closest_sample(telemetry_list, target_time):
    target = pd.Timestamp(target_time)
    return min(telemetry_list, key=lambda row: abs(pd.Timestamp(row["Date"]) - target))

def who_was_ahead(snapshot):
    moment = snapshot["incident_moment_utc"]

    own_sample = find_closest_sample(snapshot["own_car_telemetry"], moment)
    other_sample = find_closest_sample(snapshot["other_car_telemetry"], moment)

    own_distance = float(own_sample["Distance"])
    other_distance = float(other_sample["Distance"])

    if own_distance > other_distance:
        return "own_car"
    else:
        return "other_car"


if __name__ == "__main__":
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), "..", "telemetry"))
    from snapshot import get_telemetry_snapshot

    s1 = get_telemetry_snapshot(2024, "Abu Dhabi", 77, 20, "17:49", 4)
    s2 = get_telemetry_snapshot(2024, "Abu Dhabi", 81, 43, "17:08", 4)

    print("test.pdf (Bottas/Car20, grid drop penalty) - who was ahead:", who_was_ahead(s1))
    print("car81.pdf (Piastri/Car43, time penalty) - who was ahead:", who_was_ahead(s2))
