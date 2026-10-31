import sys
import os
sys.path.append(os.path.dirname(__file__))
from who_was_ahead import find_closest_sample


def closing_speed(snapshot):
    moment = snapshot["incident_moment_utc"]

    own_sample = find_closest_sample(snapshot["own_car_telemetry"], moment)
    other_sample = find_closest_sample(snapshot["other_car_telemetry"], moment)

    own_speed = float(own_sample["Speed"])
    other_speed = float(other_sample["Speed"])

    return other_speed - own_speed


if __name__ == "__main__":
    sys.path.append(os.path.join(os.path.dirname(__file__), "..", "telemetry"))
    from snapshot import get_telemetry_snapshot

    s1 = get_telemetry_snapshot(2024, "Abu Dhabi", 77, 20, "17:49", 4)
    s2 = get_telemetry_snapshot(2024, "Abu Dhabi", 81, 43, "17:08", 4)

    print("test.pdf (Bottas/Car20) - closing speed (other - own):", closing_speed(s1))
    print("car81.pdf (Piastri/Car43) - closing speed (other - own):", closing_speed(s2))
