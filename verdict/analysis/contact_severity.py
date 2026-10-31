def contact_severity(telemetry_list):
    speeds = [float(row["Speed"]) for row in telemetry_list]

    biggest_drop = 0
    for i in range(1, len(speeds)):
        drop = speeds[i] - speeds[i - 1]
        if drop < biggest_drop:
            biggest_drop = drop

    return abs(biggest_drop)


if __name__ == "__main__":
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), "..", "telemetry"))
    from snapshot import get_telemetry_snapshot

    s1 = get_telemetry_snapshot(2024, "Abu Dhabi", 77, 20, "17:49", 4)
    s2 = get_telemetry_snapshot(2024, "Abu Dhabi", 81, 43, "17:08", 4)

    print("test.pdf - contact severity (own car):", contact_severity(s1["own_car_telemetry"]))
    print("car81.pdf - contact severity (own car):", contact_severity(s2["own_car_telemetry"]))
