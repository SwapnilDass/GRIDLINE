import itertools

# From build_features.py's actual output
features = [
    {"source_file": "test.pdf", "who_was_ahead": "own_car", "closing_speed": 50.1, "contact_severity": 33.2, "penalty_points": 3},
    {"source_file": "offence.pdf", "who_was_ahead": "other_car", "closing_speed": 73.0, "contact_severity": 48.2, "penalty_points": 2},
    {"source_file": "2024_abudhabi_car1.pdf", "who_was_ahead": "own_car", "closing_speed": 100.0, "contact_severity": 28.0, "penalty_points": 2},
    {"source_file": "2024_abudhabi_car81.pdf", "who_was_ahead": "other_car", "closing_speed": -51.0, "contact_severity": 85.2, "penalty_points": 2},
    {"source_file": "2021_hungarian_car18.pdf", "who_was_ahead": "own_car", "closing_speed": 47.0, "contact_severity": 19.9, "penalty_points": 2},
]


def compare(f1, f2):
    speed_diff = abs(f1["closing_speed"] - f2["closing_speed"])
    severity_diff = abs(f1["contact_severity"] - f2["contact_severity"])
    same_who_ahead = f1["who_was_ahead"] == f2["who_was_ahead"]
    points_diff = abs(f1["penalty_points"] - f2["penalty_points"])
    return speed_diff, severity_diff, same_who_ahead, points_diff


if __name__ == "__main__":
    for f1, f2 in itertools.combinations(features, 2):
        speed_diff, severity_diff, same_who_ahead, points_diff = compare(f1, f2)
        print(f"{f1['source_file']} vs {f2['source_file']}")
        print(f"  closing_speed diff: {speed_diff:.1f} | contact_severity diff: {severity_diff:.1f} | same who_was_ahead: {same_who_ahead} | penalty_points diff: {points_diff}")
