import fastf1
import pandas as pd

def get_telemetry_snapshot(race_year, race_event, own_car, other_car, incident_time_local, utc_offset_hours):
    fastf1.Cache.enable_cache("backend/cache")

    session = fastf1.get_session(race_year, race_event, "R")
    session.load()

    own_laps = session.laps.pick_drivers(own_car)
    telemetry_own = own_laps.get_telemetry()

    # Get the race's calendar date from the first lap, so we can build a full datetime
    session_date = own_laps.iloc[0]["LapStartDate"].date()

    # Convert the FIA document's local incident time ("17:49") into a UTC datetime
    hour, minute = map(int, incident_time_local.split(":"))
    local_dt = pd.Timestamp(
        year=session_date.year, month=session_date.month, day=session_date.day,
        hour=hour, minute=minute
    )
    estimated_utc = local_dt - pd.Timedelta(hours=utc_offset_hours)

    # Find the lap whose start time is the closest one before our estimated incident time
    candidate_laps = own_laps[own_laps["LapStartDate"] <= estimated_utc]
    target_lap = candidate_laps.iloc[-1]

    window = target_lap.get_telemetry().copy()
    window["SpeedChange"] = window["Speed"].diff()
    incident_moment = window.loc[window["SpeedChange"].idxmin(), "Date"]


    # Pull a focused snapshot (5s before, 15s after) for both cars around that moment
    def snapshot_for(telemetry):
        return telemetry[
            (telemetry["Date"] >= incident_moment - pd.Timedelta(seconds=5)) &
            (telemetry["Date"] <= incident_moment + pd.Timedelta(seconds=15))
        ][["Distance", "Speed", "Brake", "Date"]].astype(str).to_dict(orient="records")

    telemetry_other = session.laps.pick_drivers(other_car).get_telemetry()

    return {
        "incident_moment_utc": str(incident_moment),
        "own_car_telemetry": snapshot_for(telemetry_own),
        "other_car_telemetry": snapshot_for(telemetry_other),
    }

if __name__ == "__main__":
    import json

    snapshot = get_telemetry_snapshot(
        race_year=2024,
        race_event="Abu Dhabi",
        own_car=77,
        other_car=20,
        incident_time_local="17:49",
        utc_offset_hours=4,
    )

    print("Incident moment (UTC):", snapshot["incident_moment_utc"])
    print("Own car samples:", len(snapshot["own_car_telemetry"]))
    print("Other car samples:", len(snapshot["other_car_telemetry"]))
    print(json.dumps(snapshot["own_car_telemetry"][:5], indent=2))
