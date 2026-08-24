import fastf1

fastf1.Cache.enable_cache("cache")

session = fastf1.get_session(2023, "Bahrain", "R")
session.load()

# Get Verstappen's fastest lap
ver_lap = session.laps.pick_driver("VER").pick_fastest()

# Get telemetry for that lap
telemetry = ver_lap.get_telemetry()

print(telemetry[["Speed", "X", "Y", "Distance", "Throttle", "Brake"]].head(20))
