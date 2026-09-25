import fastf1

fastf1.Cache.enable_cache("backend/cache")

session = fastf1.get_session(2024, "Abu Dhabi", "R")
session.load()

driver_laps = session.laps.pick_drivers(77)
print(driver_laps[["LapNumber", "LapStartTime", "LapStartDate"]])


lap_30 = driver_laps[driver_laps["LapNumber"] == 30].iloc[0]
telemetry = lap_30.get_telemetry()
print(telemetry[["Distance", "Speed", "Brake", "Date"]].head(30))

window = telemetry[
    (telemetry["Date"] >= "2024-12-08 13:48:55") &
    (telemetry["Date"] <= "2024-12-08 13:49:10")
]
print(window[["Distance", "Speed", "Brake", "Date"]])

print(telemetry.tail(10))
print("Max distance covered:", telemetry["Distance"].max())