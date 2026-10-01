#import fastf1 api to recieve session telemetry datas
import fastf1

fastf1.Cache.enable_cache("backend/cache")

#load the session info
session = fastf1.get_session(2024, "Abu Dhabi", "R")
session.load()

#print driver laps - driver no. is 77. 
#We print the lap information of driver 77 - Bottas
driver_laps = session.laps.pick_drivers(77)
print(driver_laps[["LapNumber", "LapStartTime", "LapStartDate"]])


#Get all telemetry data of the driver's laps
telemetry_all = driver_laps.get_telemetry()


#Narrow telemetry down to the ~1 min window where the incident likely happened
recent = telemetry_all[
    (telemetry_all["Date"] >= "2024-12-08 13:48:39") &
    (telemetry_all["Date"] <= "2024-12-08 13:49:35")
].copy()


#Wider window from the braking moment to where the data stops - shows the aftermath
full_stretch = telemetry_all[
    (telemetry_all["Date"] >= "2024-12-08 13:48:58") &
    (telemetry_all["Date"] <= "2024-12-08 13:51:03")
]
#Print every 15th row so it's readable instead of hundreds of rows
print(full_stretch[["Distance", "Speed", "Brake", "Date"]].iloc[::15])


#How much speed changed between each sample - big negative number = sudden slowdown
recent["SpeedChange"] = recent["Speed"].diff()

#Find the single sharpest slowdown - our best guess at the collision moment
biggest_drop_index = recent["SpeedChange"].idxmin()
print(recent.loc[biggest_drop_index])


#Show a few rows before and after that moment for context
start = recent.index.get_loc(biggest_drop_index) - 5
end = recent.index.get_loc(biggest_drop_index) + 60
print(recent.iloc[start:end][["Distance", "Speed", "Brake", "Date"]])

#Official race control log - filter for car 77 to find the Turn 6 incident report
messages = session.race_control_messages
print(messages[messages["Message"].str.contains("77", na=False)])
