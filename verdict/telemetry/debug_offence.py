import fastf1
import pandas as pd

fastf1.Cache.enable_cache("backend/cache")

session = fastf1.get_session(2021, "Hungarian", "R")
session.load()

own_laps = session.laps.pick_drivers(77)
print("Number of laps for car 77:", len(own_laps))
print("First lap LapStartDate:", own_laps.iloc[0]["LapStartDate"])
print("Last lap LapStartDate:", own_laps.iloc[-1]["LapStartDate"])

session_date = own_laps.iloc[0]["LapStartDate"].date()
hour, minute = 15, 3  # from offence.json's "Time": "15:03"
local_dt = pd.Timestamp(year=session_date.year, month=session_date.month, day=session_date.day, hour=hour, minute=minute)
estimated_utc = local_dt - pd.Timedelta(hours=2)
print("Estimated UTC incident time:", estimated_utc)
