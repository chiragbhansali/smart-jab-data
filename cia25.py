import pandas as pd
import numpy as np
from cowin_api import CoWinAPI
import datetime as dt
import time
import schedule
import threading
import json

# import random
# import proxyscrape
# import requests
# from fake_useragent import UserAgent
# from requests.exceptions import HTTPError

# Make dictionary of district IDs and district names
districts = {
    141: 'Central Delhi',
    145: 'East Delhi',
    140: 'New Delhi',
    146: 'North Delhi',
    147: 'North East Delhi',
    143: 'North West Delhi',
    148: 'Shahdara',
    149: 'South Delhi',
    144: 'South East Delhi',
    150: 'South West Delhi',
    395: 'Mumbai',
    199: 'Faridabad',
    188: 'Gurgaon',
    276: 'Bangalore Rural',
    265: 'Bangalore Urban',
    294: 'BBMP',

    571: 'Chennai',
    581: 'Hyderabad',
    650: 'Gautam Buddha Nagar',
    651: 'Ghaziabad',
    721: 'Howrah',
    725: 'Kolkata',
    165: 'Surat',
    776: 'Surat Corporation',
    363: 'Pune',
    505: 'Jaipur I',
    506: 'Jaipur II',

    314: 'Indore',
    365: 'Nagpur',
    389: 'Nashik',
    392: 'Thane',
    637: 'Bareilly',
}

min_age_limit = 18

StartTime = time.time()


class setInterval:
    def __init__(self, interval, action):
        self.interval = interval
        self.action = action
        self.stopEvent = threading.Event()
        thread = threading.Thread(target=self.__setInterval)
        thread.start()

    def __setInterval(self):
        nextTime = time.time()+self.interval
        while not self.stopEvent.wait(nextTime-time.time()):
            nextTime += self.interval
            self.action()

    def cancel(self):
        self.stopEvent.set()


def today() -> str:
    return dt.datetime.now().strftime("%d-%m-%Y")


def cia(dict_1):
    # Read CSV
    df = pd.read_csv('centers_top25.csv')
    if list(df.columns)[0] != "Center Name":
        df.drop(columns=list(df.columns)[0], inplace=True)

    # Current time in format HH:MM
    now = str(dt.datetime.today())[11:16]
    # Start date
    # date = '25-May'

    # from 25th this will be the value of date
    date = str(dt.date.today().strftime('%d-%b'))

    # Initialize CoWin API
    cowin = CoWinAPI()
    # Current time
    print(now)
    # Loop through districts
    for district_id in list(dict_1):
        # Check if API Rate limit has been exceeded
        try:
            # Get data from CoWin API for district ID
            centers = cowin.get_availability_by_district(str(district_id))
        except TypeError:
            print("API Rate Limit Exceeded!")
            continue
        # Loop through centers in the district
        for i in centers['centers']:
            # Get Center ID of the center
            center_id = i['center_id']
            # If Center ID is not in the dataframe, add all its details
            if (float(center_id) not in df["Center ID"]) and i["sessions"][0]["min_age_limit"] == 18:
                test = pd.DataFrame({"Center Name": [i["name"]], "Center ID": [i["center_id"]], "District": [i["district_name"]], "District ID": [
                                    district_id], "PIN Code": [i['pincode']], "Paid/Free": [i['fee_type']], "Minimum Age": ["18"], "Vaccine": [i["sessions"][0]['vaccine']]})
                # Append new center row to df
                df = df.append(test, ignore_index=True)
                df.drop_duplicates(subset=['Center ID'], inplace=True)
        try:
            # loop through centers in district
            for c in range(len(centers['centers'])):
                # get center_id
                center_id = centers['centers'][c]['center_id']
                # variable for checking consecutive openings
                consecutiveSlot = 0
                # loop through sessions in a center
                for s in range(len(centers['centers'][c]['sessions'])):
                    session = centers['centers'][c]['sessions'][s]
                    date_session = session['date']
                    date_session = dt.datetime.strptime(
                        date_session, '%d-%m-%Y')
                    # convert date to 25-May, 25-Jun format
                    date_session = dt.datetime.strftime(date_session, '%d-%b')
                    date_session = str(date_session)

                    # check if dose1 >= 10 and min_age_limit == 18
                    if session['available_capacity_dose1'] >= 10 and session["min_age_limit"] == 18:
                        # Get type of cell
                        res = df.loc[df["Center ID"] == float(
                            center_id), date_session].apply(type)
                        # check if type of cell is not str
                        if (res != str).bool():
                            if consecutiveSlot == 0:
                                df.loc[df["Center ID"] ==
                                       float(center_id), date_session] = now
                                consecutiveSlot += 1
                            elif consecutiveSlot > 0:
                                consecutiveSlot += 1
                                print(centers['centers'][c]['name'] +
                                      " " + str(centers['centers'][c]['pincode']))
                                df.loc[df["Center ID"] == float(
                                    center_id), date_session] = 'Prev'
        except:
            continue
    if list(df.columns)[0] != "Center Name":
        df.drop(columns=list(df.columns)[0], inplace=True)

    df.to_csv('centers_top25.csv')
    try:
        df.to_csv('centers_top25_copy.csv')
    except:
        pass
    print(f"{str(dt.datetime.today())[11:16]} CSV Saved")


cia(districts)
# Scheduler
print("scheduler start")
schedule.every(100).seconds.until("23:30").do(cia, dict_1=districts)
timenow = str(dt.datetime.today())[11:16]
while timenow != "23:31":
    schedule.run_pending()
    time.sleep(1)
    timenow = str(dt.datetime.today())[11:16]
