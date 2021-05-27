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
    dfMain = pd.read_csv('centers_top25.csv')
    dfHes = pd.read_csv('centers_top25_hesitancy.csv')
    print(dfHes.columns)
    print(dfMain.columns)
    dfArr = [dfMain, dfHes]
    # Remove first column in both dataframes (first column is index col created by pandas)
    # if list(dfMain.columns)[0] != "Center Name":
    #     dfMain.drop(columns=list(dfMain.columns)[0], inplace=True)
    for df in dfArr:
        if list(df.columns)[0] != "Center Name":
            df.drop(columns=list(df.columns)[0], inplace=True)

    # Current time in format HH:MM
    now = str(dt.datetime.today())[11:16]
    # Start date
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
            # print(json.dumps(i, indent=1))
            # If Center ID is not in the dataframe, add all its details to main dataframe
            if (float(center_id) not in dfMain["Center ID"]) and i["sessions"][0]["min_age_limit"] == 18:
                test = pd.DataFrame(
                    {
                        "Center Name": [i["name"]],
                        "Center ID": [i["center_id"]],
                        "District": [i["district_name"]],
                        "District ID": [district_id],
                        "PIN Code": [i['pincode']],
                        "Paid/Free": [i['fee_type']],
                        "Minimum Age": ["18"],
                        "Dose Capacity": ["0"],
                        "Vaccine": [i["sessions"][0]['vaccine']]
                    }
                )
                # Append new center row to df
                dfMain = dfMain.append(test, ignore_index=True)
                dfMain.drop_duplicates(subset=['Center ID'], inplace=True)

            # Add all centers to hesitancy dataframe
            if (float(center_id) not in dfHes["Center ID"]):
                test = pd.DataFrame(
                    {
                        "Center Name": [i["name"]],
                        "Center ID": [i["center_id"]],
                        "District": [i["district_name"]],
                        "District ID": [district_id],
                        "PIN Code": [i['pincode']],
                        "Paid/Free": [i['fee_type']],
                        "Minimum Age": [i["sessions"][0]["min_age_limit"]],
                        "Dose Capacity": ["0"],
                        "Vaccine": [i["sessions"][0]['vaccine']]
                    }
                )
                # Append new center row to df
                dfHes = dfHes.append(test, ignore_index=True)
                dfHes.drop_duplicates(subset=['Center ID'], inplace=True)
            # Fill max dose capacity of center in main dataframe
            if i["sessions"][0]["min_age_limit"] == 18:
                doseCapacity = []
                for session in i["sessions"]:
                    doseCapacity.append(session["available_capacity"])
                maxDC = max(doseCapacity)
                currentCapacityDf = dfMain.loc[dfMain["Center ID"]
                                               == float(center_id), "Dose Capacity"].item()
                maxDoseCapacity = max(int(maxDC), int(currentCapacityDf))
                dfMain.loc[dfMain["Center ID"] == float(
                    center_id), "Dose Capacity"] = maxDoseCapacity
            # doseCapacity = []
            # for session in i["sessions"]:
            #     doseCapacity.append(session["available_capacity"])
            # maxDC = max(doseCapacity)
            # currentCapacityDf = dfHes.loc[dfHes["Center ID"]
            #                               == float(center_id), "Dose Capacity"].item()
            # maxDoseCapacity = max(int(maxDC), int(currentCapacityDf))
            # dfHes.loc[dfHes["Center ID"] == float(
            #     center_id), "Dose Capacity"] = maxDoseCapacity
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
                        res = dfMain.loc[dfMain["Center ID"] == float(
                            center_id), date_session].apply(type)
                        # check if type of cell is not str
                        if (res != str).bool():
                            if consecutiveSlot == 0:
                                dfMain.loc[dfMain["Center ID"] ==
                                           float(center_id), date_session] = now
                                consecutiveSlot += 1
                            elif consecutiveSlot > 0:
                                consecutiveSlot += 1
                                print(centers['centers'][c]['name'] +
                                      " " + str(centers['centers'][c]['pincode']))
                                dfMain.loc[dfMain["Center ID"] == float(
                                    center_id), date_session] = 'Prev'
        except:
            continue
    if list(dfMain.columns)[0] != "Center Name":
        dfMain.drop(columns=list(dfMain.columns)[0], inplace=True)
    dfMain = dfMain.sort_values('District')
    # dfHes = dfHes.sort_values('District')
    dfMain.to_csv('centers_top25.csv')
    print("Main CSV saved")
    dfHes.to_csv('centers_top25_hesitancy.csv')
    print("Hesitancy CSV saved")
    try:
        dfMain.to_csv('centers_top25_copy.csv')
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
