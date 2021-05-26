import pandas as pd
import numpy as np
from cowin_api import CoWinAPI
import datetime as dt
import time
import schedule
import threading

# import random
# import proxyscrape
# import requests
# import json
# from fake_useragent import UserAgent
# from requests.exceptions import HTTPError

# Make dictionary of district IDs and district names
districts = {
    5: 'Guntur',
    4: 'Krishna',
    #  7: 'Kurnool',
    #  13: 'Sri Potti Sriramulu Nellore',
    8: 'Visakhapatnam',
    49: 'Kamrup Metropolitan',
    77: 'Aurangabad',
    97: 'Patna',
    108: 'Chandigarh',
    #  119: 'Durg',
    109: 'Raipur',
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
    142: 'West Delhi',
    154: 'Ahmedabad',
    770: 'Ahmedabad Corporation',
    175: 'Bhavnagar',
    771: 'Bhavnagar Corporation',
    169: 'Jamnagar',
    773: 'Jamnagar Corporation',
    173: 'Rajkot',
    775: 'Rajkot Corporation',
    165: 'Surat',
    776: 'Surat Corporation',
    155: 'Vadodara',
    777: 'Vadodara Corporation',
    199: 'Faridabad',
    188: 'Gurgaon',
    #     230: 'Jammu',
    220: 'Srinagar',
    257: 'Dhanbad',
    247: 'East Singhbhum',
    240: 'Ranchi',
    276: 'Bangalore Rural',
    265: 'Bangalore Urban',
    294: 'BBMP',
    #     264: 'Belgaum',
    269: 'Dakshina Kannada',
    278: 'Dharwad',
    # 267: 'Gulbarga',
    266: 'Mysore',
    307: 'Ernakulam',
    296: 'Thiruvananthapuram',
    312: 'Bhopal',
    313: 'Gwalior',
    314: 'Indore',
    315: 'Jabalpur',
    #     318: 'Ujjain',
    364: 'Akola',
    366: 'Amravati',
    390: 'Jalgaon',
    371: 'Kolhapur',
    395: 'Mumbai',
    365: 'Nagpur',
    382: 'Nanded',
    389: 'Nashik',
    394: 'Palghar',
    363: 'Pune',
    393: 'Raigad',
    375: 'Solapur',
    392: 'Thane',
    457: 'Cuttack',
    446: 'Khurda',
    #     453: 'Sundargarh',
    485: 'Amritsar',
    492: 'Jalandhar',
    488: 'Ludhiana',
    # 507: 'Ajmer',
    501: 'Bikaner',
    505: 'Jaipur I',
    506: 'Jaipur II',
    502: 'Jodhpur',
    503: 'Kota',
    571: 'Chennai',
    539: 'Coimbatore',
    #     563: 'Erode',
    540: 'Madurai',
    545: 'Salem',
    560: 'Tiruchirappalli',
    568: 'Tiruppur',
    581: 'Hyderabad',
    609: 'Warangal(Rural)',
    610: 'Warangal(Urban)',
    622: 'Agra',
    623: 'Aligarh',
    637: 'Bareilly',
    #     649: 'Firozabad',
    650: 'Gautam Buddha Nagar',
    651: 'Ghaziabad',
    #     654: 'Gorakhpur',
    #     661: 'Jhansi',
    663: 'Kanpur Dehat',
    664: 'Kanpur Nagar',
    670: 'Lucknow',
    676: 'Meerut',
    #     678: 'Moradabad',
    624: 'Prayagraj',
    # 684: 'Saharanpur',
    696: 'Varanasi',
    697: 'Dehradun',
    717: 'Darjeeling',
    #     719: 'East Bardhaman',
    721: 'Howrah',
    # 722: 'Jalpaiguri',
    725: 'Kolkata'
}

dict_test = {
    696: 'Varanasi',
    312: 'Bhopal',
    313: 'Gwalior',
    314: 'Indore',
    624: 'Prayagraj'
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
    df = pd.read_csv('centers_top100.csv')
    # Remove first column in df (first column is index col created by pandas)
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
    df.to_csv('centers_top100.csv')
    try:
        df.to_csv('centers_top100_copy.csv')
    except:
        pass
    print(f"{str(dt.datetime.today())[11:16]} CSV Saved")


cia(districts)
# Scheduler
print("scheduler start")
schedule.every(5).minutes.until("23:30").do(cia, dict_1=districts)
timenow = str(dt.datetime.today())[11:16]
while timenow != "23:31":
    schedule.run_pending()
    time.sleep(1)
    timenow = str(dt.datetime.today())[11:16]
