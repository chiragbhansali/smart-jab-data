import pandas as pd
import numpy as np
from cowin_api import CoWinAPI
import datetime as dt
import time
import schedule
import threading
import json
import winsound
import traceback
import gspread

errorSound = {
    "duration" : 10000,  # milliseconds
    "freq" : 440  # Hz
}

def makeSound():
    winsound.Beep(errorSound["freq"], errorSound["duration"])


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
    725: 'Kolkata',
    363: 'Pune',
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

folder = "./data/store/25"

try:
    def cia(dict_1):
        # Read CSV
        dfMain = pd.read_csv(f"{folder}/centers_top25.csv")
        dfHes = pd.read_csv(f"{folder}/centers_top25_hesitancy.csv")
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
            except:
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
                            "State": [i['state_name']],
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
                            "State": [i['state_name']],
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
                    if not (dfMain.loc[dfMain["Center ID"] == float(center_id), "Dose Capacity"].empty):
                        currentCapacityDf = dfMain.loc[dfMain["Center ID"] == float(center_id), "Dose Capacity"].item()
                    maxDoseCapacity = max(int(maxDC), int(currentCapacityDf))
                    dfMain.loc[dfMain["Center ID"] == float(
                        center_id), "Dose Capacity"] = maxDoseCapacity
                # Fill max dose capacity of center in hesitancy dataframe
                if i["sessions"][0]["min_age_limit"] >= 18:
                    doseCapacity = []
                    for session in i["sessions"]:
                        doseCapacity.append(session["available_capacity"])
                    maxDC = max(doseCapacity)
                    # Try catch is used here since some centers have wrong center IDs and hence, pandas is not able to find any center using the center ID
                    try:
                        currentCapacityDf = dfHes.loc[dfHes["Center ID"]
                                                    == float(center_id), "Dose Capacity"].item()
                    except:
                        pass
                    maxDoseCapacity = max(int(maxDC), int(currentCapacityDf))
                    dfHes.loc[dfHes["Center ID"] == float(
                        center_id), "Dose Capacity"] = maxDoseCapacity


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
                        if session['available_capacity_dose1'] >= 10:
                            if dfHes.loc[dfHes["Center ID"] == float(center_id), date_session].isnull().item():
                                dfHes.loc[dfHes["Center ID"] == float(center_id), date_session] = 0
                            currentMin = int(
                                dfHes.loc[dfHes["Center ID"] == float(center_id), date_session].item())
                            dfHes.loc[dfHes["Center ID"] == float(
                                center_id), date_session] = currentMin + 5
            except:
                continue

        dfMain = dfMain.sort_values(["State", "District"], ascending=(True, True))
        dfHes = dfHes.sort_values(["State", "District"], ascending=(True, True))
        dfMain.to_csv(f"{folder}/centers_top25.csv")
        dfHes.to_csv(f"{folder}/centers_top25_hesitancy.csv")
        try:
            dfMain.to_csv(f"{folder}/centers_top25_copy.csv")
            dfHes.to_csv(f"{folder}/centers_top25_hesitancy_copy.csv")
        except:
            pass
        print(f"{str(dt.datetime.today())[11:16]} CSV Saved")

    cia(districts)

    # Set Interval till 23:30 pm

    s1 = dt.datetime.today()
    s2 = dt.datetime.today()
    nt = s2.replace(hour=23, minute=30)

    secondsTo23_30 = (nt - s1).total_seconds()

    inter = setInterval(75, cia, districts)
    print("Interval Started")
    t = threading.Timer(secondsTo23_30, inter.cancel)
    t.start()

    def sheetUpload(csvName):
        gc = gspread.service_account(filename='./clientapi.json')
        sh = gc.open("Top 100 Center DB")
        dfMain = pd.read_csv(f"{folder}/{csvName}_copy.csv")
        dfHes = pd.read_csv(F"{folder}/{csvName}_hesitancy_copy.csv")
        dfArr = [dfMain, dfHes]
        for df in dfArr:
            if list(df.columns)[0] != "Center Name":
                df.drop(columns=list(df.columns)[0], inplace=True)
        dfMain = dfMain.fillna('')
        dfHes = dfHes.fillna('')
        worksheet_main = sh.get_worksheet(0)
        worksheet_hes = sh.get_worksheet(1)
        dfSheetMain = pd.DataFrame(worksheet_main.get_all_records())
        dfSheetHes = pd.DataFrame(worksheet_hes.get_all_records())

        sheetUpdateCount = 0

        # Adding centers in main df
        print("Adding centers in main df")
        z = len(dfSheetMain)+2
        for i in dfMain.index:
            row = dfMain.loc[i]
            if (row['Center ID']) not in list(dfSheetMain['Center ID']):
                bar=pd.DataFrame(columns=list(dfSheetMain.columns))
                bar = bar.append(row, ignore_index=True)
                bar = bar.fillna('')
                cell_list = worksheet_main.range(('A'+str(z)+':'+'FO'+str(z)))
                # bar.fillna('', inplace=True)
                for j in range(len(cell_list)):
                    upd = bar.loc[0].values[j]
                    if type(bar.loc[0].values[j]) != str:
                        bar.loc[0].values[j] = int(bar.loc[0].values[j])
                        upd = np.uint32(int(bar.loc[0].values[j])).item()
                    cell_list[j].value = upd
                # print("Sheet Update Count in Main DF", sheetUpdateCount)
                if sheetUpdateCount == 59:
                    print("time out taken")
                    time.sleep(62) # pause code execution for a minute
                    sheetUpdateCount = 0
                worksheet_main.update_cells(cell_list)
                sheetUpdateCount += 1
                z+=1

        # Adding centers in hesitancy df
        print("Adding centers in hesitancy df")
        y = len(dfSheetHes)+2
        for i in dfHes.index:
            row = dfHes.loc[i]
            if (row['Center ID']) not in list(dfSheetHes['Center ID']):
                bar=pd.DataFrame(columns=list(dfSheetHes.columns))
                bar = bar.append(row, ignore_index=True)
                cell_list = worksheet_hes.range(('A'+str(y)+':'+'FM'+str(y)))

                for j in range(len(cell_list)):
                    if type(bar.loc[0].values[j]) == float:
                        bar.loc[0].values[j] = int(bar.loc[0].values[j])
                    upd = (bar.loc[0].values[j])
                    cell_list[j].value = upd
                if sheetUpdateCount == 59:
                    print("time out taken")
                    time.sleep(62) # pause code execution for a minute
                    sheetUpdateCount = 0
                worksheet_hes.update_cells(cell_list)
                sheetUpdateCount += 1
                y+=1

        sh_columns=[]
        n = len(dfMain.columns)
        list_dates = (dfMain.columns)[0:n]
        for i in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
            sh_columns.append(i)
        for j in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
            for i in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
                sh_columns.append(j+i)
        sheet_columns = sh_columns[0:n]
        list_dates_columns = [list_dates, sheet_columns]
        date_dict = {list_dates_columns[0][i]: list_dates_columns[1][i] for i in range(0, len(list_dates_columns[0]))}

        print("Start updating values in main df")

        for i in dfSheetMain.index:
            today = dt.date.today()
            center_id = dfMain.loc[i, "Center ID"]
            for j in range(-1,6):
                date = str(today + dt.timedelta(j))
                date = dt.datetime.strptime(date, '%Y-%m-%d')
                date = dt.datetime.strftime(date, '%d-%b')
                if dfMain.loc[dfMain["Center ID"] == float(center_id), date].item() != '' and (not (dfMain.loc[dfMain["Center ID"] == float(center_id), date].isnull().item())):
                    if dfSheetMain.loc[dfSheetMain["Center ID"] == float(center_id), date].empty:
                        print("in the na block")
                        dfSheetMain.loc[dfSheetMain["Center ID"] == float(center_id), date] = 0
                    if dfMain.loc[dfMain["Center ID"] == float(center_id), date].item() != dfSheetMain.loc[dfSheetMain["Center ID"] == float(center_id), date].item():
                        cellIndexRel = dfSheetMain.index[dfSheetMain["Center ID"] == float(center_id)].tolist()[0]
                        cell_index = date_dict[date]+str(cellIndexRel+2)
                        value = dfMain.loc[dfMain["Center ID"] == float(center_id), date].item()
                        if sheetUpdateCount == 59:
                            print("time out taken")
                            time.sleep(62) # pause code execution for a minute
                            sheetUpdateCount = 0
                        worksheet_main.update(cell_index, value)
                        sheetUpdateCount += 1

        sh_columns=[]
        n = len(dfHes.columns)
        list_dates = (dfHes.columns)[0:n]
        for i in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
            sh_columns.append(i)
        for j in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
            for i in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
                sh_columns.append(j+i)
        sheet_columns = sh_columns[0:n]
        list_dates_columns = [list_dates, sheet_columns]
        date_dict = {list_dates_columns[0][i]: list_dates_columns[1][i] for i in range(0, len(list_dates_columns[0]))}

        print("Start updating values in hes df")
        for i in dfSheetHes.index:
            today = dt.date.today()
            center_id = dfHes.loc[i, "Center ID"]
            for j in range(-1,6):
                date = str(today + dt.timedelta(j))
                date = dt.datetime.strptime(date, '%Y-%m-%d')
                date = dt.datetime.strftime(date, '%d-%b')
                if dfHes.loc[dfHes["Center ID"] == float(center_id), date].item() != '':
                    if dfHes.loc[dfHes["Center ID"] == float(center_id), date].item() != dfSheetHes.loc[dfSheetHes["Center ID"] == float(center_id), date].item():
                        cellIndexRel = dfSheetHes.index[dfSheetHes["Center ID"] == float(center_id)].tolist()[0]
                        cell_index = date_dict[date]+str(cellIndexRel+2)
                        value = dfHes.loc[dfHes["Center ID"] == float(center_id), date].item()
                        if sheetUpdateCount == 59:
                            print("time out taken")
                            time.sleep(62) # pause code execution for a minute
                            sheetUpdateCount = 0
                        worksheet_hes.update(cell_index, value)
                        sheetUpdateCount += 1

    print("scheduler started for google sheet")
    schedule.every().day.at("22:00").do(sheetUpload, csvName="centers_top25")
    timenow = str(dt.datetime.today())[11:16]
    while timenow != "23:31":
        schedule.run_pending()
        time.sleep(1)
        timenow = str(dt.datetime.today())[11:16]


except Exception:
    print(traceback.print_exc())
    makeSound()