import gspread
import pandas as pd
import numpy as np
import datetime as dt
import time
import threading
import math

gc = gspread.service_account(filename='./cowin-data-a3988c41e946.json')
sh = gc.open("Top 100 Center DB")
dfMain = pd.read_csv('centers_top100_copy.csv')
dfHes = pd.read_csv('centers_top100_hesitancy_copy.csv')
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
