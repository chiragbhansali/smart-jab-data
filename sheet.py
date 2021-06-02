import gspread
import pandas as pd
import datetime as dt
import time
import threading
import math

gc = gspread.service_account(filename='./cowin-data-a3988c41e946.json')
sh = gc.open("Top 100 Center DB")
dfMain = pd.read_csv('centers_top100_copy.csv')
dfHes = pd.read_csv('centers_top100_hesitancy_copy.csv')
worksheet_Main = sh.get_worksheet(0)
worksheet_Hes = sh.get_worksheet(1)
dfUpdateMain = dfMain.fillna('')
dfUpdateHes = dfHes.fillna('')
dfWorkMain = pd.DataFrame(worksheet_Main.get_all_records())
dfWorkHes = pd.DataFrame(worksheet_Hes.get_all_records())

z = len(dfWorkMain)+2
for i in dfUpdateMain.index:
    row = dfUpdateMain.loc[i]
    if (row['Center ID']) not in list(dfWorkMain['Center ID']):
        bar=pd.DataFrame(columns=list(dfWorkMain.columns))
        bar = bar.append(row, ignore_index=True)
        cell_list = worksheet_Main.range(('A'+str(z)+':'+'FL'+str(z)))

        for j in range(len(cell_list)):
            if type(bar.loc[0].values[j]) == float:
                bar.loc[0].values[j] = int(bar.loc[0].values[j])
            upd = (bar.loc[0].values[j])
            cell_list[j].value = upd
        worksheet_Main.update_cells(cell_list)
        z+=1

y = len(dfWorkHes)+2
for i in dfUpdateHes.index:
    row = dfUpdateHes.loc[i]
    if (row['Center ID']) not in list(dfWorkHes['Center ID']):
        bar=pd.DataFrame(columns=list(dfWorkHes.columns))
        bar = bar.append(row, ignore_index=True)
        cell_list = worksheet_Hes.range(('A'+str(z)+':'+'FL'+str(z)))

        for j in range(len(cell_list)):
            if type(bar.loc[0].values[j]) == float:
                print(bar.loc[0])
                print(j)
                abcd = []
                for abc in bar.loc[0].values:
                    abcd.append(pd.isna(bar.loc[0].values[j]))
                print(abcd)
                print(bar.loc[0].values)
                bar.loc[0].values[j] = int(bar.loc[0].values[j])
            upd = (bar.loc[0].values[j])
            cell_list[j].value = upd
        worksheet_Hes.update_cells(cell_list)
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

mainDfUpdateCount = 0
for i in dfWorkMain.index:
    today = dt.date.today()
    for j in range(0,7):
        date = str(today + dt.timedelta(j))
        date = dt.datetime.strptime(date, '%Y-%m-%d')
        date = dt.datetime.strftime(date, '%d-%b')
        if mainDfUpdateCount == 100:
            time.sleep(110)
            mainDfUpdateCount = 0
        if dfUpdateMain.loc[i, date]!= '':
            if dfUpdateMain.loc[i, date]!= dfWorkMain.loc[i, date]:
                mainDfUpdateCount += 1
                cell_index = date_dict[date]+str(i+2)
                print(cell_index)
                value = dfUpdateMain.loc[i, date]
                print(value)
                worksheet_Main.update(cell_index, value)

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

for i in dfWorkHes.index:
    today = dt.date.today()
    for j in range(0,7):
        date = str(today + dt.timedelta(j))
        date = dt.datetime.strptime(date, '%Y-%m-%d')
        date = dt.datetime.strftime(date, '%d-%b')
        if dfUpdateHes.loc[i, date]!= '':
            if dfUpdateHes.loc[i, date]!= dfWorkHes.loc[i, date]:
                cell_index = date_dict[date]+str(i+2)
                print(cell_index)
                value = dfUpdateHes.loc[i, date]
                print(value)
                worksheet_Hes.update(cell_index, value)
            
            