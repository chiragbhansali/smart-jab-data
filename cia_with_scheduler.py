import pandas as pd
import time
import schedule
import datetime as dt
from cowin_api import CoWinAPI
import numpy as np


def cia(dict1):
    centres = pd.read_csv('centers_top100.csv')
    centres.drop(columns='Unnamed: 0', inplace=True)
    min_age_limit = 18
    now = str(dt.datetime.today())[11:16]
    # date = str(dt.date.today().strftime('%d-%b')) from tomorrow
    date = '26-May'
    cowin = CoWinAPI()
    for district_id in list(dict1):
        centers = cowin.get_availability_by_district(
            str(district_id), date, min_age_limit)
        for j in centers['centers']:
            n = 0
            center_id = j['center_id']
            if float(center_id) not in centres["Center ID"]:
                listx = list(centres.columns)
                listy = [j['name'][0], j["center_id"], j['district_name'], district_id, j['pincode'], j['fee_type'], '18', j['sessions'][0]['vaccine'], np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN,
                         np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]
                bar = pd.DataFrame([listy], columns=listx)
                centres = centres.append(bar, ignore_index=True)
                centres.drop_duplicates(subset=['Center ID'], inplace=True)
            if len(j) != 0:
                for i in range(len(centers['centers'][0]['sessions'])):
                    session = centers['centers'][0]['sessions'][i]
                    date_session = session['date']
                    date_session = dt.datetime.strptime(
                        date_session, '%d-%m-%Y')
                    date_session = dt.datetime.strftime(date_session, '%d-%b')
                    date_session = str(date_session)
                    if session['available_capacity'] >= 10:
                        if type(centres.loc[centres["Center ID"] == float(center_id), date_session]) != str:
                            if n == 0:
                                centres.loc[centres["Center ID"] == float(
                                    center_id), date_session] = now
                                n += 1
                            elif n > 0:
                                centres.loc[centres["Center ID"] == float(
                                    center_id), date_session] = 'NA'
    centres.to_csv('centers_top100.csv')


top100districtID = [5, 4, 7, 13, 8, 49, 77, 97, 108, 119, 109, 141, 145, 140, 146, 147, 143, 148, 149, 144, 150,
                    142, 154, 770, 175, 771, 169, 773, 173, 775, 165, 776, 155, 777, 199, 188, 230, 220, 257,
                    247, 240, 276, 265, 294, 264, 269, 278, 267, 266, 307, 296, 312, 313, 314, 315, 318, 364,
                    366, 390, 371, 395, 365, 382, 389, 394, 363, 393, 375, 392, 457, 446, 453, 485, 492, 488,
                    507, 501, 505, 506, 502, 503, 571, 539, 563, 540, 545, 560, 568, 581, 609, 610, 622, 623,
                    637, 649, 650, 651, 654, 661, 663, 664, 670, 676, 678, 624, 684, 696, 697, 717, 719, 721,
                    722, 725]
top100districts = ['Guntur', 'Krishna', 'Kurnool', 'Sri Potti Sriramulu Nellore', 'Visakhapatnam',
                   'Kamrup Metropolitan', 'Aurangabad', 'Patna', 'Chandigarh', 'Durg', 'Raipur',
                   'Central Delhi', 'East Delhi', 'New Delhi', 'North Delhi', 'North East Delhi',
                   'North West Delhi', 'Shahdara', 'South Delhi', 'South East Delhi', 'South West Delhi',
                   'West Delhi', 'Ahmedabad', 'Ahmedabad Corporation', 'Bhavnagar', 'Bhavnagar Corporation',
                   'Jamnagar', 'Jamnagar Corporation', 'Rajkot', 'Rajkot Corporation', 'Surat',
                   'Surat Corporation', 'Vadodara', 'Vadodara Corporation', 'Faridabad', 'Gurgaon', 'Jammu',
                   'Srinagar', 'Dhanbad', 'East Singhbhum', 'Ranchi', 'Bangalore Rural', 'Bangalore Urban',
                   'BBMP', 'Belgaum', 'Dakshina Kannada', 'Dharwad', 'Gulbarga', 'Mysore', 'Ernakulam',
                   'Thiruvananthapuram', 'Bhopal', 'Gwalior', 'Indore', 'Jabalpur', 'Ujjain', 'Akola',
                   'Amravati', 'Jalgaon', 'Kolhapur', 'Mumbai', 'Nagpur', 'Nanded', 'Nashik', 'Palghar',
                   'Pune', 'Raigad', 'Solapur', 'Thane', 'Cuttack', 'Khurda', 'Sundargarh', 'Amritsar',
                   'Jalandhar', 'Ludhiana', 'Ajmer', 'Bikaner', 'Jaipur I', 'Jaipur II', 'Jodhpur', 'Kota',
                   'Chennai', 'Coimbatore', 'Erode', 'Madurai', 'Salem', 'Tiruchirappalli', 'Tiruppur',
                   'Hyderabad', 'Warangal(Rural)', 'Warangal(Urban)', 'Agra', 'Aligarh', 'Bareilly',
                   'Firozabad', 'Gautam Buddha Nagar', 'Ghaziabad', 'Gorakhpur', 'Jhansi', 'Kanpur Dehat',
                   'Kanpur Nagar', 'Lucknow', 'Meerut', 'Moradabad', 'Prayagraj', 'Saharanpur', 'Varanasi',
                   'Dehradun', 'Darjeeling', 'East Bardhaman', 'Howrah', 'Jalpaiguri', 'Kolkata']
list1 = [top100districtID, top100districts]
dict1 = {list1[0][i]: list1[1][i] for i in range(0, len(list1[0]))}


def job():
    schedule.every(5).minutes.do(cia(dict1)).tag('5-min')


def cancel():
    schedule.clear('5-min')


schedule.every().day.at('05:55').do(job)
schedule.every().day.at('23:01').do(cancel)


while time_now != '23:02':
    time_now = str(dt.datetime.today())[11:19]
    schedule.run_pending()
