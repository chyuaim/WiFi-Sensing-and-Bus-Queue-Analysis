"""process_north.py

This script reads the sensor readings of the past hour from the /data directory,
it filters unwanted signals and applies classifacation model to the data to derives
the number of people queuing for each of the bus stops in the North Gate, 
the queue time distribution of current queuing passengers at each of the bus stops, and 
the aggregated queue time distribution of boarded passengers. The results are uploaded 
to MongoDB Atlas for further usages.

This tool should run continuously and undergo the filtering and classification procedures every 30 seconds 
in order to get the real-time results.

This script requires that `pandas`, `numpy`, `sklearn` and `pymongo` be installed within the Python
environment you are running this script in.

"""

# --- Imports --- #
import os
import time
from datetime import datetime, timedelta
import json
from base64 import b64decode
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import pymongo
import config
from config import queue_dist, qtd_transformation

# --- Assign global variables --- #
SENSOR              = config.SENSOR # a dictionary storing the sensor AP-ID corresponding to their MAC addresses
NORTH_GATE_RECEIVER = config.NORTH_GATE_RECEIVER # a list storing the MAC addresses of the sensors located in the South Gate

RESOURCES_DIR       = config.RESOURCES_DIR

READ_INTERVAL       = config.READ_INTERVAL
PROCESS_CYCLE       = config.PROCESS_CYCLE 

# DB_KEY is used to access the MongoDB
DB_KEY = config.DB_KEY
COLLECTION = pymongo.MongoClient(DB_KEY)['fyp_2021_busq']
COLLECTION_RAW_DATA = COLLECTION['raw_data']
COLLECTION_PPLNO_NORTH = COLLECTION['pplno_north']
COLLECTION_QTD_CURRENT_NORTH = COLLECTION['qtd_current_north']
COLLECTION_QTD_BOARDED_NORTH = COLLECTION['qtd_boarded_north']

while True:
    try:
        # --- define time interval --- #
        start = time.time() - READ_INTERVAL
        end = time.time() # the script read data from the system where the timestamp is between current time - READ_INTERVAL and current time
        
        # --- read all data within the time interval and put it into a single dataframe --- #
        data = pd.DataFrame(COLLECTION_RAW_DATA.find({'Time':{'$gte':datetime.fromtimestamp(start),'$lte':datetime.fromtimestamp(end)}},{'_id':0}).sort('Time',pymongo.ASCENDING))
        
        if data.empty: # skip the remaining processes and loop again if no data exists within the time interval
            time.sleep(PROCESS_CYCLE)
            continue
        
        data = data.dropna()
        data = pd.DataFrame(data.groupby(['Time','Target','Receiver']).max().reset_index())
        
        # --- decode the MAC addresses of the target devices and the sensors --- #
        data = data.assign(Target= data['Target'].apply(lambda s: b64decode(s.encode()).hex().upper()))
        data = data.assign(Receiver = data['Receiver'].map(SENSOR))
        data = data.assign(Strength = data['Strength']-100)
        server_time = data['Time'].max()

        # --- get only north gate data --- #
        north_data = data[data['Receiver'].isin(NORTH_GATE_RECEIVER)]
        north_data = north_data[north_data['Strength'] < 0]

        # --- filter weak signals --- #
        weak_target = north_data.groupby(['Target'])['Strength'].max().reset_index()
        weak_target = weak_target[weak_target['Strength'] < -70]['Target'].tolist()
        north_data = north_data[~north_data['Target'].isin(weak_target)]

        # --- assign a column for each sensor --- #
        north_data = pd.pivot_table(north_data,index=['Time','Target'],columns='Receiver',values='Strength',fill_value=-100).reset_index()
        for sensor in NORTH_GATE_RECEIVER:
            if sensor not in north_data:
                north_data[sensor] = -100
        north_data.columns.name = None
     
        # --- filter data in the list --- #
        with open(os.path.join(RESOURCES_DIR,"filter_list.txt"), "r") as fp:
            noise_target = json.load(fp)
        north_data = north_data[~north_data['Target'].isin(noise_target)]

        with open(os.path.join(RESOURCES_DIR,"mac_prefix.txt"), "r") as fp:
            mac_prefix = json.load(fp)
       
        # north_data = north_data[(north_data['Target'].str[:6].isin(mac_prefix)) | (north_data['Target'].str[1] == '2') | (north_data['Target'].str[1] == '6') | (north_data['Target'].str[1] == 'A') | (north_data['Target'].str[1] == 'E')].reset_index(drop=True) # with virtual mac
        north_data = north_data[north_data['Target'].str[:6].isin(mac_prefix)].reset_index(drop=True) # Without Virtual MAC Address

        # --- filter signal which appear only once --- #
        second_filter = pd.DataFrame(north_data['Target'].value_counts())
        north_data = north_data[~north_data['Target'].isin(second_filter[second_filter['Target']==1].index)]

        # --- upsample data --- #
        new_df = pd.DataFrame()
        for target in north_data['Target'].unique().tolist():
            target_dst = north_data[north_data['Target'] == target].reset_index(drop=True)

            time_diff = target_dst['Time'].diff()
            if time_diff.sum() < pd.Timedelta(seconds=60): # filter signals that last for too short
                continue
            mean = time_diff.mean()
            arrival_buffer = pd.Timedelta(seconds=0) # no arrival buffer
            departure_buffer = 3*mean
            interval_threshold = pd.Timedelta(seconds=1800) # if the time interval of two signals from the same device is more than half an hour, then they are not continuous 
            start_index = 0
            if time_diff[time_diff>=interval_threshold].shape[0] != 0: 
                for end_index in time_diff[time_diff>interval_threshold].index:
                    temp_df = target_dst.loc[[start_index]].copy() 
                    temp_df.iloc[0,0] = temp_df.iloc[0,0]-arrival_buffer # add arrival buffer
                    temp_df = pd.concat([temp_df,target_dst.loc[start_index:end_index-1].copy()])
                    temp_df = pd.concat([temp_df,target_dst.loc[[end_index-1]].copy()]) 
                    temp_df.iloc[-1,0] = temp_df.iloc[-1,0]+departure_buffer # add departure buffer
                    temp_df = temp_df.resample('S',on='Time').first().fillna(method='ffill').drop(columns='Time').reset_index()
                    new_df = pd.concat([new_df,temp_df])
                    start_index = end_index
            temp_df = target_dst.loc[[start_index]].copy()
            temp_df.iloc[0,0] = temp_df.iloc[0,0]-arrival_buffer
            temp_df = pd.concat([temp_df,target_dst.loc[start_index:].copy()])
            temp_df = pd.concat([temp_df,target_dst.iloc[[-1]].copy()])
            temp_df.iloc[-1,0] = temp_df.iloc[-1,0]+departure_buffer
            temp_df = temp_df.resample('S',on='Time').first().fillna(method='ffill').drop(columns='Time').reset_index()
            new_df = pd.concat([new_df,temp_df])
        
        if not new_df.empty:
            new_df = new_df.sort_values('Time').reset_index(drop=True)
            new_df = new_df[new_df['Time'] <= server_time]

            # --- define dataframe for no. of ppl. in the north gate --- #
            pplno = new_df.groupby('Time')['Target'].count().reset_index()
            if pplno['Time'].max() < server_time:
                pplno = pplno.append({'Time':server_time,'Target':0}, ignore_index=True)
            pplno = pplno.resample('5S',on='Time').first().fillna(0).drop(columns='Time').reset_index()


            # --- define dataframe for queue time distribution of current waiting passengers --- #
            last_appearence = new_df.groupby('Target')[['Time']].last().reset_index().rename(columns={'Time':'Last_Apppearence'})
            qtd_current = new_df[['Time','Target']].merge(last_appearence,how='left')
            qtd_current = qtd_current[qtd_current['Last_Apppearence'] >= server_time - pd.Timedelta(seconds=PROCESS_CYCLE)]
            first_appearence = qtd_current.groupby('Target')[['Time','Last_Apppearence']].first().reset_index().rename(columns={'Time':'First_Appearence'})
            qtd_current = qtd_current.merge(first_appearence,how='left')
            qtd_current['Queue_Time'] = qtd_current['Last_Apppearence'] - qtd_current['First_Appearence']
            qtd_current = qtd_current.assign(Queue_Time = qtd_current['Queue_Time'].apply(queue_dist))
            qtd_current = qtd_current.groupby('Target')[['Queue_Time']].first().reset_index()
            qtd_current = qtd_transformation(qtd_current,server_time.floor('T'))

            # --- define dataframe for queue time distribution of boarded passengers in the past hour --- #
            qtd_boarded = new_df[['Time','Target']].merge(last_appearence,how='left')
            qtd_boarded = qtd_boarded[qtd_boarded['Last_Apppearence'] < server_time - pd.Timedelta(seconds=PROCESS_CYCLE)]
            first_appearence = qtd_boarded.groupby('Target')[['Time','Last_Apppearence']].first().reset_index().rename(columns={'Time':'First_Appearence'})
            qtd_boarded = qtd_boarded.merge(first_appearence,how='left')
            qtd_boarded['Queue_Time'] = qtd_boarded['Last_Apppearence'] - qtd_boarded['First_Appearence']
            qtd_boarded = qtd_boarded.assign(Queue_Time = qtd_boarded['Queue_Time'].apply(queue_dist))
            qtd_boarded = qtd_boarded.groupby('Target')['Queue_Time'].first().reset_index()
            qtd_boarded = qtd_transformation(qtd_boarded,server_time.floor('T'))

        else:
            pplno = pd.DataFrame([{'Time':server_time-timedelta(seconds=READ_INTERVAL),'Target':0},
                            {'Time':server_time,'Target':0}])
            pplno = pplno.resample('5S', on='Time').first().fillna(0)[['Target']].reset_index()

            qtd_current = pd.DataFrame([{'Time':server_time.floor('T'),'0-5':0,'5-10':0,'10-15':0,'15-20':0, '20+':0}])

            qtd_boarded = pd.DataFrame([{'Time':server_time.floor('T'),'0-5':0,'5-10':0,'10-15':0,'15-20':0, '20+':0}])

        pplno['Target'] = pplno['Target'].astype(int)

        # --- filter records that already uploaded to DB --- #
        previous_pplno = pd.DataFrame(COLLECTION_PPLNO_NORTH.find({'Time':{'$gte':datetime.now()-timedelta(hours=1)}},{'_id':0}).sort('Time',pymongo.DESCENDING).limit(720))
        if not previous_pplno.empty:
            pplno = pplno[~pplno['Time'].isin(previous_pplno['Time'])].reset_index(drop=True)

        for df, collection in zip([qtd_current,qtd_boarded],
                [COLLECTION_QTD_CURRENT_NORTH,COLLECTION_QTD_BOARDED_NORTH]):
            previous_durations = pd.DataFrame(collection.find({'Time':{'$gte':datetime.now()-timedelta(hours=1)}},{'_id':0}).sort('Time',pymongo.DESCENDING).limit(60))
            if not previous_durations.empty: 
                if df['Time'].iloc[0] in previous_durations['Time'].to_list(): 
                    df.drop([0],inplace=True)

        # --- upload records to DB --- #
        if not pplno.empty:
            dat = pplno.to_dict(orient='records')
            i = 0
            while i+1000 < len(dat):
                COLLECTION_PPLNO_NORTH.insert_many(dat[i:i+1000])
                i += 1000
            COLLECTION_PPLNO_NORTH.insert_many(dat[i:])

        for df, collection in zip([qtd_current,qtd_boarded],
                [COLLECTION_QTD_CURRENT_NORTH,COLLECTION_QTD_BOARDED_NORTH]):
            if not df.empty:
                dat = df.to_dict(orient='records')
                collection.insert_one(dat[0])
        
        loop_time = time.time()-end
        if loop_time < PROCESS_CYCLE:
            time.sleep(PROCESS_CYCLE - loop_time)
    
    except Exception as e:
        print(e)
        time.sleep(PROCESS_CYCLE)
        pass
