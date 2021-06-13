"""gen_filter_list.py

This script reads the sensor readings of the 3 days from the /data directory,
it identifies the MAC addresses of the devices with the unwanted signals, then store them in 
filter_list.txt in the /resources directory.

This tool should run continuously and update filter_list.txt every hour
in order to identify noises in real-time.

This script requires that `pandas` be installed within the Python
environment you are running this script in.

"""

import os
from datetime import datetime
import time
import config
import pandas as pd
from base64 import b64decode
import json
import pymongo

RESOURCES_DIR       = config.RESOURCES_DIR
READ_INTERVAL       = 60*60*24*3
PROCESS_CYCLE       = 60*30

SENSOR              = config.SENSOR 

DB_KEY = config.DB_KEY
COLLECTION = pymongo.MongoClient(DB_KEY)['fyp_2021_busq']
COLLECTION_RAW_DATA = COLLECTION['raw_data']

while True:
    try:
        # --- define time interval --- #
        start = time.time() - READ_INTERVAL
        end = time.time() 

        data = pd.DataFrame(COLLECTION_RAW_DATA.find({'Time':{'$gte':datetime.fromtimestamp(start),'$lte':datetime.fromtimestamp(end)}},{'_id':0}).sort('Time',pymongo.ASCENDING))
        if data.empty:
            time.sleep(PROCESS_CYCLE)
            continue
            
        data = data.dropna()
        data = pd.DataFrame(data.groupby(['Time','Target','Receiver']).max().reset_index())

        # --- decode the MAC addresses of the target devices and the sensors --- #
        data = data.assign(Target= data['Target'].apply(lambda s: b64decode(s.encode()).hex().upper()))
        data = data.assign(Receiver = data['Receiver'].map(SENSOR))
        data = data.assign(Strength = data['Strength']-100)

        # --- filter existing noises to reduce the size of the dataset  --- #
        with open(os.path.join(RESOURCES_DIR,"mac_prefix.txt"), "r") as fp:
            mac_prefix = json.load(fp)

        # data = data[(data['Target'].str[:6].isin(mac_prefix)) | (data['Target'].str[1] == '2') | (data['Target'].str[1] == '6') | (data['Target'].str[1] == 'A') | (data['Target'].str[1] == 'E')].reset_index(drop=True)
        data = data[data['Target'].str[:6].isin(mac_prefix)].reset_index(drop=True) # Without Virtual MAC Address

        second_filter = pd.DataFrame(data['Target'].value_counts())
        data = data[~data['Target'].isin(second_filter[second_filter['Target']==1].index)]

        # --- target 1: midnight data  --- #
        midnight_data = data[(data['Time'].dt.hour >= 2) & (data['Time'].dt.hour <= 4)]
        noise_target = midnight_data['Target'].unique().tolist()

        data = data[~data['Target'].isin(noise_target)]

        # --- target 2: frequently existing devices --- #
        data['Date'] = data['Time'].dt.date
        freq_df = data.groupby(['Date','Target']).agg({'Time':lambda x:x.dt.hour.unique().size}).reset_index()
        freq_df = freq_df[freq_df['Time'] >= 6] # frequently existing devices
        noise_target.extend(freq_df['Target'].unique().tolist())

        data = data[~data['Target'].isin(noise_target)]

        # --- target 3: long staying devices --- #
        data = data[data['Time'] > data['Time'].max()-pd.Timedelta(hours=6)]
        second_filter = pd.DataFrame(data['Target'].value_counts())
        data = data[~data['Target'].isin(second_filter[second_filter['Target']<=10].index)] # for long staying device, more than 10 entries of record is expected
        for target in data['Target'].unique().tolist():
            time_diff = data[data['Target'] == target]['Time'].diff()
            time_diff = time_diff[time_diff <= pd.Timedelta(minutes=30)]
            if time_diff.sum() > pd.Timedelta(minutes=90): # filter signals that last for too long
                noise_target.append(target)

        # --- store the MAC addresses of the target to a list --- #
        with open(os.path.join(RESOURCES_DIR, 'filter_list.txt'), 'w') as fp:
            json.dump(noise_target, fp)

        loop_time = time.time()-end
        if loop_time < PROCESS_CYCLE:
            time.sleep(PROCESS_CYCLE - loop_time)
 
    except Exception as e:
        print(e)
        time.sleep(PROCESS_CYCLE)
        continue
