"""process_south.py

This script reads the sensor readings of the past hour from the /data directory,
it filters unwanted signals and applies classifacation model to the data to derives
the number of people queuing for each of the bus stops in the South Gate, 
the queue time distribution of current queuing passengers at each of the bus stops, and 
the aggregated queue time distribution of boarded passengers. The results are uploaded 
to MongoDB Atlas for further usages.

This tool should run continuously and undergo the filtering and classification procedures every 60 seconds 
in order to get the real-time results.

This script requires that `pandas`, `numpy`, `sklearn`, `torch` and `pymongo` be installed within the Python
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
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader, WeightedRandomSampler
from sklearn.preprocessing import MinMaxScaler, MaxAbsScaler  
from sklearn.model_selection import train_test_split
from sklearn.cluster import KMeans
import pymongo
import config
from config import queue_dist, qtd_transformation

# --- Assign global variables --- #
SENSOR              = config.SENSOR 
SOUTH_GATE_RECEIVER = config.SOUTH_GATE_RECEIVER

READ_INTERVAL       = config.READ_INTERVAL 
PROCESS_CYCLE       = config.PROCESS_CYCLE

RESOURCES_DIR       = config.RESOURCES_DIR

# DB_KEY is used to access the MongoDB
DB_KEY = config.DB_KEY
COLLECTION = pymongo.MongoClient(DB_KEY)['fyp_2021_busq']
COLLECTION_RAW_DATA = COLLECTION['raw_data']
COLLECTION_PPLNO_SOUTH = COLLECTION['pplno_south']
COLLECTION_QTD_CURRENT_91M = COLLECTION['qtd_current_91m']
COLLECTION_QTD_CURRENT_11 = COLLECTION['qtd_current_11']
COLLECTION_QTD_CURRENT_104 = COLLECTION['qtd_current_104']
COLLECTION_QTD_CURRENT_91P = COLLECTION['qtd_current_91p']
COLLECTION_QTD_BOARDED_91M = COLLECTION['qtd_boarded_91m']
COLLECTION_QTD_BOARDED_11 = COLLECTION['qtd_boarded_11']
COLLECTION_QTD_BOARDED_104 = COLLECTION['qtd_boarded_104']
COLLECTION_QTD_BOARDED_91P = COLLECTION['qtd_boarded_91p']

# --- Class set up of Neural Network model for classification --- #
class ClassifierDataset(Dataset):
    """
    A class used to represent the Dataset for the classification model

    ...

    Attributes
    ----------
    X_data : str
        a numpy array for the attributes used for training
    y_data : str
        a numpy array for the labels

    Methods
    -------
    __getitem__(index)
        returns the row of data with the corresponding index
    __len__(index)
        returns the length of X_data
    """
    def __init__(self, X_data, y_data = None):
        self.X_data = X_data
        self.y_data = y_data
        
    def __getitem__(self, index):
        return self.X_data[index], self.y_data[index]
        
    def __len__ (self):
        return len(self.X_data)

class Classification(nn.Module):
    """
    A class used to represent the Neural Network structure for the classification model

    ...

    Attributes
    ----------
    layer_1 : Linear (n_feature to 256)
    layer_2 : Linear (256 to 128)
    layer_3:  Linear (128 to 64)
    layer_out: Linear (64 to n_zone)

    Methods
    -------
    forward(x)
        step forward
    """
    def __init__(self, n_feature, n_zone):
        super(Classification, self).__init__()
        
        self.layer_1 = nn.Linear(n_feature, 256)
        self.layer_2 = nn.Linear(256,128)
        self.layer_3 = nn.Linear(128,64)
        self.layer_out = nn.Linear(64, n_zone)
        
        self.ReLU = nn.ReLU()
        
    def forward(self, x):
        x = self.layer_1(x)
        x = self.ReLU(x)
        
        x = self.layer_2(x)
        x = self.ReLU(x)
        
        x = self.layer_3(x)
        x = self.ReLU(x)
        
        x = self.layer_out(x)
        
        return x

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
        data = data.assign(Target = data['Target'].apply(lambda s: b64decode(s.encode()).hex().upper()))
        data = data.assign(Receiver = data['Receiver'].map(SENSOR))
        data = data.assign(Strength = data['Strength']-100)
        server_time = data['Time'].max()

        # --- get only south gate data --- #
        south_data = data[data['Receiver'].isin(SOUTH_GATE_RECEIVER)]
        south_data = south_data[south_data['Strength'] < 0]

        # --- filter weak signals --- #
        weak_target = south_data.groupby(['Target'])['Strength'].max().reset_index()
        weak_target = weak_target[weak_target['Strength'] < -70]['Target'].tolist()
        south_data = south_data[~south_data['Target'].isin(weak_target)]
 
        # --- filter data in the list --- #
        with open(os.path.join(RESOURCES_DIR,"filter_list.txt"), "r") as fp:
            noise_target = json.load(fp)
        south_data = south_data[~south_data['Target'].isin(noise_target)]

        with open(os.path.join(RESOURCES_DIR,"mac_prefix.txt"), "r") as fp:
            mac_prefix = json.load(fp)

        # south_data = south_data[(south_data['Target'].str[:6].isin(mac_prefix)) | (south_data['Target'].str[1] == '2') | (south_data['Target'].str[1] == '6') | (south_data['Target'].str[1] == 'A') | (south_data['Target'].str[1] == 'E')].reset_index(drop=True) # with virtual mac
        south_data = south_data[south_data['Target'].str[:6].isin(mac_prefix)].reset_index(drop=True) # without virtual mac

        # --- filter signal which appear only once --- #
        second_filter = pd.DataFrame(south_data['Target'].value_counts())
        south_data = south_data[~south_data['Target'].isin(second_filter[second_filter['Target']==1].index)]

        # --- assign a column for each sensor --- #
        south_data = pd.pivot_table(south_data,index=['Time','Target'],columns='Receiver',values='Strength',fill_value=-100).reset_index()
        for sensor in SOUTH_GATE_RECEIVER:
            if sensor not in south_data:
                south_data[sensor] = -100
        south_data.columns.name = None

        # --- pool and fit classification model --- #
        if not south_data.empty:
            south_data['Time_I'] = south_data['Time'].dt.floor('2T')
            grouped = south_data.groupby(['Time_I','Target'])[SOUTH_GATE_RECEIVER].mean().reset_index()
            grouped = grouped.fillna(0)
            grouped.iloc[:,2:] = grouped.iloc[:,2:].astype(int)
            
            model = Classification(n_feature = 7, n_zone = 4)
            model.load_state_dict(torch.load(os.path.join(RESOURCES_DIR,'model'),map_location=torch.device('cpu')))

            X_sub = grouped.iloc[:,2:9]
            y_sub = grouped.iloc[:10]

            scaler = MinMaxScaler()
            X_sub = scaler.fit_transform(X_sub)
            y_sub =  np.zeros(len(X_sub))

            sub_dataset = ClassifierDataset(torch.from_numpy(X_sub).float(), torch.from_numpy(y_sub))
            sub_loader = DataLoader(dataset = sub_dataset, batch_size = 1)

            with torch.no_grad():
                sub_pred = []
                for inputs, labels in sub_loader:
                    outputs = model(inputs)
                    _, predicted = torch.max(outputs, 1)
                    sub_pred.append(predicted.item()+1)
                    
            grouped['Zone'] = pd.Series(sub_pred)

            south_data = south_data[['Time','Target','Time_I']].merge(grouped,how='left')
            south_data = south_data[['Time','Target','Zone']]

        # --- upsample data --- #
        new_df = pd.DataFrame()
        for target in south_data['Target'].unique().tolist():
            target_dst = south_data[south_data['Target'] == target].reset_index(drop=True)
            # target_dst = target_dst[target_dst['Zone'] == target_dst['Zone'].mode().values[0]].reset_index(drop=True)
            target_dst['Zone'] = target_dst['Zone'].mode().values[0]

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

            # --- assign Zones --- #
            zone_dict = {1:'B91M',2:'M11',3:'M104',4:'B91P',0:'None'}
            zones = pd.pivot(new_df.groupby(['Time','Zone']).count().reset_index(),index='Time',columns='Zone',values='Target').fillna(0)
            zones = zones.reset_index()
            zones.columns.name = None
            zones = zones.rename(columns = zone_dict)
            for zone in zone_dict.values():
                if zone not in zones:
                    zones[zone] = 0
            zones = zones[['Time','B91M','M11','M104','B91P','None']].fillna(0)
            if zones['Time'].max() < server_time:
                zones = zones.append({'Time':server_time,'B91M':0,'M11':0,'M104':0,'B91P':0,'None':0}, ignore_index=True)
            zones = zones.resample('5S',on='Time').first().fillna(0).drop(columns='Time').reset_index()

            # --- define dataframe for queue time distribution of current waiting passengers --- #
            last_appearence = new_df.groupby('Target')[['Time','Zone']].last().reset_index().rename(columns={'Time':'Last_Apppearence'})
            qtd_current = new_df[['Time','Target','Zone']].merge(last_appearence,how='left')
            qtd_current = qtd_current[qtd_current['Last_Apppearence'] >= server_time - pd.Timedelta(seconds=PROCESS_CYCLE)]
            first_appearence = qtd_current.groupby('Target')[['Time','Zone','Last_Apppearence']].first().reset_index().rename(columns={'Time':'First_Appearence'})
            qtd_current = qtd_current.merge(first_appearence,how='left')
            qtd_current['Queue_Time'] = qtd_current['Last_Apppearence'] - qtd_current['First_Appearence']
            qtd_current = qtd_current.assign(Queue_Time = qtd_current['Queue_Time'].apply(queue_dist))
            qtd_current = qtd_current.groupby('Target')[['Zone','Queue_Time']].first().reset_index()

            # 91M
            qtd_current_91M = qtd_current[qtd_current['Zone'] == 1]
            qtd_current_91M = qtd_transformation(qtd_current_91M,server_time.floor('T'))

            # 11
            qtd_current_11 = qtd_current[qtd_current['Zone'] == 2]
            qtd_current_11 = qtd_transformation(qtd_current_11,server_time.floor('T'))

            # 104
            qtd_current_104 = qtd_current[qtd_current['Zone'] == 3]
            qtd_current_104 = qtd_transformation(qtd_current_104,server_time.floor('T'))

            # 91P
            qtd_current_91P = qtd_current[qtd_current['Zone'] == 4]
            qtd_current_91P = qtd_transformation(qtd_current_91P,server_time.floor('T'))

            # --- define dataframe for queue time distribution of boarded passengers in the past hour --- #
            qtd_boarded = new_df[['Time','Target','Zone']].merge(last_appearence,how='left')
            qtd_boarded = qtd_boarded[qtd_boarded['Last_Apppearence'] < server_time - pd.Timedelta(seconds=PROCESS_CYCLE)]
            first_appearence = qtd_boarded.groupby('Target')[['Time','Zone','Last_Apppearence']].first().reset_index().rename(columns={'Time':'First_Appearence'})
            qtd_boarded = qtd_boarded.merge(first_appearence,how='left')
            qtd_boarded['Queue_Time'] = qtd_boarded['Last_Apppearence'] - qtd_boarded['First_Appearence']
            qtd_boarded = qtd_boarded.assign(Queue_Time = qtd_boarded['Queue_Time'].apply(queue_dist))
            qtd_boarded = qtd_boarded.groupby('Target')[['Zone','Queue_Time']].first().reset_index()

            # 91M
            qtd_boarded_91M = qtd_boarded[qtd_boarded['Zone'] == 1]
            qtd_boarded_91M = qtd_transformation(qtd_boarded_91M,server_time.floor('T'))

            # 11
            qtd_boarded_11 = qtd_boarded[qtd_boarded['Zone'] == 2]
            qtd_boarded_11 = qtd_transformation(qtd_boarded_11,server_time.floor('T'))

            # 104
            qtd_boarded_104 = qtd_boarded[qtd_boarded['Zone'] == 3]
            qtd_boarded_104 = qtd_transformation(qtd_boarded_104,server_time.floor('T'))

            # 91P
            qtd_boarded_91P = qtd_boarded[qtd_boarded['Zone'] == 4]
            qtd_boarded_91P = qtd_transformation(qtd_boarded_91P,server_time.floor('T'))

        else:
            zones = pd.DataFrame([{'Time':server_time-timedelta(seconds=READ_INTERVAL),'B91M':0,'M11':0,'M104':0,'B91P':0,'None':0},
                            {'Time':server_time,'B91M':0,'M11':0,'M104':0,'B91P':0,'None':0}])
            zones = zones.resample('5S', on='Time').first().fillna(0)[['B91M','M11','M104','B91P','None']].reset_index()

            qtd_current_91M = pd.DataFrame([{'Time':server_time.floor('T'),'0-5':0,'5-10':0,'10-15':0,'15-20':0, '20+':0}])
            qtd_current_11 = pd.DataFrame([{'Time':server_time.floor('T'),'0-5':0,'5-10':0,'10-15':0,'15-20':0, '20+':0}])
            qtd_current_104 = pd.DataFrame([{'Time':server_time.floor('T'),'0-5':0,'5-10':0,'10-15':0,'15-20':0, '20+':0}])
            qtd_current_91P = pd.DataFrame([{'Time':server_time.floor('T'),'0-5':0,'5-10':0,'10-15':0,'15-20':0, '20+':0}])

            qtd_boarded_91M = pd.DataFrame([{'Time':server_time.floor('T'),'0-5':0,'5-10':0,'10-15':0,'15-20':0, '20+':0}])
            qtd_boarded_11 = pd.DataFrame([{'Time':server_time.floor('T'),'0-5':0,'5-10':0,'10-15':0,'15-20':0, '20+':0}])
            qtd_boarded_104 = pd.DataFrame([{'Time':server_time.floor('T'),'0-5':0,'5-10':0,'10-15':0,'15-20':0, '20+':0}])
            qtd_boarded_91P = pd.DataFrame([{'Time':server_time.floor('T'),'0-5':0,'5-10':0,'10-15':0,'15-20':0, '20+':0}])

        zones[['B91M','M11','M104','B91P','None']] = zones[['B91M','M11','M104','B91P','None']].astype(int)

        # --- filter records that already uploaded to DB --- #
        previous_zones = pd.DataFrame(COLLECTION_PPLNO_SOUTH.find({'Time':{'$gte':datetime.now()-timedelta(hours=1)}},{'_id':0,'None':0}).sort('Time',pymongo.DESCENDING).limit(720))
        if not previous_zones.empty:
            zones = zones[~zones['Time'].isin(previous_zones['Time'])].reset_index(drop=True)

        for df, collection in zip([qtd_current_91M,qtd_current_11,qtd_current_104,qtd_current_91P,
                                    qtd_boarded_91M,qtd_boarded_11,qtd_boarded_104,qtd_boarded_91P],
                                    [COLLECTION_QTD_CURRENT_91M,COLLECTION_QTD_CURRENT_11,COLLECTION_QTD_CURRENT_104,COLLECTION_QTD_CURRENT_91P,
                                    COLLECTION_QTD_BOARDED_91M,COLLECTION_QTD_BOARDED_11,COLLECTION_QTD_BOARDED_104,COLLECTION_QTD_BOARDED_91P]):
            previous_durations = pd.DataFrame(collection.find({'Time':{'$gte':datetime.now()-timedelta(hours=1)}},{'_id':0}).sort('Time',pymongo.DESCENDING).limit(60))
            if not previous_durations.empty: 
                if df['Time'].iloc[0] in previous_durations['Time'].to_list(): 
                    df.drop([0],inplace=True)
       
        # --- upload records to DB --- #
        if not zones.empty:
            dat = zones.to_dict(orient='records')
            i = 0
            while i+1000 < len(dat):
                COLLECTION_PPLNO_SOUTH.insert_many(dat[i:i+1000])
                i += 1000
            COLLECTION_PPLNO_SOUTH.insert_many(dat[i:])

        for df, collection in zip([qtd_current_91M,qtd_current_11,qtd_current_104,qtd_current_91P,
                                    qtd_boarded_91M,qtd_boarded_11,qtd_boarded_104,qtd_boarded_91P],
                                    [COLLECTION_QTD_CURRENT_91M,COLLECTION_QTD_CURRENT_11,COLLECTION_QTD_CURRENT_104,COLLECTION_QTD_CURRENT_91P,
                                    COLLECTION_QTD_BOARDED_91M,COLLECTION_QTD_BOARDED_11,COLLECTION_QTD_BOARDED_104,COLLECTION_QTD_BOARDED_91P]):
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
