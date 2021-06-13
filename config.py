"""config.py

This is a config file for process_south.py and process_north.py. This script must be placed in the same folder along with process_south.py and process_north.py


This script requires that `pandas` be installed within the Python
environment you are running this script in.
"""

import os
import pandas as pd

READ_INTERVAL       = 60*30 # The read inteval is set to half an hour, all data in the past half hour is read by the script
PROCESS_CYCLE       = 60*1 # The cycle for each loop of the script, the script is set to loop every 60 seconds

SCRIPT_DIR          = os.path.dirname(os.path.abspath(__file__)) # The file directory of the script
RESOURCES_DIR       = os.path.join(SCRIPT_DIR, 'resources') # The directory of the folder storing the resources

# a dictionary storing the sensor AP-ID corresponding to their MAC addresses
SENSOR = {1:'E4956E480EC2', 2:'E4956E480EA6',3:'E4956E480DEE',4:'E4956E480ECA',5:'E4956E480E5A',6:'E4956E480E86',7:'E4956E480EB6',8:'E4956E4A4044',9:'E4956E480E42',10:'E4956E4A4048',11:'E4956E4A4054'}
NORTH_GATE_RECEIVER = [SENSOR[i] for i in range(1,5)] # a list storing the MAC addresses of the sensors located in the North Gate
SOUTH_GATE_RECEIVER = [SENSOR[i] for i in range(5,12)] # a list storing the MAC addresses of the sensors located in the South Gate

# DB_KEY is used to connect to MongoDB
DB_KEY = "mongodb://localhost:27017"

QTD_GROUP = ['0-5','5-10','10-15','15-20','20+'] # categories for queue time distribution

# functions
def load_dat(start, end, filenames):
    """Read all .dat files within the time interval defined by start and end

    Parameters
    ----------
    start : datetime
        the starting time

    end : datetime
        the ending time

    filenames : dict
        a dictionary where the keys are the date folders and the value of a key is  list of filenames of .dat files inside the corresponding date folder

    Returns
    -------
    DataFrame
        a list of dataframe that match the time interval with the columns Time, Target, Receiver and Strength
    """
    for date in filenames.keys():
        for name in filenames[date]:
            f_time = int(name[:-4])
            if f_time>=start and f_time<=end:
                yield pd.read_csv(os.path.join(DATA_DIR,date,name), names = ['Time','Target','Receiver','Strength'])


def queue_dist(timedelta):
    """Classify a time period to a queue time group

    Parameters
    ----------
    path : Timedelta
        The time period

    Returns
    -------
    str
        a label indicating the queue time group
    """
    global QTD_GROUP
    if timedelta < pd.Timedelta(minutes=5):
        return QTD_GROUP[0]
    elif timedelta < pd.Timedelta(minutes=10):
        return QTD_GROUP[1]
    elif timedelta < pd.Timedelta(minutes=15):
        return QTD_GROUP[2]
    elif timedelta < pd.Timedelta(minutes=20):
        return QTD_GROUP[3]
    else:
        return QTD_GROUP[4]

def qtd_transformation(df, timestamp):
    """Pivot a dataframe to generate the queue time distribution table

    Parameters
    ----------
    df : DataFrame
        The input dataframe with the target MAC addresses and their queue time labels
    timestamp: datetime
        The time value of the table
    Returns
    -------
    Dataframe
        a one-row dataframe with the number of people queuing for each of the queue time labels
    """
    global QTD_GROUP
    df = df['Queue_Time'].value_counts().reset_index().transpose()
    df = df.rename(columns=df.iloc[0]).drop(df.index[0]).reset_index(drop=True)
    if df.empty:
        df = pd.DataFrame({item:[0] for item in QTD_GROUP})
    df['Time'] = timestamp
    for item in QTD_GROUP:
        if item not in df.columns:
            df[item] = 0
    df = df[['Time']+QTD_GROUP]
    return df