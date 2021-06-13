import socket
import threading
import json
import pandas as pd
import time
import os
import shutil
import datetime
import config
from base64 import b64encode
import pymongo


DATA_DIR = os.path.join(os.path.dirname(__file__),'data')
SENSOR_MAP = {v:k for k,v in config.SENSOR.items()}
STORE_INTERVAL = 30 # seconds
DB_KEY = config.DB_KEY
COLLECTION = pymongo.MongoClient(DB_KEY)['fyp_2021_busq']['raw_data']

class UdpService:

    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.df = []
        self.interval_counter = time.time()
        # udp connection
        try:
            udp_threading = threading.Thread(target=self.server_udp)
            udp_threading.start()
        except socket.error as e:
            pass

    # set up udp server
    def server_udp(self):
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp_socket.bind((self.ip, self.port))
        while True:
            try:
                content, dest_info = udp_socket.recvfrom(1024)
                raw_data = content.decode()
                self.df.append(json.loads(raw_data))
                
                if time.time()-self.interval_counter >= STORE_INTERVAL:
                    threading.Thread(target=self.save_file, args=(pd.DataFrame(self.df),)).start()
                    self.df = []
                    self.interval_counter = time.time()

            except Exception as e:
                pass

    def save_file(self, df):
        df = df.rename(columns={'srvTime':'Time','txAddr':'Target','rxAddr':'Receiver','rssi':'Strength'})
        df = df.assign(Time = pd.to_datetime(df['Time'],unit='ms').dt.tz_localize(tz='Etc/GMT+8').dt.tz_convert(tz=None).dt.floor('S'))
        df = df.assign(Target = df['Target'].apply(lambda s: b64encode(bytes.fromhex(str(s))).decode()))
        df = df.assign(Receiver = df['Receiver'].map(SENSOR_MAP))
        df = df.assign(Strength = df['Strength'] + 100)
        df = df.groupby(['Time','Target','Receiver'])['Strength'].max().reset_index()
        if not df.empty:
            dat = df.to_dict(orient='records')
            i = 0
            while i+1000 < len(dat):
                COLLECTION.insert_many(dat[i:i+1000])
                i += 1000
            COLLECTION.insert_many(dat[i:])
    
UdpService('0.0.0.0', 3650)
