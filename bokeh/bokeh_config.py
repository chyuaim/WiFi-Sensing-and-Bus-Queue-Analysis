from bokeh.models import Range1d, FactorRange, HoverTool, DatetimeTickFormatter, BasicTickFormatter
from bokeh.plotting import ColumnDataSource, figure, curdoc
from bokeh.layouts import column, row
import bokeh.palettes as Color
from bokeh.palettes import Viridis5

import pymongo
import pandas as pd
from datetime import datetime, timedelta

DB_KEY = "mongodb://localhost:27017"
COLLECTION = pymongo.MongoClient(DB_KEY)['fyp_2021_busq']
COLLECTION_PPLNO_NORTH          = COLLECTION['pplno_north']
COLLECTION_PPLNO_SOUTH          = COLLECTION['pplno_south']
COLLECTION_QTD_CURRENT_NORTH    = COLLECTION['qtd_current_north']
COLLECTION_QTD_BOARDED_NORTH    = COLLECTION['qtd_boarded_north']
COLLECTION_QTD_CURRENT_SOUTH    = COLLECTION['qtd_current_south']
COLLECTION_QTD_BOARDED_SOUTH    = COLLECTION['qtd_boarded_south']
COLLECTION_QTD_CURRENT_91M      = COLLECTION['qtd_current_91m']
COLLECTION_QTD_CURRENT_11       = COLLECTION['qtd_current_11']
COLLECTION_QTD_CURRENT_104      = COLLECTION['qtd_current_104']
COLLECTION_QTD_CURRENT_91P      = COLLECTION['qtd_current_91p']
COLLECTION_QTD_BOARDED_91M      = COLLECTION['qtd_boarded_91m']
COLLECTION_QTD_BOARDED_11       = COLLECTION['qtd_boarded_11']
COLLECTION_QTD_BOARDED_104      = COLLECTION['qtd_boarded_104']
COLLECTION_QTD_BOARDED_91P      = COLLECTION['qtd_boarded_91p']

def get_south_df(collection):
    df = pd.DataFrame(collection.find({'Time':{'$gte':datetime.now()-timedelta(hours=1)}},{'_id':0,'None':0}).sort('Time',pymongo.DESCENDING).limit(720)) # get latest hour of data
    if df.empty:
        df = df.append([{'Time':datetime.now()-timedelta(hours=1),'B91M':0,'M11':0,'M104':0,'B91P':0},
                            {'Time':datetime.now(),'B91M':0,'M11':0,'M104':0,'B91P':0}], ignore_index=True)
        df = df.resample('5S', on='Time').first().ffill()[['B91M','M11','M104','B91P']].reset_index()
    return df

def get_north_df(collection):
    df = pd.DataFrame(collection.find({'Time':{'$gte':datetime.now()-timedelta(hours=1)}},{'_id':0}).sort('Time',pymongo.DESCENDING).limit(720)) # get latest hour of data
    if df.empty:
        df = df.append([{'Time':datetime.now()-timedelta(hours=1),'Target':0},
                            {'Time':datetime.now(),'Target':0}], ignore_index=True)
        df = df.resample('5S', on='Time').first().ffill()['Target'].reset_index()
    return df

def get_qtd_df(collection):
    df = pd.DataFrame(collection.find({'Time':{'$gte':datetime.now()-timedelta(hours=1)}},{'_id':0}).sort('Time',pymongo.DESCENDING).limit(1))
    if df.empty:
        df = df.append([{'Time':'0-4','Value': 0}, {'Time':'5-9','Value': 0}, {'Time':'10-14','Value': 0}, {'Time':'15-19','Value': 0}, {'Time':'20+','Value': 0}], ignore_index=True)
    else:
        df = df[['T0_4','T5_9','T10_14','T15_19','T20']].transpose().reset_index().rename(columns={'index':'Time',0:'Value'})
        df['Time'] = df['Time'].map({'T0_4':'0-4', 'T5_9':'5-9','T10_14':'10-14','T15_19':'15-19', 'T20':'20+'})
    df['Color'] = Viridis5
    return df

def get_qtd_south_df(collection_91M,collection_11,collection_104,collection_91P):
    df = pd.DataFrame()
    for collection in [collection_91M,collection_11,collection_104,collection_91P]:
        df = pd.concat([df,pd.DataFrame(collection.find({'Time':{'$gte':datetime.now()-timedelta(hours=1)}},{'_id':0}).sort('Time',pymongo.DESCENDING).limit(1))])
    if df.empty:
        df = df.append([{'Time':'0-4','Value': 0}, {'Time':'5-9','Value': 0}, {'Time':'10-14','Value': 0}, {'Time':'15-19','Value': 0}, {'Time':'20+','Value': 0}], ignore_index=True)
    else:
        df = df.groupby('Time').sum().reset_index()
        df = df[['T0_4','T5_9','T10_14','T15_19','T20']].transpose().reset_index().rename(columns={'index':'Time',0:'Value'})
        df['Time'] = df['Time'].map({'T0_4':'0-4', 'T5_9':'5-9','T10_14':'10-14','T15_19':'15-19', 'T20':'20+'})
    df['Color'] = Viridis5
    return df

def get_y_upper(df, y_upper):
    max_value = df.max(numeric_only=True).max()
    if y_upper <= max_value or y_upper >= max_value*2:
        y_upper = int(max_value * 1.25)
    if y_upper < 5:
        y_upper = 5
    return y_upper

def sync_y_upper(df1, df2, y_upper):
    max_value = max(df1.max(numeric_only=True).max(),df2.max(numeric_only=True).max())
    if y_upper <= max_value or y_upper >= max_value*2:
        y_upper = int(max_value * 1.25)
    if y_upper < 5:
        y_upper = 5
    return y_upper


def create_plot(y_upper):
    p = figure(x_axis_label= 'Time', x_axis_type='datetime',
                y_axis_label= 'No. of Ppl.',y_range=Range1d(0,y_upper,bounds='auto'),tools='hover')

    p.xaxis.formatter = DatetimeTickFormatter(microseconds = ['%fus'],milliseconds = ['%3Nms', '%S.%3Ns'],seconds = ['%Ss'],
    minsec = [':%M:%S'],minutes = ['%H:%M:%S'],hourmin = ['%H:%M:%S'],hours = ['%H:%M:%S'],days = ['%m/%d', '%a%d']
    ,months = ['%m/%Y', '%b %Y'],years = ['%Y'])

    p.sizing_mode = 'stretch_both'
    p.toolbar.logo = None
    p.toolbar_location = None

    p.x_range.range_padding = 0

    return p

def create_bar_plot(df,y_upper):
    p = figure(x_range=df['Time'].tolist(), y_range=(0,y_upper), x_axis_label= 'Queue Time (Min)', y_axis_label= 'No. of Ppl.', plot_height=250,
           toolbar_location=None, tools="")
    
    p.sizing_mode = 'stretch_both'
    p.toolbar.logo = None

    return p