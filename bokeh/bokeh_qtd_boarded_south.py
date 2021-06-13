from bokeh.plotting import ColumnDataSource, curdoc

import pymongo
import pandas as pd

import bokeh_config
from bokeh_config import get_qtd_south_df, sync_y_upper, create_bar_plot

COLLECTION_BOARDED_91M = bokeh_config.COLLECTION_QTD_BOARDED_91M
COLLECTION_BOARDED_11 = bokeh_config.COLLECTION_QTD_BOARDED_11
COLLECTION_BOARDED_104 = bokeh_config.COLLECTION_QTD_BOARDED_104
COLLECTION_BOARDED_91P = bokeh_config.COLLECTION_QTD_BOARDED_91P

COLLECTION_CURRENT_91M = bokeh_config.COLLECTION_QTD_CURRENT_91M
COLLECTION_CURRENT_11 = bokeh_config.COLLECTION_QTD_CURRENT_11
COLLECTION_CURRENT_104 = bokeh_config.COLLECTION_QTD_CURRENT_104
COLLECTION_CURRENT_91P = bokeh_config.COLLECTION_QTD_CURRENT_91P

df = get_qtd_south_df(COLLECTION_BOARDED_91M,COLLECTION_BOARDED_11,COLLECTION_BOARDED_104,COLLECTION_BOARDED_91P) 

df_current = get_qtd_south_df(COLLECTION_CURRENT_91M,COLLECTION_CURRENT_11,COLLECTION_CURRENT_104,COLLECTION_CURRENT_91P)

y_upper = sync_y_upper(df,df_current,0)

#Generate graph
source = ColumnDataSource(data=df)
p = create_bar_plot(df,y_upper)

p.vbar(x='Time', top='Value', width=0.3, color='Color', source=source, fill_alpha=0.2)

def callback():
    global y_upper, p, COLLECTION_CURRENT_91M, COLLECTION_CURRENT_91M, COLLECTION_CURRENT_91M, COLLECTION_CURRENT_91M, COLLECTION_BOARDED_91M,COLLECTION_BOARDED_11,COLLECTION_BOARDED_104,COLLECTION_BOARDED_91P, source

    df = get_qtd_south_df(COLLECTION_BOARDED_91M,COLLECTION_BOARDED_11,COLLECTION_BOARDED_104,COLLECTION_BOARDED_91P) 

    df_current = get_qtd_south_df(COLLECTION_CURRENT_91M,COLLECTION_CURRENT_11,COLLECTION_CURRENT_104,COLLECTION_CURRENT_91P)

    y_upper = sync_y_upper(df,df_current,y_upper)
    
    p.y_range.end = y_upper
    
    source.data = df

curdoc().add_root(p)
refreshRate = 5000
curdoc().add_periodic_callback(callback, refreshRate)