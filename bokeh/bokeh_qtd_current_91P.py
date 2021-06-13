from bokeh.plotting import ColumnDataSource, curdoc

import pymongo

import bokeh_config
from bokeh_config import get_qtd_df, sync_y_upper, create_bar_plot

collection = bokeh_config.COLLECTION_QTD_CURRENT_91P
collection_boarded = bokeh_config.COLLECTION_QTD_BOARDED_91P

df = get_qtd_df(collection)

y_upper = sync_y_upper(df,get_qtd_df(collection_boarded),0)

#Generate graph
source = ColumnDataSource(data=df)
p = create_bar_plot(df,y_upper)

p.vbar(x='Time', top='Value', width=0.3, color='Color', source=source, fill_alpha=0.2)

def callback():
    global y_upper, p, collection, collection_boarded, source

    df = get_qtd_df(collection)

    y_upper = sync_y_upper(df,get_qtd_df(collection_boarded),y_upper)
    
    p.y_range.end = y_upper
    
    source.data = df

curdoc().add_root(p)
refreshRate = 5000
curdoc().add_periodic_callback(callback, refreshRate)