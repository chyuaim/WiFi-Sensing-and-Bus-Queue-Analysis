from bokeh.models import HoverTool
from bokeh.plotting import ColumnDataSource, curdoc

import pymongo
import pandas as pd

import bokeh_config
from bokeh_config import get_south_df, get_y_upper, create_plot

collection = bokeh_config.COLLECTION_PPLNO_SOUTH

df = get_south_df(collection)
df['Value'] = df[['B91M','M11','M104','B91P']].sum(axis=1)

source = ColumnDataSource(data=df)

#Generate graph
y_upper = get_y_upper(df,0)

p = create_plot(y_upper)

p.line(x='Time', y='Value', source=source, line_width=2, alpha=0.7)
hover = p.select(dict(type=HoverTool))
hover.tooltips = [('Time', '@Time{%H:%M:%S}'),  ('No. of ppl.', '@Value')]
hover.formatters={'@Time':'datetime'}
hover.mode = 'mouse'

def callback():
    global y_upper, p, collection, source

    df = get_south_df(collection)
    df['Value'] = df[['B91M','M11','M104','B91P']].sum(axis=1)

    y_upper = get_y_upper(df,y_upper)

    p.y_range.end = y_upper
    
    source.data = df

curdoc().add_root(p)
refreshRate = 5000
curdoc().add_periodic_callback(callback, refreshRate)