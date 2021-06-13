from bokeh.models import HoverTool
from bokeh.plotting import ColumnDataSource, curdoc

import pymongo

import bokeh_config
from bokeh_config import get_north_df, get_y_upper, create_plot

collection = bokeh_config.COLLECTION_PPLNO_NORTH

df = get_north_df(collection)

source = ColumnDataSource(data=df)

y_upper = get_y_upper(df,0)

#Generate graph
p = create_plot(y_upper)

p.line(x='Time', y='Target', source=source, line_width=2, alpha=0.7)
hover = p.select(dict(type=HoverTool))
hover.tooltips = [('Time', '@Time{%H:%M:%S}'),  ('No. of ppl.', '@Target')]
hover.formatters={'@Time':'datetime'}
hover.mode = 'mouse'

def callback():
    global y_upper, p, collection, source
    
    df = get_north_df(collection)

    y_upper = get_y_upper(df,y_upper)
    
    p.y_range.end = y_upper
    
    source.data = df

curdoc().add_root(p)
refreshRate = 5000
curdoc().add_periodic_callback(callback, refreshRate)