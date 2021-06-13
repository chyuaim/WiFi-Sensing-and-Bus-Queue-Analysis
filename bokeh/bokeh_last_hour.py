from bokeh.models import HoverTool
from bokeh.plotting import ColumnDataSource, curdoc
import bokeh.palettes as Color

import pymongo

import bokeh_config
from bokeh_config import get_south_df, get_y_upper, create_plot

collection = bokeh_config.COLLECTION_PPLNO_SOUTH

df = get_south_df(collection)

source = ColumnDataSource(data=df)

df['Total'] = df[['B91M','M11','M104','B91P']].sum(axis=1)
y_upper = get_y_upper(df[['Time','Total']],0)

#Generate graph
p = create_plot(y_upper)

labels = df[['B91M','M11','M104','B91P']].sum().reset_index().sort_values(by=0,ascending=False)['index'].tolist()

for col, colour in zip(labels, Color.viridis(4)):
    p.line(x='Time', y=col, source=source, legend_label=col[1:], line_width=2, color=colour, alpha=0.7)
    hover = p.select(dict(type=HoverTool))
    hover.tooltips = [('Time', '@Time{%H:%M:%S}'),  ('91M', '@B91M'), ('11', '@M11'), ('104', '@M104'), ('91P', '@B91P')]
    hover.formatters={'@Time':'datetime'}
    hover.mode = 'mouse'

p.legend.click_policy = "hide"

def callback():
    global y_upper, p, collection, source
    
    df = get_south_df(collection)

    df['Total'] = df[['B91M','M11','M104','B91P']].sum(axis=1)
    y_upper = get_y_upper(df[['Time','Total']],y_upper)
    
    p.y_range.end = y_upper
    
    source.data = df

curdoc().add_root(p)
refreshRate = 5000
curdoc().add_periodic_callback(callback, refreshRate)