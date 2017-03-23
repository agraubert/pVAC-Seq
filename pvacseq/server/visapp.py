### Fetch the data from the postgres server
import postgresql as psql
from bokeh.io import curdoc
import re
import json
import decimal
args = curdoc().session_context.request.arguments
try:
    parentID = int(args.get('target-process')[0])
    fileID = int(args.get('target-file')[0])
    col_maps = json.loads(args.get('cols')[0].decode())
    sample = str(args.get('samplename')[0])
except BaseException as e:
    raise ValueError("Unable to parse the requried arguments") from e
tablekey = "data_%s_%s" % (
    (parentID if parentID >= 0 else 'dropbox'),
    fileID
)
db = psql.open('localhost/pvacseq')
cols = [
    col for (col,) in
    db.prepare('SELECT column_name FROM information_schema.columns WHERE table_name = $1')(tablekey)
]
raw_data = db.prepare("SELECT %s FROM %s"%(','.join(cols), tablekey))()
entries = [
    {
        col:float(val) if isinstance(val, decimal.Decimal) else val
        for (col, val) in zip(cols, entry)
    }
    for entry in raw_data
]
# entries.append({
#     key:0 for key in col_maps
# })
# entries[-1]['rowid']=0
entries.sort(key=lambda x:x['rowid'])
del raw_data
cols = col_maps
### The data is stored in entries.
# Entries is a list of dicts, where each dict maps a cloumn name to a value
# cols is a dict mapping the column names to a display name
# sample is the sample name of the requested file
### From here to the bottom, the code can be changed to modify the plotted data
from bokeh.layouts import row, widgetbox
from bokeh.charts import Scatter
from bokeh.models import ColumnDataSource, HoverTool, Slider
from bokeh.models.ranges import Range1d as Range
from bokeh.models.widgets import Select
from pandas import DataFrame

# a lot of the funkiness in how the plot is updated is because there
# is no low-level API for scatter plots
# Low-level charts can take a ColumnDataSource as the input data,
# and will automatically update when the ColumnDataSource's data is
# changed. Scatter only wants a DataFrame, list of dicts, or dict of lists.
# None of which notify the plot when changed, so instead we have to rebuild
# the plot
x_field = Select(
    title="X-Axis Value",
    options=sorted([
        (key, val) for (key, val) in cols.items()
    ], key = lambda x:x[1]),
    value = 'corresponding_wt_score'
)
y_field = Select(
    title = 'Y-Axis Value',
    options=sorted([
        (key, val) for (key, val) in cols.items()
    ], key = lambda x:x[1]),
    value = 'best_mt_score'
)
binding_filter = Slider(
    start=0,
    end=max(entry['best_mt_score'] for entry in entries),
    value=50000,
    step=10,
    title="Binding Filter",
)
df = DataFrame({
    key:[
        entry[key] for entry in entries
    ]
    for key in cols
}, columns=[key for key in cols])
def update(attr, old, new):
    global df
    x = x_field.value
    y = y_field.value
    xlabel = cols[x]
    ylabel = cols[y]
    df = DataFrame({
        key:[
            entry[key] for entry in entries
        ]
        for key in cols
    }, columns=[key for key in cols])
    # source = ColumnDataSource(df)
    p = Scatter(
        df,
        x=x,
        y=y,
        legend='top_right',
        plot_height=600, plot_width=800,
        title = '%s vs %s'%(ylabel, xlabel),
        xlabel = xlabel,
        ylabel = ylabel,
        tooltips = [
            ('ID', '$index'),
            (xlabel, '@%s'%x),
            (ylabel, '@%s'%y)
        ]
    )

    figure.children[1] = p
    # figure.children[1].x = x
    # figure.children[1].y = y

def update_filters(attr, old, new):
    global df
    global p
    binding_threshold = binding_filter.value
    df = df.where(lambda x:x['best_mt_score']<=binding_threshold)
    p.update()

x_field.on_change('value', update)
y_field.on_change('value', update)
binding_filter.on_change('value', update_filters)
box = widgetbox(
    x_field,
    y_field,
    binding_filter
)
p = Scatter( #dummy plot
    entries[:2],
    x='start',
    y='stop'
)
figure = row(box, p)
update(0,0,0)
curdoc().add_root(figure)
curdoc().title = sample
