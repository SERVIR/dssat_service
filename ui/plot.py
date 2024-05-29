"""
Functions to create highchart plots
"""

from highcharts_core.chart import Chart
from highcharts_core.chart import HighchartsOptions
from highcharts_core.options.series.bar import ColumnRangeSeries
from highcharts_core.options.plot_options.bar import ColumnRangeOptions
from highcharts_core.options.series.scatter import ScatterSeries
from highcharts_core.options.plot_options.scatter import ScatterOptions

from base import AdminBase

Q_RANGE = (.05, .95)
def columnRange_data(df):
    sim_data = df.groupby("year").sim.quantile(Q_RANGE)
    sim_data = sim_data.sort_index().round(2)
    data = []
    for year in sim_data.index.get_level_values(0).unique():
        data.append((year, sim_data.loc[year, Q_RANGE[0]], sim_data.loc[year, Q_RANGE[1]]))
    return data

def validation_chart(adminBase:AdminBase):
    my_chart = Chart()
    my_chart.options = HighchartsOptions()
    my_chart.options.title = {
        'text': f'Observed and Simulated yield for the baseline scenario in {adminBase.admin1}', 
        "style": {
            "font-size": "15px"
        }
    }
    my_chart.options.y_axis = {
        "title": {
            'text': 'Yield (kg/ha)', 
            "style": {
                "font-size": "15px"
            }
        },
        "labels": {
            "style": {
                "font-size": "15px",
            }
        }
    }
    my_chart.options.x_axis = {
        "title": {
            'text': 'Year', 
            "style": {
                "font-size": "15px",
            }
        },
        "labels": {
            "style": {
                "font-size": "15px",
            }
        }
    }
    
    tmp_df = adminBase.validation_run
    # Column bar series
    data = columnRange_data(tmp_df)
    column = ColumnRangeSeries().from_array(data)
    column.name = "Simulated (90% CI)"
    column.color = "#00cc66"
    my_chart.add_series(column)
    # Observed scatterplot
    data = tmp_df.groupby("year").obs.mean().round(3).reset_index().to_numpy()
    scatter = ScatterSeries().from_array(data)
    scatter.name = "Observed"
    scatter.color = "#003300"
    scatter.marker = {"symbol": "square", "radius": 6}

    my_chart.add_series(scatter)
    return my_chart



