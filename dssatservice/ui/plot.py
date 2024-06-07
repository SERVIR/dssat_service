"""
Functions to create highchart plots
"""

from highcharts_core.chart import Chart
from highcharts_core.chart import HighchartsOptions
from highcharts_core.options.series.bar import ColumnRangeSeries
from highcharts_core.options.plot_options.bar import ColumnRangeOptions
from highcharts_core.options.series.scatter import ScatterSeries
from highcharts_core.options.plot_options.scatter import ScatterOptions
from highcharts_core.options.series.bar import ColumnSeries
from highcharts_core.options.plot_options.bar import ColumnOptions
from highcharts_core.options.legend import Legend

from .base import AdminBase, Session
from data.transform import parse_overview

import numpy as np

SERIES_CI = [95, 75, 50, 25]
Q_RANGE_PLOTS = [(.5-q/200, .5+q/200) for q in SERIES_CI]
# COLORS = ["#99ff99", "#66ff66", "#33cc33", "#009933"]
COLORS = ["#66ff66", "#33cc33", "#009933", "#006600"]

Q_RANGE_PLOTS = (
    (0.005, 0.995), (0.05, 0.95), (0.125, 0.875), (0.25, 0.75),
)

DEV_STAGES = [
    'Emergence-End Juvenile', 'End Juvenil-Floral Init',
    'Floral Init-End Lf Grow', 'End Lf Grth-Beg Grn Fil',
    'Grain Filling Phase'
]
DEV_STAGES_LABELS = [
    "Emerg.-End<br>Juv.", "End Juv-<br>Flor Init", "Flor Init-<br>End Lf Gro",
    "End lf Gro-<br>Beg Grain<br>Fil", "Grain<br>Fill"
]
STRESS_COLUMNS = {
    "water": "watStress",
    "nitrogen":  "nitroStress"
}
CAT_NAMES = ["Very low", "Low", "Normal", "High", "Very high"]
CAT_COLORS = ['#cc0000', "#ff9933", "#ffff66", "#99cc00", "#009933"]

def columnRange_data(df, qrange=(.05, .95)):
    sim_data = df.groupby("year").sim.quantile(qrange)
    sim_data = sim_data.sort_index().round(2)
    data = []
    for year in sim_data.index.get_level_values(0).unique():
        data.append(
            (year, sim_data.loc[year, qrange[0]], sim_data.loc[year, qrange[1]])
        )
    return data

def validation_chart(session:Session):
    adminBase=session.adminBase
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
    my_chart.options.tooltip = {
        "header_format": '<span style="font-size: 12px; font-weight: bold">{point.key}</span><br/>',
        "point_format": '<span style="color:{point.color};font-size: 12px">\u25CF </span>' +\
            '<span style="font-size: 12px">{series.name}</span><br/>'
    }
    my_chart.options.legend = Legend(
        label_format='<span style="font-size: 12px">{name}</span><br/>'
    )
    tmp_df = adminBase.validation_run
    for n, qrange in enumerate(Q_RANGE_PLOTS):
        data = columnRange_data(tmp_df, qrange)
        column = ColumnRangeSeries().from_array(data)
        column.name = f"Simulated yield ({SERIES_CI[n]}% CI)"
        column.color = COLORS[n]
        column.grouping = False
        column.border_width = 0.
        my_chart.add_series(column)
    # Observed scatterplot
    data = tmp_df.groupby("year").obs.mean().round(3).reset_index().to_numpy()
    scatter = ScatterSeries().from_array(data)
    scatter.name = "Observed"
    scatter.color = "#ff3300"
    scatter.marker = {"symbol": "square", "radius": 6}

    my_chart.add_series(scatter)
    return my_chart

def init_anomalies_chart():
    """
    Creates and returns an anomaly chart
    """
    my_chart = Chart()
    my_chart.options = HighchartsOptions()
    my_chart.options.title = {
        'text': 'DSSAT simulated maize yield anomaly probability', 
        "style": {
            "font-size": "15px"
        }
    }
    my_chart.options.y_axis = {
        "title": {
            'text': 'Probability (%)', 
            "style": {
                "font-size": "15px"
            }
        },
        "labels": {
            "style": {
                "font-size": "15px",
            }
        },
        "max": 100
    }
    my_chart.options.x_axis = {
        "title": {
            'text': 'Experiment', 
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
    for name, color in zip(CAT_NAMES[::-1], CAT_COLORS[::-1]):
        box = ColumnSeries()
        box.name = name
        box.color = color
        box.data = []
        my_chart.add_series(box)
        box.stacking = 'normal'
        box.data_labels = {
                "enabled": True
            }
    my_chart.options.tooltip = {
        "header_format": '<span style="font-size: 12px; font-weight: bold">{point.key}</span><br/>',
        "point_format": '<span style="color:{point.color};font-size: 12px">\u25CF </span>' +\
            '<span style="font-size: 12px">{series.name}</span><br/>'
    }
    my_chart.options.legend = Legend(
        label_format='<span style="font-size: 12px">{name}</span><br/>'
    )
    return my_chart

# Limit for what is considered "Normal". 0.44 Splits equal groups (terciles)
Z_LIM = 0.44
Z_EXT_LIM = 2 # Limit for extreme values

def assign_categories(data):
    """
    Assign a yield category based on the anomaly compared to baseline
    """
    data = np.array(data)
    very_low = (data < -Z_EXT_LIM).mean()*100
    low = (data < -Z_LIM).mean()*100 - very_low
    very_high = (data > Z_EXT_LIM).mean()*100
    high = (data > Z_LIM).mean()*100 - very_high
    norm = 100 - low - high - very_high - very_low
    return list(map(int, [very_high, high, norm, low, very_low]))

def add_anomaly_bar(chart, session):
    """
    Add an anomaly bar to one existing anomaly chart
    """
    data = session.adminBase.get_quantile_anomalies(session.latest_run)
    cat_data = assign_categories(data)
    label = f"{session.adminBase.cultivar_labels[session.simPars.cultivar]}<br>" + \
        f"Planted on {session.simPars.planting_date.strftime('%b %d %Y')}<br>" + \
        f"{sum(session.simPars.nitrogen_rate):.0f} kg N/ha applied in {len(session.simPars.nitrogen_rate)} events"
    x = label
    series = chart.to_dict()["userOptions"]["series"]
    for n, cat in enumerate(cat_data):
        series_data = series[n].get("data", [])
        series_data += [(x, cat)]
        series[n]["data"] = series_data
    chart.update_series(*series)

def clear_anomaly_chart(chart):
    """"""
    series = chart.to_dict()["userOptions"]["series"]
    for n, serie in enumerate(series):
        series[n]["data"] = []
    chart.update_series(*series)

def init_stress_chart(stress_type):
    """
    Returns an stress chart. stress_type is water or nitrogen
    """
    assert stress_type in STRESS_COLUMNS
    my_chart = Chart()
    my_chart.options = HighchartsOptions()
    my_chart.options.title = {
        'text': f'{stress_type.title()} stress', 
        "style": {
            "font-size": "15px"
        }
    }
    my_chart.options.y_axis = {
        "title": {
            'text': f'Stress (%)', 
            "style": {
                "font-size": "15px"
            }
        },
        "labels": {
            "style": {
                "font-size": "15px",
            }
        },
        "max": 100
    }
    my_chart.options.x_axis = {
        "title": {
            'text': 'Crop Dev. Stage', 
            "style": {
                "font-size": "15px",
            }
        },
        "labels": {
            "style": {
                "font-size": "15px",
            },
            "auto_rotation_limit": 0,
            "allow_overlap": True
        },
        "categories": DEV_STAGES_LABELS
    }      
    return my_chart

def process_overview(overview):
    overview = parse_overview("".join(overview))
    overview = overview.set_index(["RUN", 'devPhase']).astype(float).reset_index()
    overview["watStress"] = overview[['stressWatPho', 'stressWatGro']].max(axis=1)
    overview["nitroStress"] = overview[['stressNitPhto', 'stressNitGro']].max(axis=1)
    return overview

def add_stress_bar(chart, session):
    "data is pandas.Series with the dev stage as index"
    overview = process_overview(session.latest_overview.copy())
    var_column = STRESS_COLUMNS[chart.options.title.text.split()[0].lower()]
    data = overview.groupby("devPhase")[var_column].mean()
    data = 100*data
    box = ColumnSeries()
    box.data = [data.to_dict().get(dev_st) for dev_st in DEV_STAGES]
    if chart.options.series is None:
        n = 0
    else:
        n = len(chart.options.series)
    box.name = f"Exp {n}"
    chart.add_series(box)
    
def clear_stress_chart(chart):
    chart.options.series = None
    
def init_columnRange_chart():
    my_chart = Chart()
    my_chart.options = HighchartsOptions()
    my_chart.options.title = {
        'text': 'DSSAT simulated maize yield', 
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
        },
    }
    my_chart.options.x_axis = {
        "title": {
            'text': 'Experiment', 
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
    my_chart.options.tooltip = {
        "header_format": '<span style="font-size: 12px; font-weight: bold">{point.key}</span><br/>',
        "point_format": '<span style="color:{point.color};font-size: 12px">\u25CF </span>' +\
            '<span style="font-size: 12px">{series.name}</span><br/>'
    }
    my_chart.options.legend = Legend(
        label_format='<span style="font-size: 12px">{name}</span><br/>'
    )
    for n, _ in enumerate(Q_RANGE_PLOTS):
        my_chart.add_series(
            ColumnRangeSeries(
                name=f"Simulated yield ({SERIES_CI[n]}% CI)", 
                color=COLORS[n], grouping=False, border_width=0.
            )
        )
    return my_chart

def add_yield_column(chart, session, series_idx=0, qrange=(.05, .95)):
    tmp_df = session.latest_run
    tmp_df['year'] = 1
    tmp_df["sim"] = tmp_df.HARWT.astype(float)/1000
    data = dict(zip(("low", "high"), columnRange_data(tmp_df, qrange)[0][1:]))

    label = f"{session.adminBase.cultivar_labels[session.simPars.cultivar]}<br>" + \
            f"Planted on {session.simPars.planting_date.strftime('%b %d %Y')}<br>" + \
            f"{sum(session.simPars.nitrogen_rate):.0f} kg N/ha applied in {len(session.simPars.nitrogen_rate)} events"

    data["name"] = label
    data["x"] = label

    series = chart.options.series[series_idx]
    if series.data is None:
        series.data = [data]
    else:
        series.data += [data]

def add_yield_ci_columns(chart, session):
    for n, qrange in enumerate(Q_RANGE_PLOTS):
        add_yield_column(chart, session, n, qrange)