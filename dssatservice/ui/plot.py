"""
Functions to create highchart plots.
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

from matplotlib import colormaps
from matplotlib.colors import to_hex
from datetime import timedelta

from .base import AdminBase, Session, QUANTILES_TO_COMPARE
from dssatservice.data.transform import parse_overview

import numpy as np
from scipy import stats

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
DEV_STAGES_FULL_LABELS = [
    "Emergence to end<br>of Juvenile stage", 
    "End of juvenile<br>to floral initiation",
    "Floral initiation to<br>end of leaf growth", 
    "End of leaf grow to<br>begin of grain filling",
    "Grain filling"
]
STRESS_COLUMNS = {
    "water": "watStress",
    "nitrogen":  "nitroStress"
}
CAT_NAMES = ["Very low", "Low", "Normal", "High", "Very high"]
CAT_COLORS = ['#cc0000', "#ff9933", "#ffff66", "#99cc00", "#009933"]
ADMIN_NAMES = {"kenya": "County", "zimbabwe": "District"}

def columnRange_data(df, qrange=(.05, .95)):
    """
    NOT IMPLEMENTED, it was part of former stages of the service.
    """
    sim_data = df.groupby("year").sim.quantile(qrange)
    sim_data = sim_data.sort_index().round(2)
    data = []
    for year in sim_data.index.get_level_values(0).unique():
        data.append(
            (year, sim_data.loc[year, qrange[0]], sim_data.loc[year, qrange[1]])
        )
    return data

def get_bin_counts(series):
    """
    NOT IMPLEMENTED, it was part of former stages of the service.
    """
    MAX_BINS = 10
    vals = series.sort_values().to_numpy()
    vals = vals + np.random.normal(0, .001, 50)
    min_step = (vals.max() - vals.min())/MAX_BINS
    # min_step = vals.mean()*.2
    bins = [vals[0]]
    counts = [0]
    for n, val in enumerate(vals[:-1]):
        bin_limit = val + .5*(vals[n+1]-val)
        counts[-1] += 1
        if (val - bins[-1]) >= min_step: 
            if counts[-1] < 3:
                continue
            else:
                bins.append(bin_limit)
                counts.append(0)
    counts[-1] += 1
    bins.append(vals[-1])
    return np.array(counts), np.array(bins)

def validation_chart(session:Session):
    """
    NOT IMPLEMENTED, it was part of former stages of the service.
    """
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
            'text': 'Yield (t/ha)', 
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
    my_chart.options.legend = Legend(
        label_format='<span style="font-size: 12px">{name}</span><br/>'
    )
    tmp_df = adminBase.validation_run
    for year in tmp_df.year.unique():
        # counts, bins = get_bin_counts(tmp_df.loc[tmp_df.year==year].sim)
        counts, bins = np.histogram(tmp_df.loc[tmp_df.year==year].sim, bins=5)
        counts = counts/sum(counts)
        
        column = ColumnRangeSeries().from_dict({
            "data":[
                {
                    'high': round(bins[n+1], 2), 'low': round(bins[n], 2), 'x': int(year), 
                    "color": to_hex(colormaps["Greens"](count/.35)),
                    "count": count, 
                    "name": f'{bins[n]:.2f}-{bins[n+1]:.2f} t/ha',
                    "custom": {
                        "extraInformation": f'{100*count/sum(counts):.0f}% probability'
                    }
                }
                for n, count in enumerate(counts)
            ],
            "borderWidth": 0,
            "showInLegend": False
        })
        column.grouping = False
        column.tooltip = {
            "header_format": '<span style="font-size: 12px; font-weight: bold">{point.key}</span><br/>',

        "pointFormat": '<span style="font-size: 12px">{point.custom.extraInformation}</span><br/>'
    }
        my_chart.add_series(column)
    # Observed scatterplot
    data = tmp_df.groupby("year").obs.mean().round(3).reset_index().to_numpy()
    scatter = ScatterSeries().from_dict({
        "data": [
            {"x": x, "y": y} for x, y in data
        ]
    })
    scatter.name = "Observed"
    scatter.color = "#FFFFFF"
    scatter.marker = {
        "symbol": "square", "radius": 6, "line_color":"#000000",
        "line_width": 2, 
    }
    scatter.tooltip = {
        "header_format": '<span style="font-size: 12px; font-weight: bold">{point.key}</span><br/>',
        "point_format": '<span style="color:{point.color};font-size: 12px">\u25CF </span>' +\
            '<span style="font-size: 12px">{series.name}: {point.y} kg/ha</span><br/>'
    }
    my_chart.add_series(scatter)
    my_chart.options.x_axis.min = tmp_df.year.min() - .5
    my_chart.options.x_axis.max = tmp_df.year.max() + .5
    return my_chart.to_dict()

def init_anomalies_chart():
    """
    NOT IMPLEMENTED, it was part of former stages of the service.
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
    return my_chart.to_dict()

# Limit for what is considered "Normal". 0.44 Splits equal groups (terciles)
Z_LIM = 0.44
Z_EXT_LIM = 2 # Limit for extreme values

def assign_categories(data):
    """
    NOT IMPLEMENTED, it was part of former stages of the service.
    """
    data = np.array(data)
    very_low = (data < -Z_EXT_LIM).mean()*100
    low = (data < -Z_LIM).mean()*100 - very_low
    very_high = (data > Z_EXT_LIM).mean()*100
    high = (data > Z_LIM).mean()*100 - very_high
    norm = 100 - low - high - very_high - very_low
    
    cats = {
        "Very low": int(very_low), "Low": int(low), "Normal": int(norm),
        "High": int(high), "Very high": int(very_high)
    }
    return cats
    # return list(map(int, [very_high, high, norm, low, very_low]))

def get_anomaly_series_data(session, model_based=True):
    """
    NOT IMPLEMENTED, it was part of former stages of the service.
    """
    # This is for anomaly based in model
    if model_based:
        data = session.adminBase.get_quantile_anomalies(session.latest_run)
    # This is for anomaly based in observation
    else:
        yld = session.latest_run.HARWT.astype(float)/1000
        # mu based on observations
        mu = session.adminBase.validation_run.groupby("year").obs.mean().mean()
        # std_intra represents the intra-region variability, which is not know from
        # observations, then it is assumed from simulations
        std_intra = session.adminBase.validation_run.sim.std()
        mu_log = np.log(mu) - .5*std_intra**2
        std_intra_log = np.sqrt(np.log((std_intra**2 + mu**2)/mu**2))
        # std is the season-to-season variability, known from observations
        std = session.adminBase.validation_run.groupby("year").obs.mean().std()
        # observed intra-region quantiles are estimated
        mu_list = np.array([
            stats.lognorm(std_intra_log, scale=np.exp(mu_log)).isf(1-q) 
            for q in QUANTILES_TO_COMPARE
        ])
        data = (yld.quantile(QUANTILES_TO_COMPARE) - mu_list)/std
    
    cat_data = assign_categories(data)
    label = f"{session.adminBase.cultivar_labels[session.simPars.cultivar]}<br>" + \
        f"Planted on {session.simPars.planting_date.strftime('%b %d %Y')}<br>" + \
        f"{sum(session.simPars.nitrogen_rate):.0f} kg N/ha applied in {len(session.simPars.nitrogen_rate)} events"
    new_data = {}
    for key, val in cat_data.items():
        new_data[key] = (label, val)
    return new_data

def clear_yield_chart(chart_dict):
    """
    NOT IMPLEMENTED, it was part of former stages of the service.
    """
    for serie in chart_dict["userOptions"]["series"]:
        serie["data"] = []

def init_stress_chart(stress_type):
    """
    Initilizes the stress chart for the stress type passed. Stress type can be 
    either water or nitrogen
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
    my_chart.options.tooltip = {
        "header_format": '<span style="font-size: 12px; font-weight: bold">{point.key}</span><br/>',
        "point_format": '<span style="color:{point.color};font-size: 12px">\u25CF </span>' +\
            '<span style="font-size: 12px">{series.name}: {point.y:.0f} %</span><br/>'
    }  
    my_chart = my_chart.to_dict()
    my_chart["userOptions"]["series"] = []
    return my_chart

def process_overview(overview):
    """
    Process the lines of the DSSAT overview file.
    """
    overview = parse_overview("".join(overview))
    overview = overview.set_index(["RUN", 'devPhase']).astype(float).reset_index()
    overview["watStress"] = overview[['stressWatPho', 'stressWatGro']].max(axis=1)
    overview["nitroStress"] = overview[['stressNitPhto', 'stressNitGro']].max(axis=1)
    return overview

def get_stress_series_data(session, stresstype):
    """
    Returns a bar to be added to the stressplot for the stress type specified.
    Stress type can be either water or nitrogen. The bar is made considering the 
    latest simulation run for the session.
    """
    overview = process_overview(session.latest_overview.copy())
    var_column = STRESS_COLUMNS[stresstype]
    data = overview.groupby("devPhase")[var_column].mean()
    data = 100*data
    box = ColumnSeries()
    box.data = [data.to_dict().get(dev_st) for dev_st in DEV_STAGES]
    return box.to_dict()
    
def clear_stress_chart(chart_dict):
    """
    NOT IMPLEMENTED, it was part of former stages of the service.
    """
    chart_dict["userOptions"]["series"] = []
    
def init_columnRange_chart(session):
    """
    Initializes the yield columnRange chart for the requested session.
    """
    my_chart = Chart()
    my_chart.options = HighchartsOptions()
    my_chart.options.title = {
        'text': 'DSSAT simulated maize yield', 
        "style": {
            "font-size": "15px"
        }
    }
    if any(np.isnan(session.adminBase.obs_reference)):
        my_chart.options.y_axis = {
            "title": {
                'text': 'Yield (t/ha)', 
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
    else:
        my_chart.options.y_axis = {
            "title": {
                'text': 'Yield (t/ha)', 
                "style": {
                    "font-size": "15px",
                }
            },
            "labels": {
                "style": {
                    "font-size": "15px",
                }
            },
            "plot_lines": [
                {
                    "value": session.adminBase.obs_reference[1]/1000,
                    "color": "#32323232",
                    "width": 8,
                    "dash_style": "Solid",
                    "z_index": 99,
                    "label": {
                        "text": f"<b>{ADMIN_NAMES.get(session.adminBase.schema)}<br/>average</b>",
                        "align": "left",
                        "style": {"color": "black", "font-size": 13}
                    }
                },
                {
                    "value": session.adminBase.obs_reference[0]/1000,
                    "color": "#32323232",
                    "width": 3,
                    "dash_style": "Dash",
                    "z_index": 99,
                    "label": {
                        "text": f"<b>{ADMIN_NAMES.get(session.adminBase.schema)}<br/>min</b>",
                        "align": "left",
                        "style": {"color": "black", "font-size": 13}
                    }
                },
                {
                    "value": session.adminBase.obs_reference[2]/1000,
                    "color": "#32323232",
                    "width": 3,
                    "dash_style": "Dash",
                    "z_index": 99,
                    "label": {
                        "text": f"<b>{ADMIN_NAMES.get(session.adminBase.schema)}<br/>max</b>",
                        "align": "left",
                        "style": {"color": "black", "font-size": 13}
                    }
                }
            ]
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
        },
        "type": "Category",
        
    }
    my_chart = my_chart.to_dict()
    my_chart["userOptions"]["series"] = []
    return my_chart

def get_columnRange_series_data(session, series_len):
    """
    Get the series data for the columnRange chart using the results from the 
    latest simulation. series_len is the current number of series in the chart
    """
    tmp_df = session.latest_run
    tmp_df['year'] = 1
    tmp_df["sim"] = tmp_df.HARWT.astype(float)/1000
    
    counts, bins = np.histogram(tmp_df.sim, bins=5)
    counts = counts/sum(counts)
        
    harvest_date_min = session.simPars.planting_date + \
        timedelta(days=session.latest_run.MAT.astype(int).quantile(.25))
    harvest_date_max = session.simPars.planting_date + \
        timedelta(days=session.latest_run.MAT.astype(int).quantile(.75))
    harvest_range = f"{harvest_date_min.strftime('%b %d %Y')} - {harvest_date_max.strftime('%b %d %Y')}"
    
    tmp_df = session.adminBase.cultivars
    cul = tmp_df.loc[tmp_df.cultivar == session.simPars.cultivar].index[0]
    label = '<span style="font-size: 12px; font-weight: bold">' + \
            f"{cul} season<br>" + \
            f"Planted on {session.simPars.planting_date.strftime('%b %d %Y')}<br>" + \
            f"Harvest on {harvest_range}<br>" + \
            f"{sum(session.simPars.nitrogen_rate):.0f} kg N/ha applied in {len(session.simPars.nitrogen_rate)} events<br>"
    if session.simPars.irrigation:
        label += f"Irrigated</span><br>Harvest on {harvest_range}"
        avg_irr = int(session.latest_run.TIRR.astype(int).mean())
        label += f"<br>An average of {avg_irr} mm of irrigation needed"
    else: 
        label += f"Rainfed</span><br>Harvest on {harvest_range}"
        
    column = ColumnRangeSeries().from_dict({
        "data":[
            {
                'high': round(bins[n+1], 2), 'low': round(bins[n], 2), 
                "x": series_len + 1,
                "color": to_hex(colormaps["Greens"](count/.35)),
                "count": count, 
                "custom": {
                    "extraInformation": f'{label}<br>{bins[n]:.2f}-{bins[n+1]:.2f} t/ha<br>' + \
                        f'({100*count/sum(counts):.0f}% probability)',
                   
                }
            }
            for n, count in enumerate(counts)
        ],
        "borderWidth": 0,
        "showInLegend": False,
        "pointInterval": 1,
    })
    
    column.grouping = False
    column.tooltip = {
        "header_format": '<span style="font-size: 12px; font-weight: bold">{point.extra}</span><br/>',
        "pointFormat": '<span style="font-size: 12px">{point.custom.extraInformation}</span><br/>'
    }
    return column


def current_forecast_yield_plot(session):
    """
    Returns the columnRange yield plot for the latest forecast.
    """
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
            'text': 'Yield (t/ha)', 
            "style": {
                "font-size": "15px",
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
            'text': '', 
            "style": {
                "font-size": "0px",
            }
        },
        "labels": {
            "style": {
                "font-size": "0px",
            }
        },
        "type": "Category",

    }
    my_chart = my_chart.to_dict()
    my_chart["userOptions"]["series"] = []
    
    tmp_df = session.adminBase.forecast_results
    tmp_df = tmp_df.loc[tmp_df.MAT != -99]
    tmp_df["HARWT"] = tmp_df.HARWT/1000
    
    counts, bins = np.histogram(tmp_df.HARWT, bins=5)
    counts = counts/sum(counts)

    column = ColumnRangeSeries().from_dict({
        "data":[
            {
                'high': round(bins[n+1], 2), 'low': round(bins[n], 2), 
                "x": 0,
                "color": to_hex(colormaps["Greens"](count/.35)),
                "count": count, 
                "custom": {
                    "extraInformation": f'<b>{session.adminBase.admin1}</b><br>' +\
                        f'{bins[n]:.2f}-{bins[n+1]:.2f} t/ha<br>' + \
                        f'({100*count/sum(counts):.0f}% probability)',
                }
            }
            for n, count in enumerate(counts)
        ],
        "borderWidth": 0,
        "showInLegend": False,
        "pointInterval": 1,
    })
    
    column.grouping = False
    column.tooltip = {
        "header_format": '<span style="font-size: 12px; font-weight: bold">{point.extra}</span><br/>',
        "pointFormat": '<span style="font-size: 12px">{point.custom.extraInformation}</span><br/>'
    }
    
    scatter = ScatterSeries().from_dict({
        "data": [
            {"x": 0, "y": tmp_df.HARWT.mean().round(2)}
        ],
        "showInLegend": False,
        "color": "#FFFFFF",
        "marker": {
            "symbol": "square", "radius": 6, "line_color":"#000000",
            "line_width": 2, 
        },
        "tooltip": {
            "header_format": '<span style="font-size: 12px; font-weight: bold">Siimulation average</span><br/>',
            "point_format": '<span style="font-size: 12px"> {point.y} t/ha</span><br/>'
        }
    })
    
    my_chart["userOptions"]["series"] = [column.to_dict(), scatter.to_dict()]
    
    if not any(np.isnan(session.adminBase.obs_reference)):
        min_value = session.adminBase.obs_reference[0]/1000
        # min_year = session.adminBase.historical_data.set_index("year").value.idxmin()
        min_point = ScatterSeries().from_dict({
            "data": [
                {"x": 0, "y": round(min_value, 2)}
            ],
            "showInLegend": False,
            "color": "#000000",
            "marker": {
                "symbol": "triangle", "radius": 6, "line_color":"#000000",
                "line_width": 0, 
            },
            "tooltip": {
                "header_format": f'<span style="font-size: 12px; font-weight: bold">Reference period minimum</span><br>',
                "point_format": '<span style="font-size: 12px"> {point.y} t/ha</span><br/>'
            }
        })
        
        max_value = session.adminBase.obs_reference[2]/1000
        # max_year = session.adminBase.historical_data.set_index("year").value.idxmax()
        max_point = ScatterSeries().from_dict({
            "data": [
                {"x": 0, "y": round(max_value, 2)}
            ],
            "showInLegend": False,
            "color": "#000000",
            "marker": {
                "symbol": "triangle-down", "radius": 6, "line_color":"#000000",
                "line_width": 0, 
            },
            "tooltip": {
                "header_format": f'<span style="font-size: 12px; font-weight: bold">Reference period maximum</span><br>',
                "point_format": '<span style="font-size: 12px"> {point.y} t/ha</span><br/>'
            }
        })
    
        my_chart["userOptions"]["series"] += [
            min_point.to_dict(), max_point.to_dict()
        ]
        
    return my_chart

def current_forecast_stress_plot(session):
    """
    Returns the stress plot for the latest forecast
    """
    my_chart = Chart()
    my_chart.options = HighchartsOptions()
    my_chart.options.title = {
        'text': f'Stress indexes', 
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
            'text': 'Crop Development Stage', 
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
        "categories": DEV_STAGES_LABELS,
    }    
    my_chart.options.tooltip = {
        "header_format": '<span style="font-size: 12px; font-weight: bold">{point.key}</span><br/>',
        "point_format": '<span style="color:{point.color};font-size: 12px">\u25CF </span>' +\
            '<span style="font-size: 12px">{series.name}: {point.y:.0f} %</span><br/>' 
    }  
    my_chart = my_chart.to_dict()

    tmp_df = session.adminBase.forecast_overview
    tmp_df["watStress"] = tmp_df[["stressWatPho", "stressWatGro"]].sum(axis=1)
    data = session.adminBase.forecast_overview.groupby("devPhase")["watStress"].mean()
    data = 100*data
    box = ColumnSeries().from_dict({
        "data": [data.to_dict().get(dev_st) for dev_st in DEV_STAGES],
        "name": "Water Stress",
        "color": "blue",
})

    my_chart["userOptions"]["series"] = [box.to_dict()] 
    
    tmp_df["nitStress"] = tmp_df[["stressNitPhto", "stressNitGro"]].sum(axis=1)
    data = session.adminBase.forecast_overview.groupby("devPhase")["nitStress"].mean()
    data = 100*data
    box = ColumnSeries().from_dict({
        "data": [data.to_dict().get(dev_st) for dev_st in DEV_STAGES],
        "name": "Nitrogen Stress",
        "color": "brown"
    })
    my_chart["userOptions"]["series"].append(box.to_dict())
    return my_chart