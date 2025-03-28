"""
This module contains the function to run the model given a set of inputs.
"""
from spatialDSSAT.run import GSRun
from DSSATTools import Weather
import dssatservice.database as db

import numpy as np
import pandas as pd

from datetime import datetime, timedelta
from itertools import product
import tempfile
import os
import shutil
from tqdm import tqdm
import logging
import psycopg2 as pg

# For SRAD estimation when using NMME data
from sklearn.neighbors import KNeighborsRegressor


MIN_SAMPLES = 4
# MAX_SIM_LENGTH = 8*30  
MAX_SIM_LENGTH = 270 # This is maximum simulation lenght since planting.
logger = logging.getLogger(__name__)

HARM_VARS = ["constant", "cos1", "sin1", "cos2", "sin2"]
def add_harmonic_coefs(tmp_df):
    """
    Add hamonic coefficients to a weather timeseries. This is needed to estimate
    solar radiation in NMME forecast data.
    """
    tmp_df["t"] = np.array([
        int(i.strftime('%j')) 
        for i in tmp_df.index
    ])/365
    tmp_df["constant"] = 1
    tmp_df["cos1"] = np.cos(2*np.pi*tmp_df["t"])
    tmp_df["sin1"] = np.sin(2*np.pi*tmp_df["t"])
    tmp_df["cos2"] = np.cos(np.pi*tmp_df["t"])
    tmp_df["sin2"] = np.sin(np.pi*tmp_df["t"])

def run_spatial_dssat(con:pg.extensions.connection, schema:str, admin1:str, 
                      plantingdate:datetime, cultivar:str,
                      nitrogen:list[tuple], nens:int=50, 
                      all_random:bool=True, overview:bool=False,
                      return_input=False, weather_table:str='era5',
                      **kwargs):
    """
    Runs DSSAT in spatial mode for the defined country (schema) and admin
    subdivision (admin1). 

    Parameters
    ----------
    con: pg.extensions.connection
        pg connection
    schema: str
        Name of the schema (country)
    admin1: str
        Name of the administrative subdivision in that country
    nitrogen: list of tuples [(int, float), ...]
            A list of tuples where each tuple is one Nitrogen application. The 
            tuple contains two values, where the first one indicates the application
            time (days after planting) and the second one the nitrogen rate (kg/ha).
    planting: datetime
        Planting date
    cultivar: str
        Cultivar code. It must be available on the DSSAT list of cultivars.
    nens: int
        Number of random samples within the region
    all_random: bool
        If false soil and weather are from the same pixel. If true soil and
        weather pixels are shuffled and randomly selected.
    overview: bool
        If true it will return the overview file string
    return_input: bool
        If True it will return a list with the path to the .SOL and .WTH and the
        model won't run. This is useful for calibration, as the inputs wound be 
        queried from the db only once.
    connection: 
        a psycopg2 connection object. It will be used instead of setting new
        conection to dbname
    weather_table: str
        Weather table to get the data from. Default is era5
    kwargs: 
        kwargs to pass to the GSRun.run function
    """
    # Simulation will start 30 days prior (As sugested by Ines et al., 2013)
    # start_date = plantingdate
    start_date = plantingdate - timedelta(days=30)
    end_date = plantingdate + timedelta(days=MAX_SIM_LENGTH)
    db.check_admin1_in_country(con, schema, admin1)
    # Get soils and verify a minimum number of pixel samples
    soils = db.get_soils(con, schema, admin1, 1)
    # Assign weather retrieval function
    if weather_table == 'era5':
        get_weather_for_point = db.get_era5_for_point
    elif weather_table == 'prism':
        get_weather_for_point = db.get_prism_for_point
    else:
        raise NameError(f'{weather_table} tables not in database')
    # Get weather pixels
    query = """
        WITH pts As ( 
        SELECT (ST_PixelAsCentroids(ST_Clip(wt.rast, ad.geom))).geom as geom
        FROM {0}.{1}_rain as wt, {0}.admin as ad
        WHERE 
            fdate=%s
        AND ad.admin1=%s
        )  
        SELECT ST_X(geom), ST_Y(geom) FROM pts;
        """.format(schema, weather_table)
    cur = con.cursor()
    cur.execute(query, (start_date, admin1,))
    rows = cur.fetchall()
    cur.close()
    all_pixels_weather = pd.Series([(i[0], i[1]) for i in rows])
    
    if len(soils) < MIN_SAMPLES:
       soils = db.get_soils(con, schema, admin1, 2)
       if len(soils) < MIN_SAMPLES:
          soils = db.get_soils(con, schema, admin1, None)
          assert len(soils) > MIN_SAMPLES, \
            f"Region is not large enough to have at least {MIN_SAMPLES} samples"

    all_pixels_soil = soils.apply(lambda row: (row.lon, row.lat), axis=1)
    n_pixels = min(len(all_pixels_soil), len(all_pixels_weather))
    if all_random:
        # In case all pixel combinations are posible
        if n_pixels < np.sqrt(nens):
            pix_prod = list(product(all_pixels_soil, all_pixels_weather))
            soil_pixels = [p[0] for p in pix_prod]
            weather_pixels = [p[1] for p in pix_prod]
        else:
            soil_pixels = all_pixels_soil.sample(nens, replace=n_pixels<nens)
            weather_pixels = all_pixels_weather.sample(nens, replace=n_pixels<nens)
    else:
        nens = min(nens, n_pixels)
        soil_pixels = weather_pixels = soil_pixels.sample(nens, replace=False)

    # tmpdir to save wth files
    if return_input:
        tmp_dir_name = tempfile.mkdtemp()
        input_files =[]
    else:
        tmp_dir = tempfile.TemporaryDirectory()
        tmp_dir_name = tmp_dir.name
    # Add treatments
    gs = GSRun()        

    # Check if TAVG and TAMP are in static table
    tav_exists = db.verify_static_par_exists(con, schema, "tav")
    tamp_exists = db.verify_static_par_exists(con, schema, "tamp")
    
    iter_pixels = list(enumerate(zip(soil_pixels, weather_pixels)))
    for (n, (soil, weather)) in tqdm(iter_pixels):
        soil_profile = soils.loc[
            (soils.lon==soil[0]) & (soils.lat==soil[1]),
            "soil" 
        ].values[0]
        
        # Get weather
        # Verify that all the series are available from past weather
        latest_past_weather = db.latest_date(con, schema, f"{weather_table}_rain")
        if latest_past_weather >= end_date: # End of season
            weather_df = get_weather_for_point(
                con, schema, weather[0], weather[1], 
                start_date, end_date
            )
            if weather_df is None:
                continue
        else: # Forecast
            latest_forecast_weather = db.latest_date(con, schema, "nmme_rain")
            end_date = latest_forecast_weather
            # Get latest year of past weather. That year is used to train a 
            # KNN estimator for srad
            start_past_forecast = min(
                latest_past_weather - timedelta(365), start_date
            )
            past_weather_df = get_weather_for_point(
                con, schema, weather[0], weather[1], 
                start_past_forecast, latest_past_weather
            )
            if past_weather_df is None:
                continue
            ens = np.random.randint(1, 11)
            future_weather_df = db.get_nmme_for_point(
                con, schema, weather[0], weather[1], 
                latest_past_weather, end_date, ens
            )    
            if future_weather_df is None:
                continue        
            # Estimate forecast srad
            # Adjust a harmonic model to past srad
            add_harmonic_coefs(past_weather_df)          
            past_weather_df["srad_rolling"] = \
                past_weather_df.srad.rolling(10).mean()
            A = past_weather_df.dropna()[HARM_VARS].to_numpy()
            b = past_weather_df.dropna()["srad_rolling"].to_numpy()
            coefs, _, _, _ = np.linalg.lstsq(A, b)
            # Estimate the srad difference when compared to harmonic
            past_weather_df["srad_harm"] = (
                past_weather_df[HARM_VARS].to_numpy() @ coefs
            ).flatten()
            past_weather_df["srad_dif"] = \
                past_weather_df.srad - past_weather_df.srad_harm
            
            # Adjust a KNN regressor to srad_diff using cos1 and rain
            knn_reg = KNeighborsRegressor()
            x = past_weather_df[["cos1", "rain"]].to_numpy()
            y = past_weather_df.srad_dif.to_numpy()
            knn_reg = knn_reg.fit(x, y)
            add_harmonic_coefs(future_weather_df)
            future_weather_df["srad_harm"] = (
                future_weather_df[HARM_VARS].to_numpy() @ coefs
            ).flatten()
            future_weather_df["srad_dif"] = knn_reg.predict(
                future_weather_df[["cos1", "rain"]].to_numpy()
            )
            future_weather_df["srad"] = \
                future_weather_df.srad_harm + future_weather_df.srad_dif
            
            past_weather_df = past_weather_df[
                ["tmax", 'tmin', 'rain', 'srad']
            ]
            future_weather_df = future_weather_df[
                ["tmax", 'tmin', 'rain', 'srad']
            ]
            
            # Fill whatever is missed by repeating past_weather
            post_forecast_df = past_weather_df.copy()
            post_forecast_df.index = post_forecast_df.index + timedelta(365)
            post_forecast_df = post_forecast_df.loc[
                ~post_forecast_df.index.isin(future_weather_df.index)
            ]
            
            # Concat dfs
            weather_df = pd.concat([
                past_weather_df, future_weather_df, post_forecast_df
            ])
            weather_df = weather_df.sort_index()
            weather_df = weather_df.loc[
                pd.to_datetime(weather_df.index) >= start_date
            ]
            
            
        if tav_exists and tamp_exists:
            tav = db.get_static_par(con, schema, weather[0], weather[1], "tav")
            tamp = db.get_static_par(con, schema, weather[0], weather[1], "tamp")
        else:
            tav = None
            tamp = None

        if (weather_df is None) or (len(weather_df) < 1):
            # In the unlikely case that there is no data for that location.
            # This can occur in soil pixels that are near coasts or very close
            # to the domain's boundary
            continue
        weather_df["tmax"] -= 273.15
        weather_df["tmin"] -= 273.15
        weather_df["srad"] /= 1e6
        weather_df["rain"] = weather_df.rain.abs()

        # weather_df = weather_df.sort_index()
        weather_df.index = pd.to_datetime(weather_df.index)
        pars = {i: i.upper() for i in weather_df.columns}
        # Weather class checks data consistency. If some inconsistency is found 
        # (for example Tmax < Tmin) it will raise an error. It is not unusual to
        # to find small inconsistencies in global datasets.  Then, in case that 
        # there is an inconsitency, that pixel will be skiped.
        try:
            dssat_weather = Weather(
                weather_df, pars, weather[1], weather[0],
                tav=tav, amp=tamp
                )
        except AssertionError:
            continue
        dssat_weather._name = f"WS{n:02}{dssat_weather._name[4:]}"

        dssat_weather.write(tmp_dir_name)

        # Planting 
        planting = {
            "PDATE": plantingdate, 
            "PLDP": 5
        }
        if return_input:
            input_files.append((
                (weather, os.path.join(tmp_dir_name, f"{dssat_weather._name}.WTH")),
                (soil, soil_profile)
            ))
            continue
        gs.add_treatment(
            soil_profile=soil_profile,
            weather=os.path.join(tmp_dir_name, f"{dssat_weather._name}.WTH"),
            nitrogen=nitrogen,
            planting=planting,
            cultivar=cultivar
        )
    if return_input:
        return input_files
    # Run DSSAT
    # Set automatic management
    # planting_window_start = plantingdate - timedelta(days=15)
    # planting_window_end = plantingdate + timedelta(days=15)
    # sim_controls = {
    #     "PLANT": "F", # Automatic, force in last day of window
    #     "PFRST": planting_window_start.strftime("%y%j"),
    #     "PLAST": planting_window_end.strftime("%y%j"),
    #     "PH2OL": 50, "PH2OU": 100, "PH2OD": 20, 
    #     "PSTMX": 40, "PSTMN": 10
    # }
    # Get run kwargs if defined
    sim_controls = {}
    start_date = kwargs.get("start_date", start_date)
    sim_controls = kwargs.get("sim_controls", sim_controls)
    out = gs.run(
        start_date=start_date,
        sim_controls=sim_controls
    )
    tmp_dir.cleanup()
    if (out.MAT == "-99").mean() > .5:
        logger.warning(
            "Most of the simulations were terminated before reaching maturity. "
            "It is likely that the available weather data was not long enough "
            "to complete the simulation."
        )
    # print("")
    if overview:
        return out, gs.overview
    return out
        

    

    
