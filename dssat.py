"""
This module contains 
"""
from spatialDSSAT.run import GSRun
from DSSATTools import Weather
import database as db

import numpy as np
import pandas as pd

from datetime import datetime, timedelta
from itertools import product
import tempfile
import os
import shutil
from tqdm import tqdm


MIN_SAMPLES = 4
MAX_SIM_LENGTH = 6*30 # This is maximum simulation lenght since planting. 

def run_spatial_dssat(dbname:str, schema:str, admin1:str, 
                      plantingdate:datetime, cultivar:str,
                      nitrogen:list[tuple], nens:int=50, 
                      all_random:bool=True):
    """
    Runs DSSAT in spatial mode for the defined country (schema) and admin
    subdivision (admin1). 

    Arguments
    ----------
    dbname: str
        Name of the database
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
    """
    # Simulation will start 30 days prior (As sugested by Ines et al., 2013)
    start_date = plantingdate - timedelta(days=30)
    end_date = plantingdate + timedelta(days=MAX_SIM_LENGTH)
    con = db.connect(dbname)
    # Get soils and verify a minimum number of pixel samples
    soils = db.get_soils(con, schema, admin1, 1)
    if len(soils) < MIN_SAMPLES:
       soils = db.get_soils(con, schema, admin1, 2)
       if len(soils) < MIN_SAMPLES:
          soils = db.get_soils(con, schema, admin1, None)
          assert len(soils) < MIN_SAMPLES, \
            f"Region is not large enough to have at least {MIN_SAMPLES} samples"

    pixels = soils.apply(lambda row: (row.lon, row.lat), axis=1)
    n_pixels = len(pixels)
    if all_random:
        # In case all pixel combinations are posible
        if n_pixels < np.sqrt(nens):
            pix_prod = list(product(pixels, pixels))
            soil_pixels = [p[0] for p in pix_prod]
            weather_pixels = [p[1] for p in pix_prod]
        else:
            soil_pixels = pixels.sample(nens, replace=n_pixels<nens)
            weather_pixels = pixels.sample(nens, replace=n_pixels<nens)
    else:
        nens = min(nens, n_pixels)
        soil_pixels = weather_pixels = pixels.sample(nens, replace=False)

    # tmpdir to save wth files
    tmp_dir = tempfile.TemporaryDirectory()
    # Add treatments
    gs = GSRun()

    # Check if TAVG and TAMP are in static table
    tav_exists = db.verify_static_par_exists(dbname, schema, "tav")
    tamp_exists = db.verify_static_par_exists(dbname, schema, "tamp")
    
    for (n, (soil, weather)) in tqdm(list(enumerate(zip(soil_pixels, weather_pixels)))):
        soil_profile = soils.loc[
            (soils.lon==soil[0]) & (soils.lat==soil[1]),
            "soil" 
        ].values[0]
        weather_df = db.get_era5_for_point(
            con, schema, weather[0], weather[1], 
            start_date, end_date
        )
        if tav_exists and tamp_exists:
            tav = db.get_static_par(con, schema, weather[0], weather[1], "tav")
            tamp = db.get_static_par(con, schema, weather[0], weather[1], "tamp")
        else:
            tav = None
            tamp = None

        if weather_df is None:
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

        dssat_weather.write(tmp_dir.name)

        gs.add_treatment(
            soil_profile=soil_profile,
            weather=os.path.join(tmp_dir.name, f"{dssat_weather._name}.WTH"),
            nitrogen=nitrogen,
            planting=plantingdate,
            cultivar=cultivar
        )
    # Run DSSAT
    con.close()
    
    out = gs.run(start_date=start_date)
    tmp_dir.cleanup()
    print("")
    return out
        

    

    
