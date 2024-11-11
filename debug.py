"""
This file is to debug while developing, but also serves as a demonstration
as how the dssatservice package is used to setup and operate the service.

Some functions are examples of basic setup funcionalities like: adding a new 
domain, adding the soil data, adding the cultivars information, etc.
"""
import dssatservice.data.download as dwn
import dssatservice.data.ingest as ing
import dssatservice.database as db
from dssatservice.ui.base import (
    admin_list, AdminBase, Session
)
import dssatservice.data.transform as tr
from dssatservice.dssat import run_spatial_dssat
from datetime import datetime
import psycopg2 as pg

import os
import time
import numpy as np
import re
import pandas as pd

variable = "TMAX"
date = datetime(2024, 1, 2)

dbname = "dssatserv"

def add_country():
    shp = "/home/dquintero/dssat_service/kenya_mngCalibration/data/shapes/ken_admbnda_adm1_iebc_20191031.shp"
    db.add_country(dbname, "Kenya", shp, "ADM1_EN")

def ingest_era5_data():
    con = pg.connect(dbname=dbname)
    ing.ingest_era5_record(con, "kenya", date)
    ing.ingest_era5_series(
        dbname, "kenya", 
        datetime(2010, 1, 1), 
        datetime(2010, 1, 31)
    )
    con.close()

def ingest_soil_data():
    con = pg.connect(dbname=dbname)
    soil_path = "/home/dquintero/dssat_service/data/soil_data/iSDASoil/KE.SOL"
    mask1 = "/home/dquintero/dssat_service/data/subsaharanAfrica-maize.tif"
    mask2 = "/home/dquintero/dssat_service/data/subsaharanAfrica-suitableAg-v2.tif"
    # _create_soil_table(dbname, "kenya")
    ing.ingest_soil(
        con=con,
        schema="kenya",
        soilfile=soil_path,
        mask1=mask1,
        mask2=mask2
    )
    con.close()

def run_model():
    time0 = time.time()
    con = pg.connect(dbname=dbname)
    df, overview = run_spatial_dssat(
        con=con, 
        schema="kenya", 
        admin1="Nakuru",
        plantingdate=datetime(2022, 2, 1),
        cultivar="990002",
        nitrogen=[(5, 20), (30, 10), (50, 10)],
        overview=True
    )
    df = df.iloc[:, 3:].astype(int).replace(-99, np.nan)
    print(df.describe())
    from collections import Counter
    print(Counter([l[:7] for l in overview if "Sowing" in l]))
    N_uptake = [
        int(re.findall("(\d+)", l)[0]) 
        for l in overview 
        if "N uptake during growing season" in l
    ]
    print(np.mean(N_uptake), np.std(N_uptake))
    print(f"{(time.time() - time0):.3f} seconds running one season")
    con.close()

def run_model_forecast_onthefly():
    """
    This is just running the model as one user would do it by getting NMME data.
    It is not the function to run the operative forecast!!!
    """
    con = pg.connect(dbname=dbname)
    time0 = time.time()
    df, overview = run_spatial_dssat(
        con=con, 
        schema="kenya", 
        admin1="Bomet",
        plantingdate=datetime(2024, 3, 1),
        cultivar="990002",
        nitrogen=[(5, 20), (30, 10), (50, 10)],
        overview=True
    )
    df = df.iloc[:, 3:].astype(int).replace(-99, np.nan)
    print(df.describe())
    # parse_overview("".join(overview))
    from collections import Counter
    print(Counter([l[:7] for l in overview if "Sowing" in l]))
    N_uptake = [
        int(re.findall("(\d+)", l)[0]) 
        for l in overview 
        if "N uptake during growing season" in l
    ]
    print(np.mean(N_uptake), np.std(N_uptake))
    print(f"{(time.time() - time0):.3f} seconds running one season")
    con.close()


def ingest_static_data():
    ing.ingest_static(
        con=con,
        schema="kenya",
        rast="/home/dquintero/dssat_service/data/weather_data/tav_tamp/tamp_kenya.tif",
        parname="tamp"
    )

def ingest_historical_data():
    con = pg.connect(dbname=dbname)
    observed_df = pd.read_csv(
        f"/home/dquintero/dssat_service/forecast_data/Kenya/obs_data.csv"
    )
    # Make sure the admin name column is admin1
    observed_df = observed_df.rename(columns={"admin_1": "admin1"})
    observed_df["value"] *= 1000
    db.dataframe_to_table(
        f"postgresql+psycopg2://{con.info.user}:eQY3_Fwd@localhost:{con.info.port}/{con.info.dbname}",
        observed_df,
        "kenya",
        "historical_data",
        "admin1"
    )
    con.close()
    
def ingest_latest_forecast():
    con = pg.connect(dbname=dbname)
    schema = "zimbabwe"
    # This piece of code is to upload the latest forecast tables to the db
    # Forecast map
    file = "/home/dquintero/dssat_service/forecast_data/Zimbabwe/latest_forecast.geojson"
    db.add_latest_forecast(con, schema, file)
    # All simulations results
    results_df = pd.read_csv(
        "/home/dquintero/dssat_service/forecast_data/Zimbabwe/forecast_20241111.csv"
    )
    db.dataframe_to_table(
        f"postgresql+psycopg2://{con.info.user}:eQY3_Fwd@localhost:{con.info.port}/{con.info.dbname}",
        results_df,
        schema,
        "latest_forecast_results",
        "admin1"
    )
        # Overview file info
    overview_df = pd.read_csv(
        "/home/dquintero/dssat_service/forecast_data/Zimbabwe/forecast_overview_20241111.csv"
    )
    db.dataframe_to_table(
        f"postgresql+psycopg2://{con.info.user}:eQY3_Fwd@localhost:{con.info.port}/{con.info.dbname}",
        overview_df,
        schema,
        "latest_forecast_overview",
        "admin1"
    )
    con.close()
    
def ingest_cultivars():
    con = pg.connect(dbname=dbname)
    schema = "zimbabwe"
    # db._create_cultivars_table(con, schema)
    ing.ingest_cultivars(
        con, 
        schema, 
        "/home/dquintero/dssat_service/cultivar_selection/Zimbabwe/cultivar_table.csv"
    )
    con.close()
    
def ingest_nmme_data():
    con = pg.connect(dbname=dbname)
    schema = "kenya"
    ens = 1
    ing.ingest_nmme_rain(con, schema, ens)
    ing.ingest_nmme_temp(con, schema, ens)
    
if __name__ == "__main__":
    # con = pg.connect(dbname=dbname)
    ingest_latest_forecast()
    # ingest_historical_data()
    # ingest_cultivars()
    # AdminBase(con, "kenya", "Uasin Gishu")
    # con.close()
    exit()