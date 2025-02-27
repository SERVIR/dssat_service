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
from dssatservice.ui import plot
import dssatservice.data.transform as tr
from dssatservice.dssat import run_spatial_dssat
from datetime import datetime
import psycopg2 as pg

import os
import time
import numpy as np
import re
import pandas as pd

INPUT_PATH = "/home/diego/dssat_service_data"
with open('psswd', 'r') as f:
    psswd = f.readline().strip()
con = pg.connect(dbname="dssatserv", password=psswd, user='diego', host='localhost')

def add_country():
    shp = f"{INPUT_PATH}/rwanda_admin1/rwa_adm1.shp"
    db.add_country(con, "Rwanda", shp, "NAME_1")

def ingest_soil_data():
    soil_path = f"{INPUT_PATH}/RW.SOL"
    mask1 = f"{INPUT_PATH}/subsaharanAfrica-maize.tif"
    mask2 = f"{INPUT_PATH}/subsaharanAfrica-suitableAg-v2.tif"
    ing.ingest_soil(
        con=con,
        schema="rwanda",
        soilfile=soil_path,
        mask1=mask1,
        mask2=mask2
    )

def ingest_static_data():
    ing.ingest_static(
        con=con,
        schema="rwanda",
        rast=f"{INPUT_PATH}/tamp_rwanda_2m.tif",
        parname="tamp"
    )
    ing.ingest_static(
        con=con,
        schema="rwanda",
        rast=f"{INPUT_PATH}/tav_rwanda_2m.tif",
        parname="tav"
    )

def ingest_era5_data():
    ing.ingest_era5_series(
        con, "rwanda", 
        datetime(2024, 1, 1), 
        datetime(2025, 3, 1)
    )
    con.close()

def run_model():
    time0 = time.time()
    df, overview = run_spatial_dssat(
        con=con, 
        schema="rwanda", 
        admin1="Amajyepfo",
        plantingdate=datetime(2024, 2, 1),
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

def era5_climatology():
    ing.calculate_climatology(con, 'rwanda')

def ingest_cultivars():
    ing.ingest_cultivars(con, "rwanda", f"{INPUT_PATH}/cultivar_table.csv")

def ingest_nmme_data():
    ing.ingest_nmme(con, 'rwanda')

def run_model_forecast_onthefly():
    """
    This is just running the model as one user would do it by getting NMME data.
    It is not the function to run the operative forecast!!!
    """
    time0 = time.time()
    df, overview = run_spatial_dssat(
        con=con, 
        schema="rwanda", 
        admin1="Amajyepfo",
        plantingdate=datetime(2024, 10, 1),
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
    
if __name__ == "__main__":
    # add_country()
    # ingest_soil_data()
    # ingest_static_data()
    # ingest_era5_data()
    # run_model()
    # era5_climatology()
    # ingest_cultivars()
    # ingest_nmme_data()
    # run_model_forecast_onthefly()
    
    con.close()
