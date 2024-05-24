from data.download import download_era5
from data.ingest import (
    ingest_era5_record, ingest_era5_series, ingest_soil, 
    ingest_static
    )
from database import (
    add_country, _create_soil_table, verify_series_continuity,
    get_era5_for_point, get_soils, connect
    )
from data.transform import parse_overview
from dssat import run_spatial_dssat
from datetime import datetime
import psycopg2 as pg

import os
import time
import numpy as np
import re

variable = "TMAX"
date = datetime(2024, 1, 2)

dbname = "dssatserv"

# def add_country():
#     shp = "/home/user/dssat_service/kenya_mngCalibration/data/shapes/ken_admbnda_adm1_iebc_20191031.shp"
#     add_country(dbname, "Kenya", shp, "ADM1_EN")


def ingest_era5_data():
# os.remove(nc_path)
    ingest_era5_record(dbname, "kenya", date)
    ingest_era5_series(
        dbname, "kenya", 
        datetime(2010, 1, 1), 
        datetime(2010, 1, 31)
    )

def ingest_soil_data():
    soil_path = "/home/user/dssat_service/data/soil_data/iSDASoil/KE.SOL"
    mask1 = "/home/user/dssat_service/data/subsaharanAfrica-maize.tif"
    mask2 = "/home/user/dssat_service/data/subsaharanAfrica-suitableAg-v2.tif"
    # _create_soil_table(dbname, "kenya")
    ingest_soil(
        dbname=dbname,
        schema="kenya",
        soilfile=soil_path,
        mask1=mask1,
        mask2=mask2
    )


def run_model():
    time0 = time.time()
    # con = connect(dbname)
    df, overview = run_spatial_dssat(
        dbname=dbname, 
        schema="kenya", 
        admin1="Nakuru",
        plantingdate=datetime(2022, 2, 1),
        cultivar="990002",
        nitrogen=[(5, 20), (30, 10), (50, 10)],
        overview=True
    )
    # con.close()
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

def ingest_static_data():
    ingest_static(
        dbname=dbname,
        schema="kenya",
        rast="/home/user/dssat_service/data/weather_data/tav_tamp/tamp_kenya.tif",
        parname="tamp"
    )

if __name__ == "__main__":
    # out = verify_series_continuity(
    #     connect(dbname=dbname), 
    #     schema="kenya",
    #     table="era5_rain", 
    #     datefrom=datetime(2010, 1, 1), 
    #     dateto=datetime(2023, 12, 31)
    # )
    # print(out)
    # ingest_soil_data()
    # run_model()
    
    # Add country
    # add_country(
    #     dbname, 
    #     "Zimbabwe", 
    #     "/home/user/dssat_service/fewsnet_data/admin_bounds/zimbabwe_fewsnet_admin2.geojson", 
    #     "name"
    # )
    
    # Ingest soil data
    ingest_soil(
        dbname=dbname,
        schema="zimbabwe",
        soilfile="/home/user/dssat_service/data/soil_data/iSDASoil/ZW.SOL",
        mask1="/home/user/dssat_service/data/subsaharanAfrica-maize.tif",
        mask2="/home/user/dssat_service/data/subsaharanAfrica-suitableAg-v2.tif"
    )
    
    # Ingest static data
    # static_data = [
    #     ("tamp", "/home/user/dssat_service/data/weather_data/tav_tamp/tamp_zimbabwe.tif"),
    #     ("tav", "/home/user/dssat_service/data/weather_data/tav_tamp/tav_zimbabwe.tif")
    # ]
    # for parname, rast in static_data:
    #     ingest_static(
    #         dbname=dbname,
    #         schema="zimbabwe",
    #         rast=rast,
    #         parname=parname
    #     )
    
    # Ingest era5 data
    # ingest_era5_series(
    #     dbname, "zimbabwe", 
    #     datetime(2010, 2, 1), 
    #     datetime(2023, 12, 31)
    # )