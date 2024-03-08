from data.download import download_era5
from data.ingest import (
    ingest_era5_record, ingest_era5_series, ingest_soil
    )
from database import (
    add_country, _create_soil_table, verify_series_continuity,
    get_era5_for_point, get_soils, connect
    )
from dssat import run_spatial_dssat
from datetime import datetime
import psycopg2 as pg

import os
import time
import numpy as np

variable = "TMAX"
date = datetime(2024, 1, 2)

dbname = "dssatserv"

def add_country():
    shp = "/home/user/dssat_service/kenya_mngCalibration/data/shapes/ken_admbnda_adm1_iebc_20191031.shp"
    add_country(dbname, "Kenya", shp, "ADM1_EN")


def ingest_era5_data():
# os.remove(nc_path)
    ingest_era5_record(dbname, "kenya", date)
    ingest_era5_series(
        dbname, "kenya", 
        datetime(2010, 1, 1), 
        datetime(2010, 1, 31)
    )

def ingest_soil_data():
    soil_path = "/home/user/dssat_service/data/soil_data/SoilGrids/KE.SOL"
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
    df = run_spatial_dssat(
        dbname=dbname, 
        schema="kenya", 
        admin1="Baringo",
        plantingdate=datetime(2021, 9, 1),
        cultivar="990002",
        nitrogen=[(0, 50), (30, 40)]
    )
    # con.close()
    print(df)
    print(f"{(time.time() - time0):.3f} seconds running one season")


if __name__ == "__main__":
    # out = verify_series_continuity(
    #     connect(dbname=dbname), 
    #     schema="kenya",
    #     table="era5_rain", 
    #     datefrom=datetime(2010, 1, 1), 
    #     dateto=datetime(2023, 12, 31)
    # )
    # print(out)
    # run_model()
    itime = time.time()
    ingest_era5_data()
    print(time.time()-itime)