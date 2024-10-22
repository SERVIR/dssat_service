from dssatservice.data.download import download_era5, download_nmme
from dssatservice.data.ingest import (
    ingest_era5_record, ingest_era5_series, ingest_soil, 
    ingest_static, ingest_cultivars, ingest_baseline_pars,
    ingest_baseline_run, calculate_climatology, ingest_nmme_rain,
    ingest_nmme_temp, ingest_nmme
    )
from dssatservice.database import (
    add_country, _create_soil_table, verify_series_continuity,
    get_era5_for_point, get_soils, connect, _create_cultivars_table,
    fetch_admin1_list, fetch_baseline_pars, get_envelope,
    _create_climate_forecast_table, fetch_cultivars, add_latest_forecast,
    dataframe_to_table, fetch_forecast_tables
    )
from dssatservice.ui.base import (
    admin_list, AdminBase, Session
)
from dssatservice.data.transform import parse_overview
from dssatservice.dssat import run_spatial_dssat
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
#     shp = "/home/dquintero/dssat_service/kenya_mngCalibration/data/shapes/ken_admbnda_adm1_iebc_20191031.shp"
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
    soil_path = "/home/dquintero/dssat_service/data/soil_data/iSDASoil/KE.SOL"
    mask1 = "/home/dquintero/dssat_service/data/subsaharanAfrica-maize.tif"
    mask2 = "/home/dquintero/dssat_service/data/subsaharanAfrica-suitableAg-v2.tif"
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

def run_model_forecast():
    time0 = time.time()
    # con = connect(dbname)
    df, overview = run_spatial_dssat(
        dbname=dbname, 
        schema="kenya", 
        admin1="Bomet",
        plantingdate=datetime(2024, 3, 1),
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
        rast="/home/dquintero/dssat_service/data/weather_data/tav_tamp/tamp_kenya.tif",
        parname="tamp"
    )
    
def create_cultivars(con, country, csvfile):
    _create_cultivars_table(con, country)
    ingest_cultivars(
        con, country, csvfile
    )



if __name__ == "__main__":
    # NMME Download
    schema = "kenya"
    con = pg.connect(dbname=dbname)
    import pandas as pd
    observed_df = pd.read_csv(
        f"/home/dquintero/dssat_service/forecast_data/Kenya/obs_data.csv"
    )
    dataframe_to_table(
        f"postgresql+psycopg2://{con.info.user}:eQY3_Fwd@localhost:{con.info.port}/{con.info.dbname}",
        observed_df,
        "kenya",
        "historical_data",
        "admin1"
    )
    exit()
    fetch_forecast_tables(con, schema, "Bomet")
    # This piece of code is to upload the latest forecast tables to the db
    # Forecast map
    # file = "/home/dquintero/dssat_service/forecast_data/Kenya/latest_forecast.geojson"
    # add_latest_forecast(con, schema, file)
    # All simulations results
    exit()
    results_df = pd.read_csv(
        "/home/dquintero/dssat_service/forecast_data/Kenya/forecast_241021.csv"
    )
    dataframe_to_table(
        f"postgresql+psycopg2://{con.info.user}:eQY3_Fwd@localhost:{con.info.port}/{con.info.dbname}",
        results_df,
        "kenya",
        "latest_forecast_results",
        "admin1"
    )
    # Overview file info
    overview_df = pd.read_csv(
        "/home/dquintero/dssat_service/forecast_data/Kenya/forecast_overview_241021.csv"
    )
    dataframe_to_table(
        f"postgresql+psycopg2://{con.info.user}:eQY3_Fwd@localhost:{con.info.port}/{con.info.dbname}",
        overview_df,
        "kenya",
        "latest_forecast_overview",
        "admin1"
    )
    exit()
    
    # fetch_cultivars(con, schema, "Nakuru")
    # exit()
    # create_cultivars(
    #     con, 
    #     schema, 
    #     "/home/dquintero/dssat_service/cultivar_selection/Kenya/cultivar_table.csv"
    # )
    
    # Get the envelope for that region
    # bbox = get_envelope(dbname, schema)
    # download_nmme("Precipitation", 1, bbox)
    # calculate_climatology(dbname, schema)
    # ingest_nmme_rain(dbname, schema, 1)
    ingest_nmme_rain(con, schema, 1)
    # ingest_nmme_temp(dbname, schema, 1)
    # ingest_nmme(dbname, schema)
    # run_model_forecast()
    # con = connect(dbname)
    # admin = admin_list(con, "kenya")
    # session = Session(AdminBase(con, "kenya", "Bomet"))
    # # session.adminBase.baseline_quantile_stats()
    # # session.adminBase.baseline_description()
    # session.run_experiment()
    # add_anomaly_bar(None, session, False)
    # # session.run_experiment()
    # session.simPars.nitrogen_rate = (10, 20, 20)
    # session.new_baseline()
    # ingest_baseline_run(
    #     dbname, "zimbabwe",
    #     "/home/dquintero/dssat_service/web_service_dev/experiments/parameters/zimbabwe_baseline_run.csv"
    # )
    # ingest_baseline_pars(
    #     dbname, "zimbabwe",
    #     "/home/dquintero/dssat_service/web_service_dev/experiments/parameters/zimbabwe_baseline_pars.csv"
    # )
    # for country in ("zimbabwe", "kenya"):
    #     _create_cultivars_table(dbname, country)
    #     ingest_cultivars(
    #         dbname, country,
    #         f"/home/dquintero/dssat_service/web_service_dev/experiments/parameters/{country}_cultivars_final.csv"
    #     )
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
    #     "/home/dquintero/dssat_service/fewsnet_data/admin_bounds/zimbabwe_fewsnet_admin2.geojson", 
    #     "name"
    # )
    
    # Ingest soil data
    # ingest_soil(
    #     dbname=dbname,
    #     schema="zimbabwe",
    #     soilfile="/home/dquintero/dssat_service/data/soil_data/iSDASoil/ZW.SOL",
    #     mask1="/home/dquintero/dssat_service/data/subsaharanAfrica-maize.tif",
    #     mask2="/home/dquintero/dssat_service/data/subsaharanAfrica-suitableAg-v2.tif"
    # )
    
    # Ingest static data
    # static_data = [
    #     ("tamp", "/home/dquintero/dssat_service/data/weather_data/tav_tamp/tamp_zimbabwe.tif"),
    #     ("tav", "/home/dquintero/dssat_service/data/weather_data/tav_tamp/tav_zimbabwe.tif")
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