
import sys 
sys.path.append("/home/user/dssat_service/regional_service")
from dssat import run_spatial_dssat
from datetime import datetime, timedelta
import numpy as np
import os 
os.chdir("/home/user/dssat_service/regional_service/experiments")

dbname = "dssatserv"
# Top 15 states make up to nearly 75% of total production
ADMIN1_LIST = [
    'Trans Nzoia', 'Bungoma', 'Uasin Gishu', 'Nandi', 'Kakamega', 'Kisii','Nakuru', 'Siaya', 'Migori', 
    'Narok', 'Nyamira', 'Meru', 'Kericho','Bomet',
    'Makueni'
]

with open("cultivars_list.txt", "r") as f:
    lines = f.readlines()
cultivars = [l[:6] for l in lines[1:]]

def run_model(admin1, cultivar, plantingdate):
    # con = connect(dbname)
    planting_window_start = plantingdate - timedelta(days=15)
    planting_window_end = plantingdate + timedelta(days=15)
    sim_controls = {
        "PLANT": "F", # Automatic, force in last day of window
        "PFRST": planting_window_start.strftime("%y%j"),
        "PLAST": planting_window_end.strftime("%y%j"),
        "PH2OL": 50, "PH2OU": 100, "PH2OD": 20, 
        "PSTMX": 40, "PSTMN": 10,
        "WATER": "N", "NITRO": "N"
    }
    df = run_spatial_dssat(
        dbname=dbname, 
        schema="kenya", 
        admin1=admin1,
        plantingdate=plantingdate,
        cultivar=cultivar,
        nitrogen=[(5, 0),],
        # overview=True,
        sim_controls=sim_controls,
        all_random=False
    )
    df = df.iloc[:, 3:].astype(int).replace(-99, np.nan)
    # print(df.describe())
    return df

if __name__ == "__main__":
    from itertools import product
    import pandas as pd
    YEARS = (2020, )
    YEARS = (2018, 2019, 2020, 2021, 2022)
    for year in YEARS:
        iterable = product(ADMIN1_LIST[:], cultivars)
        for admin1, cultivar in iterable:
            tmp_df = run_model(
                admin1, cultivar, datetime(year, 4, 1)
            )
            tmp_df.to_csv(f"cultivar_results/{admin1}_{year}_{cultivar}.csv", index=False)
        
            