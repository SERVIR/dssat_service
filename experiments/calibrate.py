
"""
This script tests and adjusts some of the parameters of the model.
For each of the cultivar it finds the nitrogen rate that minimizes the error.
Then it tries three planting months, and estimates correlation and error for each.

Use Africa cultivars. 
1. Adjust BIAS for each cultivar using fertilizer.
2. Try different planting months
3. Select best planting month based on correlation.
4. Keep cultivars that met some correlation/rmse criteria.
"""
import sys 
sys.path.append("/home/user/dssat_service/regional_service")
from dssat import run_spatial_dssat
from spatialDSSAT.run import GSRun
from datetime import datetime, timedelta
import numpy as np
import pandas as pd 
from scipy.optimize import minimize_scalar
from scipy.stats import pearsonr
import os 
os.chdir("/home/user/dssat_service/regional_service/experiments")

DBNAME = "dssatserv"
# Top 15 states make up to nearly 75% of total production
ADMIN1_LIST = [
    'Trans Nzoia', 'Bungoma', 'Uasin Gishu', 'Nandi', 'Kakamega', 'Kisii',
    'Nakuru', 'Siaya', 'Migori', 'Narok', 'Nyamira', 'Meru', 'Kericho','Bomet',
    'Makueni'
]

# with open("cultivars_list.txt", "r") as f:
#     lines = f.readlines()
# cultivars = [l[:6] for l in lines[1:]]
CULTIVARS = [
    'IF0001', 'IF0002', 'IF0003', 'IF0004', 'IF0005', 'IF0006', 'IF0007', 
    'IF0008', 'IF0009', 'IF0010', 'IF0011', 'IM0001', 'IM0002', 'IM0003', 
    'IF0018', 'IF0019', 'IF0020', 'IF0021', 'IF0022', 'IB0067', 'KA0001', 
    'EM0001', 'KY0001', 'KY0002', 'KY0003', 'KY0004', 'KY0005', 'KY0006', 
    'KY0007', 'KY0008', 'KY0009', 'KY0010', 'KY0011', 'KY0012', 'KY0013', 
    'KY0014', 'KY0015', 'KY0016', 'KY0017', 'KY0018', 'GH0010'
]
obs = pd.read_csv("~/dssat_service/fewsnet_data/kenya_shortRains_maize.csv")
# obs = obs.loc[obs.season_name == "Long rains harvest"]
obs = obs.loc[obs.year > 2010]

def get_dssat_inputs(admin1, year):
    inputs = run_spatial_dssat(
        dbname=DBNAME, 
        schema="kenya", 
        admin1=admin1,
        plantingdate=datetime(year, 3, 1),
        cultivar=CULTIVARS[0],
        nitrogen=[(5, 15), (30, 15), (60, 15)],
        # overview=True,
        all_random=False,
        return_input=True
    )
    return inputs

def run_single(fertilizer, cultivar, plantingdate, inputs):
    fertilizer = int(fertilizer)
    fertilizer = int(fertilizer/3)
    nitro = [(0, fertilizer), (30, fertilizer), (60, fertilizer)]
    gs = GSRun()
    for (_, wth), (_, sol) in inputs:
        gs.add_treatment(
            soil_profile=sol,
            weather=wth,
            nitrogen=nitro,
            planting=plantingdate,
            cultivar=cultivar
        )
    planting_window_start = plantingdate - timedelta(days=15)
    planting_window_end = plantingdate + timedelta(days=15)
    sim_controls = {
        "PLANT": "F", # Automatic, force in last day of window
        "PFRST": planting_window_start.strftime("%y%j"),
        "PLAST": planting_window_end.strftime("%y%j"),
        "PH2OL": 50, "PH2OU": 100, "PH2OD": 20, 
        "PSTMX": 40, "PSTMN": 10
    }
    df = gs.run(
        start_date=plantingdate - timedelta(30),
        sim_controls=sim_controls
    )
    return df

def calculate_mse(admin1, fertilizer, cultivar, inputs_dict):
    residuals = []
    for _, row in obs.loc[obs.admin_1 == admin1].iterrows():
        plantingdate = datetime(row.year, 4, 1)
        df = run_single(
            fertilizer, cultivar, plantingdate, inputs_dict[row.year]
        )
        residuals.append(df.HARWT.astype(int).median()/1000 - row.value)
    return sum([r**2 for r in residuals])


def optimize_nitrogen(admin1, cultivar, inputs):
    res = minimize_scalar(
        fun=lambda x: calculate_mse(admin1, x, cultivar, inputs),
        bounds=(0, 200),
        options={"maxiter": 10, "disp": 0, "xatol": 5}
    )
    return res.x

def process_cultivar(admin1, cultivar, inputs_dict):
    records = []
    nitro = optimize_nitrogen(admin1, cultivar, inputs_dict)
    for month in range(3, 6):
        sim_yield = []
        for _, row in obs.loc[obs.admin_1 == admin1].iterrows():
            plantingdate = datetime(row.year, month, 1)
            df = run_single(
                nitro, cultivar, plantingdate, inputs_dict[row.year]
            )
            sim_yield.append(df.HARWT.astype(int).median()/1000)
        obs_yield = obs.loc[obs.admin_1 == admin1, "value"].values
        r, p =pearsonr(sim_yield, obs_yield)
        me = (np.array(sim_yield) - obs_yield).mean()
        mae = abs(obs_yield - np.array(sim_yield)).mean()
        rmse = np.sqrt((obs_yield - np.array(sim_yield))**2).mean()
        records.append((
            admin1, cultivar, nitro, month, r, p, me, mae, rmse
        ))
        print("{0}\t{1}\t{2:>.1f}\t{3:>}\t{4:>.3f}\t{5:>.3f}\t{6:>.3f}\t{7:>.3f}\t{8:>.3f}".format(*records[-1]))
    pd.DataFrame(
        records, 
        columns=["admin1", "cultivar", "nitro", "month", "r", "p", "me", "mae", "rmse"]
    ).to_csv(f"/home/user/dssat_service/regional_service/experiments/parameters/{admin1}_{cultivar}.csv", index=False)

if __name__ == "__main__":
    for admin1 in ADMIN1_LIST[:]:
        inputs_dict = {}
        for _, row, in obs.loc[obs.admin_1 == admin1].iterrows():
            inputs_dict[row.year] = get_dssat_inputs(admin1, row.year)
        for cultivar in CULTIVARS:
            process_cultivar(admin1, cultivar, inputs_dict)
    # from scipy.optimize import least_squares
    # from scipy.stats import pearsonr
    # records = []
    # admin1 = ADMIN1_LIST[1]
    # for cultivar in cultivars:
    #     fcost = lambda x: calculate_mse(admin1, x, cultivar)
    #     res = least_squares(
    #         fcost, x0=50, bounds=(0, 200), max_nfev=10, xtol=.05,
    #         verbose=2,
    #     )
    #     nitro = int(res.x)
    #     for month in range(3, 6):
    #         sim_yield = []
    #         for _, row in obs.loc[obs.admin_1 == admin1].iterrows():
    #             plantingdate = datetime(row.year, month, 1)
    #             df = run_model(admin1, cultivar, plantingdate, nitro)
    #             sim_yield.append(df.HARWT.astype(int).median()/1000)
    #         obs_yield = obs.loc[obs.admin_1 == admin1, "value"].values
    #         r, p =pearsonr(sim_yield, obs_yield)
    #         me = (np.array(sim_yield) - obs_yield).mean()
    #         mae = abs(obs_yield - np.array(sim_yield)).mean()
    #         rmse = np.sqrt((obs_yield - np.array(sim_yield))**2).mean()
    #         records.append((
    #             admin1, cultivar, nitro, month, r, p, me, mae, rmse
    #         ))
    # print(records[-1])
    # YEARS = (2020, )
    # YEARS = (2018, 2019, 2020, 2021, 2022)
    # for year in YEARS:
    #     iterable = product(ADMIN1_LIST[:], cultivars)
    #     for admin1, cultivar in iterable:
    #         tmp_df = run_model(
    #             admin1, cultivar, datetime(year, 4, 1)
    #         )
    #         tmp_df.to_csv(f"cultivar_results/{admin1}_{year}_{cultivar}.csv", index=False)
        
            