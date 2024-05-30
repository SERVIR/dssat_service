"""
Contains functions and classes to handle the selection of different options 
in the user interface.
"""
import sys
sys.path.append("..")
import database as db
from dssat import run_spatial_dssat
from datetime import datetime, timedelta
import numpy as np
import pandas as  pd
from dataclasses import dataclass

BASELINE_YEARS = (2017, 2018, 2019, 2020, 2021)
Q_RANGE = (.05, .95)
QUANTILES_TO_COMPARE = np.arange(0.025, 1, 0.05)
SCHEMAS = ("kenya", "zimbabwe")

def admin_list(con, schema):
    """
    Returns a list with the admin units set for simulation in that schema
    """
    return list(sorted(db.fetch_admin1_list(con, schema)))

class AdminBase:
    """
    This class handles the admin unit and its parameters. Those parameters include:
    baseline parameters, baseline runs, cultivars for that admin unit. Basically 
    anything that is associated to that admin unit.
    """
    def __init__(self, con, schema, admin1):
        """
        Initializes an object having a psycopg2 connection object, schema name
        and admin unit name.
        """
        self.connection = con
        self.admin1 = admin1 
        self.schema = schema
        self.baseline_pars = db.fetch_baseline_pars(con, schema, admin1)
        baseline_data = db.fetch_baseline_run(con, schema, admin1)
        self.baseline_run = baseline_data.loc[
            baseline_data.year.isin(BASELINE_YEARS)
        ]

        self.baseline_stats = self.baseline_quantile_stats()
        self.validation_run =  baseline_data.dropna()
        self.cultivars = db.fetch_cultivars(con, schema, admin1)
        
        
    def baseline_description(self):
        """
        Returns a string that describes the current baseline scenario.
        """
        cultivar, month, nitro, crps, rpss =self.baseline_pars.values()
        cultivar =self.cultivars.loc[cultivar]
        desc = f"Baseline was estimated assuming a {cultivar.season_length} " + \
            f"days {cultivar.yield_category} " + \
            f"yield potential cultivar ({cultivar.yield_range} t/ha) " + \
            f"planted on {datetime(2000, month, 1).strftime('%B')} " + \
            f"and fertilized with {nitro:.0f} kg of Nitrogen"
        return desc
    
    def baseline_quantile_stats(self):
        """
        Calculates the mean and std for each quantile in the baseline run.
        """
        baseline_df = self.baseline_run
        baseline_stats = (
            baseline_df
            .groupby(["year"]).sim
            .quantile(QUANTILES_TO_COMPARE)
            .reset_index().rename(columns={"level_1": "quantile"})
            .groupby("quantile").sim
            .agg(["mean", "std"])
        )
        return baseline_stats
    
    def get_quantile_anomalies(self, df_run):
        """
        Estimates the quantile anomaly for a run based on its own baseline
        """
        df_run = df_run.copy()
        df_run["HARWT"] = df_run.HARWT.astype(float)/1000
        run_stats = (
            df_run.HARWT
            .quantile(QUANTILES_TO_COMPARE)
            .reset_index().rename(columns={"index": "quantile"})
            .set_index("quantile")
        )
        anomalies = (run_stats.HARWT - self.baseline_stats["mean"])/ \
            self. baseline_stats["std"]
        return anomalies

@dataclass
class SimulationPars:
    nitrogen_dap: list
    nitrogen_rate: list
    cultivar: str
    planting_date: datetime

class Session:
    """
    This class represents the simulation sesion. Each simulation sesion has to
    be initialized by passing an AdminBase object. Therefore, each session focus
    in one admin unit.
    
    This class handles the interaction with the ui. Therefore, this is the class
    that runs the model and that runs the model.
    """
    def __init__(self, adminBase:AdminBase):
        """
        Initializes the session setting the simulation parameters to the admin 
        baseline
        """
        self.adminBase = adminBase
        self.simPars = SimulationPars(
            nitrogen_dap = (5, 30, 50),
            nitrogen_rate = [self.adminBase.baseline_pars["nitrogen"]/3]*3,
            planting_date = datetime(
                2022, self.adminBase.baseline_pars["planting_month"], 1
            ),
            cultivar = self.adminBase.baseline_pars["cultivar"]
        )
        self.experiment_results = pd.DataFrame(
            [], 
            columns=[
                "planting", "nitro_dap", "nitro_rate", "cultivar", 
                "yield_range", 'harvest_range', 
            ]
        )
        self.latest_run = None
        self.latest_overview = None
    
    def add_experiment_results(self):
        yield_range = (
            self.latest_run.HARWT.astype(float)/1000
        ).quantile((.05, .95)).values
        harvest_range = (
            self.latest_run.MAT.astype(int)
        ).quantile((.05, .95)).values
        tmp_df = pd.DataFrame([{
            "planting": self.simPars.planting_date,
            "nitro_dap": self.simPars.nitrogen_dap,
            "nitro_rate": self.simPars.nitrogen_rate,
            "cultivar": self.simPars.cultivar,
            "yield_range": tuple(yield_range),
            "harvest_range": tuple(map(int, harvest_range))
        }])
        self.experiment_results = pd.concat(
            [self.experiment_results, tmp_df], 
            ignore_index=True
        )
        
    
    def run_experiment(self):
        """
        Runs the model using the last parameters defined.
        """
        nitro = list(zip(self.simPars.nitrogen_dap, self.simPars.nitrogen_rate))
        plantingdate = datetime(
            self.simPars.planting_date.year, 
            self.simPars.planting_date.month, 
            self.simPars.planting_date.day
        )
        planting_window_start = plantingdate - timedelta(days=5)
        planting_window_end = plantingdate + timedelta(days=5)
        sim_controls = {
            "PLANT": "F", # Automatic, force in last day of window
            "PFRST": planting_window_start.strftime("%y%j"),
            "PLAST": planting_window_end.strftime("%y%j"),
        }
        df, overview = run_spatial_dssat(
            dbname="", 
            con=self.adminBase.connection,
            schema=self.adminBase.schema, 
            admin1=self.adminBase.admin1,
            plantingdate=plantingdate,
            cultivar=self.simPars.cultivar,
            nitrogen=nitro,
            overview=True,
            all_random=True,
            sim_controls=sim_controls
        )
        self.latest_run = df
        self.latest_overview = overview
        self.add_experiment_results()
    
    # def run_baseline(pars):
    #     df = pd.DataFrame()
    # if "nitrogen_dap" in pars.__dict__:
    #     nitro = list(zip(pars.nitrogen_dap, pars.nitrogen_rate))
    # else:
    #     nitro = [(0, pars.nitrogen),]
    # for year in BASELINE_YEARS:
    #     tmp_df = run_spatial_dssat(
    #         dbname=DBNAME, 
    #         schema=pars.adminBase.schema, 
    #         admin1=pars.adminBase.admin1,
    #         plantingdate=datetime(pars.planting_date.year, pars.planting_date.month, pars.planting_date.day),
    #         cultivar=pars.cultivar,
    #         nitrogen=nitro,
    #         overview=False,
    #         all_random=True
    #     )
    #     tmp_df["year"] = year 
    #     df = pd.concat([df, tmp_df], ignore_index=True)
    # return df
        
         