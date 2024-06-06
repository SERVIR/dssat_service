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

QUANTILES_TO_COMPARE = np.arange(0.025, 1, 0.05)
SCHEMAS = ("kenya", "zimbabwe")

def admin_list(con, schema):
    """
    Returns a list with the admin units set for simulation in that schema
    """
    return list(sorted(db.fetch_admin1_list(con, schema)))

@dataclass
class SimulationPars:
    nitrogen_dap: list
    nitrogen_rate: list
    cultivar: str
    planting_date: datetime

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
        pars = db.fetch_baseline_pars(con, schema, admin1)
        self.baseline_pars = SimulationPars(
            nitrogen_dap = (5, 30, 60),
            nitrogen_rate = tuple([pars["nitrogen"]/3]*3),
            cultivar = pars["cultivar"],
            planting_date = datetime(BASELINE_YEARS[-1], pars["planting_month"], 1)
        )
        baseline_data = db.fetch_baseline_run(con, schema, admin1)
        self.baseline_run = baseline_data.loc[
            baseline_data.year.isin(BASELINE_YEARS)
        ]

        self.baseline_stats = self.baseline_quantile_stats()
        self.validation_run =  baseline_data.dropna()
        tmp_df = db.fetch_cultivars(con, schema, admin1)
        tmp_df["yield_avg"] = (tmp_df.yield_avg/1000).round(1)
        tmp_df = tmp_df.set_index(["yield_avg", "season_length"])
        self.cultivars = tmp_df.sort_index()
        
        self.cultivar_labels = dict(zip(
            self.cultivars.cultivar,
            self.cultivars.index.map(
                lambda x: f"{x[0]} kg/ha pot. - {x[1]} days mat."
            ),
        ))
        self.cultivar_labels_inv = {v: k for k, v in self.cultivar_labels.items()}
        
        
    def baseline_description(self):
        """
        Returns a string that describes the current baseline scenario.
        """
        cultivar = self.baseline_pars.cultivar
        plantingdate = self.baseline_pars.planting_date
        nitro = sum(self.baseline_pars.nitrogen_rate)
        cultivar = self.cultivars.loc[self.cultivars.cultivar == cultivar].iloc[0]
        desc = f"Baseline was estimated assuming a {cultivar.name[1]} " + \
            f"days {cultivar.yield_category} " + \
            f"yield potential cultivar ({cultivar.name[0]} t/ha) " + \
            f"planted on {plantingdate.strftime('%B')} " + \
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
        self.simPars = self.adminBase.baseline_pars
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
        
    
    def run_experiment(self, baseline_run=False, **kwargs):
        """
        Runs the model using the last parameters defined.
        """
        nitro = list(zip(self.simPars.nitrogen_dap, self.simPars.nitrogen_rate))
        if baseline_run:
            plantingdate = datetime(
                kwargs.get("year", None), 
                self.simPars.planting_date.month, 
                self.simPars.planting_date.day
            )
        else:
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
        if baseline_run:
            return df
        else:
            self.latest_run = df
            self.latest_overview = overview
            self.add_experiment_results()
        
    
    def new_baseline(self):
        """
        Run the model to get a new baseline given the current parameters
        """
        df_list = []
        for year in BASELINE_YEARS:
            tmp_df = self.run_experiment(baseline_run=True, year=year)
            tmp_df["year"] = year
            df_list.append(tmp_df)
        df = pd.concat(df_list, ignore_index=True)
        df = df[["year", "HARWT"]].rename(columns={"HARWT": "sim"})
        df["sim"] = df.sim.astype(float)/1000
        df["obs"] = np.nan
        self.baseline_pars = self.simPars
        self.adminBase.baseline_run = df
        self.adminBase.baseline_stats = self.adminBase.baseline_quantile_stats()
        
         