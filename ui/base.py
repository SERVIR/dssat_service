"""
Contains functions and classes to handle the selection of different options 
in the user interface.
"""
import sys
sys.path.append("..")
import database as db
from datetime import datetime


BASELINE_YEARS = (2017, 2018, 2019, 2020, 2021)
SCHEMAS = ("kenya", "zimbabwe")

def admin_list(con, schema):
    """
    Returns a list with the admin units set for simulation in that schema
    """
    return list(sorted(db.fetch_admin1_list(con, schema)))


class AdminBase:
    def __init__(self, con, schema, admin1):
        self.admin1 = admin1 
        self.schema = schema
        self.baseline_pars = db.fetch_baseline_pars(con, schema, admin1)
        baseline_data = db.fetch_baseline_run(con, schema, admin1)
        self.baseline_run = baseline_data.loc[
            baseline_data.year.isin(BASELINE_YEARS)
        ]
        self.validation_run =  baseline_data.dropna()
        self.cultivars = db.fetch_cultivars(con, schema, admin1)
        
    def baseline_description(self):
        cultivar, month, nitro, crps, rpss =self.baseline_pars.values()
        cultivar =self.cultivars.loc[cultivar]
        desc = f"Baseline was estimated assuming a {cultivar.season_length} " + \
            f"days {cultivar.yield_category} " + \
            f"yield potential cultivar ({cultivar.yield_range} t/ha) " + \
            f"planted on {datetime(2000, month, 1).strftime('%B')} " + \
            f"and fertilized with {nitro:.0f} kg of Nitrogen"
        return desc
    
    def run_new_baseline(self):
        return
    
    
        
         