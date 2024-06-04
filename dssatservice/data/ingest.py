"""
This module includes functions to ingest data in the database. It integrates the
download, transform, and ingestion process.
"""
from datetime import datetime, timedelta
import os 

import sys
sys.path.append("..")
import database as db

from . import download
from . import transform
from tqdm import tqdm

import rasterio as rio
import pandas as pd

VARIABLES_ERA5_NC = db.VARIABLES_ERA5_NC

def ingest_era5_record(dbname:str, schema:str, date:datetime):
    """
    Add a row to each ERA5 table: rain, tmax, tmin, and srad. Given a schema (country),
    it will download, process, and ingest the data for that schema. The country must 
    be already created in the database. The data extent is defined by the geometry
    in the COUNTRY.admin table.
    """
    schema = schema.lower()
    # Check admin shapefile is in the db
    assert db.table_exists(dbname, schema, "admin"), \
        f"{schema}.admin does not exists. Make sure to add it using " +\
        "the add_country function"

    # Get the envelope for that region
    bbox = db.get_envelope(dbname, schema)

    for var, ncvar in VARIABLES_ERA5_NC.items():
        nc_path = download.download_era5(date, var, bbox)
        tiff_path = transform.nc_to_tiff(ncvar, date, nc_path)
        table = f"era5_{var}"
        # Delete rasters if exists
        db.delete_rasters(dbname, schema, table, date)
        db.tiff_to_db(tiff_path, dbname, schema, table, date)
        os.remove(nc_path)
        os.remove(tiff_path)

def ingest_era5_series(dbname:str, schema:str, datefrom:datetime, dateto:datetime):
    """
    Ingest data for the requested schema, from the specified to the specified
    dates. It ingests all four ERA5 variables needed to run the model
    """
    date = datefrom 
    while date <= dateto:
        ingest_era5_record(dbname, schema, date)
        date += timedelta(days=1)

def ingest_soil(dbname:str, schema:str, soilfile:str, mask1:str,
                mask2:str):
    """
    Ingests soil in the database. It will create one row per soil profile. Each
    soil profile will contain a flag for two crop masks.

    Arguments
    ----------
    dbname: str
        Name of the database
    schema: str
        Name of the schema (domain or country)
    soilfile: str
        Path to a DSSAT .SOL file that includes all the soil profiles of the
        for that country. Those are obtained from 
        https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/1PEEY0
    mask1 and mask2: str
        Path to two different crop masks
    """
    with open(soilfile, "r") as f:
        soil_lines = f.readlines()
    ds_mask1 = rio.open(mask1)
    ds_mask2 = rio.open(mask2)

    allProfile = False
    soilProfile_lines = []

    con = db.connect(dbname)
    cur = con.cursor()
    if not db.table_exists(dbname, schema, f"soil"):
        db._create_soil_table(dbname, schema)

    for line in tqdm(soil_lines):
        if line[0] == "*":
            if len(soilProfile_lines) > 1:
                allProfile = True
            else:
                soilProfile_lines = [line]
                allProfile = False
                continue
        if not allProfile:       
            soilProfile_lines.append(line)

        if allProfile:
            lat, lon = map(float, soilProfile_lines[2][25:42].split())
            mask1_value = ds_mask1.sample([(lon, lat)]).__next__()
            mask1_value = repr(any(mask1_value)).upper()
            mask2_value = ds_mask2.sample([(lon, lat)]).__next__()
            mask2_value = repr(any(mask2_value)).upper()
            # TODO: Raise if the point is not in crop mask
            soilProfile_lines = "".join(soilProfile_lines)
            # Write to DB
            query = """
                INSERT INTO {0}.soil(geom, mask1, mask2, soil) 
                VALUES (ST_Point({1}, {2}), {3}, {4}, '{5}');
                """.format(schema, lon, lat, mask1_value, mask2_value, 
                           soilProfile_lines)
            cur.execute(query)
            con.commit()
            allProfile = False
            soilProfile_lines = [line]
    cur.close()
    con.close()

def ingest_static(dbname:str, schema:str, rast:str, parname:str):
    """
    Ingests static raster data. This data includes any static data exluding soils.
    It is open to include any static data such as crop masks, planting dates, etc.
    It was originally created to ingest TAV and TAMP soil parameters for DSSAT.

    Arguments
    ----------
    dbname: str
        Name of the database
    schema: str
        Name of the schema (domain or country)
    rast: str
        Path to the raster to ingest.
    parname: str
        Name of the static variable to ingest. Up to 32 characters
    """

    if not db.table_exists(dbname, schema, "static"):
        db._create_static_table(dbname, schema)
    assert not db.verify_static_par_exists(dbname, schema, parname), \
        f"{parname} already in static table. Remove it before ingesting it back"
    
    db.tiff_to_db(
        tiffpath=rast,
        dbname=dbname,
        schema=schema,
        table="static",
        par=parname
    )
    
def ingest_cultivars(dbname:str, schema:str, csv:str):
    """
    Ingest cultivar data. The data to ingest must be in a csv with the next
    columns: 
        admin1
        yield_category: one among High, Medium, Low
        season_length: average season lenght
        cultivar: DSSAT cultivar code
        yield_avg: average potential yield
        yield_range: string with yield range for that yield category
    """
    con = db.connect(dbname)
    cur = con.cursor()
    df = pd.read_csv(csv)
    for _, row in df.iterrows():
        query = """
            INSERT INTO {0}.cultivars(
                admin1, cultivar, yield_category, season_length, yield_avg,
                yield_range
                ) 
            VALUES ('{1}', '{2}', '{3}', '{4}', '{5}', '{6}');
            """.format(
                schema, row.admin1.replace("'", "''"), row.cultivar, 
                row.yield_category, row.season_length, row.yield_avg, 
                row.yield_range
            )
        cur.execute(query)
        con.commit()
        
def ingest_baseline_pars(dbname:str, schema:str, csv:str):
    """
    Ingest baseline parameters. The data to ingest must be in a csv with the next
    columns: 
        admin1
        cultivar: DSSAT cultivar code
        nitrogen : nitrogen rate used 
        planting_month : planting month as integer
        rpss: Ranked probability skill score
        crps: Continous ranked probability score
    """
    if not db.table_exists(dbname, schema, "baseline_pars"):
        db._create_baseline_pars_table(dbname, schema)
    con = db.connect(dbname)
    cur = con.cursor()
    df = pd.read_csv(csv)
    for _, row in df.iterrows():
        query = """
            INSERT INTO {0}.baseline_pars(
                admin1, cultivar, nitrogen, planting_month, crps, rpss
                ) 
            VALUES ('{1}', '{2}', {3}, {4}, {5}, {6});
            """.format(
                schema, row.admin1.replace("'", "''"), row.cultivar, row.nitro, 
                row.month, row.crps, row.rpss
            )
        cur.execute(query)
        con.commit()

def ingest_baseline_run(dbname:str, schema:str, csv:str):
    """
    Ingest baseline run. The data to ingest must be in a csv with the next
    columns: 
        admin1
        harwt: dssat yield (t/ha)
        obs: observed yield for that year (t/ha)
        year: year
    """
    if not db.table_exists(dbname, schema, "baseline_run"):
        db._create_baseline_run_table(dbname, schema)
    con = db.connect(dbname)
    cur = con.cursor()
    df = pd.read_csv(csv)
    for _, row in df.iterrows():
        query = """
            INSERT INTO {0}.baseline_run(
                admin1, harwt, obs, year
                ) 
            VALUES ('{1}', '{2}', '{3}', '{4}');
            """.format(
                schema, row.admin1.replace("'", "''"), row.harwt, row.obs, 
                row.year
            )
        cur.execute(query)
        con.commit()