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
        db.create_static_table(dbname, schema)

    db.verify_static_par_exists(dbname, schema, parname)
    db.tiff_to_db(
        tiffpath=rast,
        dbname=dbname,
        schema=schema,
        table="static",
        par=parname
    )

