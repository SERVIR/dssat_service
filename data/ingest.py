"""
This module includes functions to ingest data in the database. It integrates the
download, transform, and ingestion process.
"""
from datetime import datetime, timedelta
import os 

from . import database as db
from .database import VARIABLES_ERA5_NC
from . import download
from . import transform
from tqdm import tqdm

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

    allProfile = False
    soilProfile_lines = []

    con = db.connect(dbname)
    cur = con.cursor()
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
            soilProfile_lines = "".join(soilProfile_lines)
            # Write to DB
            query = """
                INSERT INTO {0}.soil(geom, mask1, mask2, soil) 
                VALUES (ST_Point({1}, {2}), TRUE, TRUE, '{3}');
                """.format(schema, lon, lat, soilProfile_lines)
            cur.execute(query)
            con.commit()
            allProfile = False
            soilProfile_lines = [line]
    cur.close()
    con.close()



