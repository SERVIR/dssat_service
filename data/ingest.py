"""
This module includes functions to ingest data in the database. It integrates the
download, transform, and ingestion process.
"""
from datetime import datetime, timedelta
import os 

from . import database as db
from . import download
from .import transform


VARIABLES_ERA5_NC = {
    "tmax": "Temperature_Air_2m_Max_24h",
    "tmin": "Temperature_Air_2m_Min_24h",
    "rain": "Precipitation_Flux",
    "srad": "Solar_Radiation_Flux",
    # "wind": "Wind_Speed_10m_Mean",
    # "tdew": "Dew_Point_Temperature_2m_Mean"   
}


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
    # Check if era5 tables already exists
    for var in VARIABLES_ERA5_NC.keys():
        if not db.table_exists(dbname, schema, f"era5_{var}"):
            db.create_reanalysis_table(dbname, schema, f"era5_{var}")

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
    Ingest date for the requested schema, from the specified to the specified
    dates
    """
    date = datefrom 
    while date <= dateto:
        ingest_era5_record(dbname, schema, date)
        date += timedelta(days=1)

    




