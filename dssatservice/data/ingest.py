"""
This module includes functions to ingest data in the database. It integrates the
download, transform, and ingestion process.
"""
from datetime import datetime, timedelta
import os 
import shutil
import sys
# sys.path.append("..")
import dssatservice.database as db
import psycopg2 as pg

from . import download
from . import transform
from tqdm import tqdm

import rasterio as rio
import pandas as pd
import numpy as np
import logging


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
    logger = logging.getLogger("cdsapi")
    date = datetime(date.year, date.month, date.day)
    for var, ncvar in VARIABLES_ERA5_NC.items():      
        try:
            nc_path = download.download_era5(date, var, bbox)
            tiff_path = transform.nc_to_tiff(ncvar, date, nc_path)
            table = f"era5_{var}"
            # Delete rasters if exists
            db.delete_rasters(dbname, schema, table, date)
            db.tiff_to_db(tiff_path, dbname, schema, table, date)
            os.remove(nc_path)
            os.remove(tiff_path)
            logger.info(
                f"\nERA5 INGEST: {date.date()} {ncvar} for {schema} ingested\n"
            )
        except Exception as e:
            if "Your request has not produced a valid" in str(e):
                logger.info(
                    f"\nERA5 INGEST: {date.date()} {ncvar} for {schema} failed. " +\
                    "There is no data matching the request.\n"
                )
            else:
                raise
        

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
        
def ingest_nmme_rain(dbname:str, schema:str, ens:int):
    logger = logging.getLogger(__name__)
    if not db.table_exists(dbname, schema, f"nmme_rain"):
         db._create_climate_forecast_table(dbname, schema, f"nmme_rain")
    con = db.connect(dbname)
    cur = con.cursor()
    bbox = db.get_envelope(dbname, schema)
    
    # Get the reference geotransform raster from the climatology raster
    where = f"\"month\"=1 AND variable=\\'tmean_mean\\'"
    ref_rast = "/tmp/refrast.tiff"
    transform.db_to_tiff(dbname, schema, "era5_clim", where, ref_rast)
    
    # Download forecast
    variable = "Precipitation"
    folder, files = download.download_nmme(
        variable, ens, bbox, geotrans_ref=ref_rast
    )
    files_dict = {
        datetime.strptime(f.split(".")[0], "%Y%m%d"): f
        for f in sorted(files)
    } 
    
    # Save to DB
    table = f"nmme_rain"
    for date, file in files_dict.items():
        file_path = os.path.join(folder, f"gtr{file}")
        # Delete rasters if exists
        where = "fdate='{0}' AND ens={1}".format(date.strftime("%Y-%m-%d"), ens)
        db.delete_rasters(dbname, schema, table, where=where)
        db.tiff_to_db(file_path, dbname, schema, table, date, ens=ens)
        logger.info(
            f"\nNMME INGEST: {date.date()} nmme_rain ens {ens} for {schema} ingested\n"
        )         
    shutil.rmtree(folder)

def ingest_nmme_temp(dbname:str, schema:str, ens:int):
    """
    Ingest nmme temperature data. For that it conducts the next steps:
        1. Download and geotransform nmme data
        2. Calculate monthly bias based on the climatology in era5_clim table
        3. Adjust daily Tmean series using the monthly bias
        4. Estimates Tmax and Tmean using the monthly average Trange.
        5. Saves Tmax and Tmin in the DB
    """
    logger = logging.getLogger(__name__)
    variables = ["tmin", "tmax"]
    for var in variables:
        if not db.table_exists(dbname, schema, f"nmme_{var}"):
            db._create_climate_forecast_table(dbname, schema, f"nmme_{var}")
    con = db.connect(dbname)
    cur = con.cursor()
    bbox = db.get_envelope(dbname, schema)
    
    # Get the reference geotransform raster from the climatology raster
    where = f"\"month\"=1 AND variable=\\'tmean_mean\\'"
    ref_rast = "/tmp/refrast.tiff"
    transform.db_to_tiff(dbname, schema, "era5_clim", where, ref_rast)
    
    # Download forecast
    variable = "Temperature"
    folder, files = download.download_nmme(
        variable, ens, bbox, geotrans_ref=ref_rast
    )
    files_dict = {
        datetime.strptime(f.split(".")[0], "%Y%m%d"): f
        for f in sorted(files)
    } 
    
    # Create the monthly bias raster
    months = set([d.month for d in files_dict.keys()])
    for month in months:
        forecast_avg_path = os.path.join(folder, f"Tavg{month}.tiff")
        tiff_list = [
            os.path.join(folder, f"gtr{f}")
            for d, f in files_dict.items()
            if d.month == month
        ]
        # Forecast monthly average
        transform.tiff_union(tiff_list, forecast_avg_path)
        # Climatology monthly average
        ref_rast = "/tmp/refrast.tiff"
        where = f"\"month\"={month} AND variable=\\'tmean_mean\\'"
        transform.db_to_tiff(dbname, schema, "era5_clim", where, ref_rast)
        # Monthly bias
        bias_path = os.path.join(folder, f"bias{month}.tiff")
        transform.rast_calc(
            A=ref_rast, B=forecast_avg_path, calc="A-B", 
            outfile=bias_path
        )
        
    # Create Tmax and Tmin from Tmean and Trange
    months = set([d.month for d in files_dict.keys()])
    for month in months:
        trange_path = "/tmp/tmprast_trange.tiff"
        where = f"\"month\"={month} AND variable=\\'trange_mean\\'"
        transform.db_to_tiff(dbname, schema, "era5_clim", where, trange_path)
        files_month = {
            d: f for d, f in files_dict.items()
            if d.month == month
        } 
        for date, file in files_month.items():
            file_path = os.path.join(folder, f"gtr{file}")
            # Bias adjust Tmean
            bias_path = os.path.join(folder, f"bias{date.month}.tiff")
            biasadj_path = os.path.join(folder, f"adj{file}")
            transform.rast_calc(
                A=file_path, B=bias_path, calc="A+B", 
                outfile=biasadj_path
            )
            # Create Tmin
            tmin_path = os.path.join(folder, f"tmin{file}")
            transform.rast_calc(
                A=biasadj_path, B=trange_path, calc="A-0.5*B", 
                outfile=tmin_path
            )
            # Create Tmax
            tmax_path = os.path.join(folder, f"tmax{file}")
            transform.rast_calc(
                A=biasadj_path, B=trange_path, calc="A+0.5*B", 
                outfile=tmax_path
            )
    
    # Save Tmax and Tmin to DB
    for date, file in files_dict.items():
        for var in ["tmin", "tmax"]:
            table = f"nmme_{var}"
            file_path = os.path.join(folder, f"{var}{file}")
            where = "fdate='{0}' AND ens={1}".format(date.strftime("%Y-%m-%d"), ens)
            db.delete_rasters(dbname, schema, table, where=where)
            db.tiff_to_db(file_path, dbname, schema, table, date, ens=ens)
            logger.info(
                f"\nNMME INGEST: {date.date()} {table} ens {ens} for {schema} ingested\n"
            )         
    shutil.rmtree(folder)
    
def ingest_nmme(dbname:str, schema:str):
    for e in range(1, 11):
        ingest_nmme_temp(dbname, schema, e)
        ingest_nmme_rain(dbname, schema, e)

def calculate_climatology(dbname:str, schema:str):
    table = "era5_clim"
    assert not db.table_exists(dbname, schema, table), \
        f"{schema}.{table} exists. Drop the table before running this function  "
    db._create_climatology_table(dbname, schema)
    ds = "era5"
    months = range(1, 13)
    con = db.connect(dbname)
    cur = con.cursor()
    variables = ["tmax", "tmin"]
    for month in months:
        for var in variables:
            table = f"{ds}_{var}"
            agg = 'mean'
            variable = f"{var}_{agg}"
            rast_query = """
                SELECT ST_Union(rast, '{3}') as rast FROM {0}.{1}
                WHERE
                    date_part('month', fdate)={2}
            """.format(schema, table, month, agg.upper())
            sql = """
                WITH agg As ({4})
                INSERT INTO {0}.{1} ("month", variable, rast)(
                    SELECT {2}, '{3}', agg.rast FROM agg
                )
                    
            ;""".format(schema, f"{ds}_clim", month, variable, rast_query)
            cur.execute(sql)
            con.commit()
            print(f"{var} {month} row created")
        # Mean temperature raster
        rast_query = """
            SELECT ST_Union(rast, 'MEAN') as rast FROM {0}.{1}
            WHERE
                "month"={2}
        """.format(schema, f"{ds}_clim", month)
        sql = """
            WITH temps As ({4})
            INSERT INTO {0}.{1} ("month", variable, rast)(
                SELECT {2}, '{3}', temps.rast FROM temps
            )
                
        ;""".format(schema, f"{ds}_clim", month, "tmean_mean", rast_query)
        cur.execute(sql)
        con.commit()
        print(f"tmean {month} row created")
        # Temperature range raster
        rast_query = """
            -- Time series of temperature range for that month
            WITH merged As (
                SELECT * FROM {0}.era5_tmax
                    WHERE date_part('month', fdate)={1}
                UNION ALL
                (SELECT * FROM {0}.era5_tmin
                    WHERE date_part('month', fdate)={1})
            )
            SELECT fdate, ST_Union(rast, 'RANGE') as rast FROM merged
            GROUP BY fdate
        """.format(schema, month)
        sql = """
            WITH Trange As ({4})
            INSERT INTO {0}.{1} ("month", variable, rast)(  
                SELECT {2}, '{3}', ST_Union(rast, 'MEAN') FROM Trange
            )
        ;""".format(schema, f"{ds}_clim", month, "trange_mean", rast_query)
        cur.execute(sql)
        con.commit()
        print(f"trange_mean {month} row created")
        sql = """
            WITH Trange As ({4})
            INSERT INTO {0}.{1} ("month", variable, rast)(  
                SELECT {2}, '{3}', ST_Union(rast, 'RANGE') FROM Trange
            )
        ;""".format(schema, f"{ds}_clim", month, "trange_range", rast_query)
        cur.execute(sql)
        con.commit()
        print(f"trange_range {month} row created")
    cur.close()
    con.close()
            
        
        
    