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
from ftplib import FTP
import zipfile
import tempfile

VARIABLES_ERA5_NC = db.VARIABLES_ERA5_NC
VARIABLES_PRISM = db.VARIABLES_PRISM

def ingest_era5_record(con:pg.extensions.connection, schema:str, date:datetime):
    """
    Add a row to each ERA5 table: rain, tmax, tmin, and srad. Given a schema (country),
    it will download, process, and ingest the data for that schema. The country must 
    be already created in the database. The data extent is defined by the geometry
    in the COUNTRY.admin table.
    """
    schema = schema.lower()
    # Check admin shapefile is in the db
    assert db.table_exists(con, schema, "admin"), \
        f"{schema}.admin does not exists. Make sure to add it using " +\
        "the add_country function"

    # Get the envelope for that region
    bbox = db.get_envelope(con, schema)
    logger = logging.getLogger("cdsapi")
    date = datetime(date.year, date.month, date.day)
    for var, ncvar in VARIABLES_ERA5_NC.items():      
        try:
            nc_path = download.download_era5(date, var, bbox)
            tiff_path = transform.nc_to_tiff(ncvar, date, nc_path)
            table = f"era5_{var}"
            # Delete rasters if exists
            db.delete_rasters(con, schema, table, date)

            db.tiff_to_db(tiff_path, con, schema, table, date)
            os.remove(nc_path)
            os.remove(tiff_path)
            logger.info(
                f"\nERA5 INGEST: {date.date()} {ncvar} for {schema} ingested\n"
            )
        except Exception as e:
            if "Request has not produced a valid combination" in str(e):
                logger.info(
                    f"\nERA5 INGEST: {date.date()} {ncvar} for {schema} failed. " +\
                    "There is no data matching the request.\n"
                )
            else:
                raise
        

def ingest_era5_series(con:pg.extensions.connection, 
                       schema:str, datefrom:datetime, dateto:datetime):
    """
    Ingest data for the requested schema, from the specified to the specified
    dates. It ingests all four ERA5 variables needed to run the model
    """
    date = datefrom 
    while date <= dateto:
        ingest_era5_record(con, schema, date)
        date += timedelta(days=1)

def ingest_soil(con:pg.extensions.connection, schema:str, soilfile:str, 
                mask1:str=None, mask2:str=None):
    """
    Ingests soil in the database. It will create one row per soil profile. Each
    soil profile will contain a flag for two crop masks.

    Parameters
    ----------
    con: pg.extensions.connection
        Database connection
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
    if mask1:
        ds_mask1 = rio.open(mask1)
    if mask2:
        ds_mask2 = rio.open(mask2)

    allProfile = False
    soilProfile_lines = []

    # con = db.connect(dbname)
    cur = con.cursor()
    if not db.table_exists(con, schema, f"soil"):
        db._create_soil_table(con, schema)

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
            if mask1:
                mask1_value = ds_mask1.sample([(lon, lat)]).__next__()
                mask1_value = repr(any(mask1_value)).upper()
            else: 
                mask1_value = 'TRUE'
            if mask2:
                mask2_value = ds_mask2.sample([(lon, lat)]).__next__()
                mask2_value = repr(any(mask2_value)).upper()
            else:
                mask2_value = 'TRUE'
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
    # con.close()

def ingest_static(con:pg.extensions.connection, schema:str, rast:str, 
                  parname:str):
    """
    Ingests static raster data. This data includes any static data exluding soils.
    It is open to include any static data such as crop masks, planting dates, etc.
    It was originally created to ingest TAV and TAMP soil parameters for DSSAT.

    Parameters
    ----------
    con: pg.extensions.connection
        Dataase connection
    schema: str
        Name of the schema (domain or country)
    rast: str
        Path to the raster to ingest.
    parname: str
        Name of the static variable to ingest. Up to 32 characters
    """

    if not db.table_exists(con, schema, "static"):
        db._create_static_table(con, schema)
    assert not db.verify_static_par_exists(con, schema, parname), \
        f"{parname} already in static table. Remove it before ingesting it back"
    
    db.tiff_to_db(
        tiffpath=rast,
        con=con,
        schema=schema,
        table="static",
        par=parname
    )
    
def ingest_cultivars(con:pg.extensions.connection, schema:str, csv:str):
    """
    Ingest cultivar data. The data to ingest must be in a csv with the next
    columns: admin1, maturity_type (cultivar maturity type), season_length 
    (average season lenght), cultivar (DSSAT cultivar code)
    """
    # con = db.connect(dbname)
    cur = con.cursor()
    df = pd.read_csv(csv)
    for _, row in df.iterrows():
        query = """
            INSERT INTO {0}.cultivar_options(
                admin1, cultivar, maturity_type, season_length
                ) 
            VALUES (%s, %s, %s, %s);
            """.format(schema)
        cur.execute(
            query, 
            (row.admin1, row.cultivar, row.maturity_type, int(row.season_length))
        )
        con.commit()
    cur.close()
        
def ingest_baseline_pars(con:pg.extensions.connection, schema:str, csv:str):
    """
    NOT IMPLEMENTED, it was part of former stages of the service.
    Ingest baseline parameters. 
    """
    if not db.table_exists(con, schema, "baseline_pars"):
        db._create_baseline_pars_table(con, schema)
    # con = db.connect(dbname)
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
    cur.close()

def ingest_baseline_run(con:pg.extensions.connection, schema:str, csv:str):
    """
    NOT IMPLEMENTED, it was part of former stages of the service.
    Ingest baseline run. 
    """
    if not db.table_exists(con, schema, "baseline_run"):
        db._create_baseline_run_table(con, schema)
    # con = db.connect(con)
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
    cur.close()
        
def ingest_nmme_rain(con:pg.extensions.connection, schema:str, ens:int,
                     weather_table:str='era5'):
    """
    Ingest the NMME rain data
    """
    logger = logging.getLogger(__name__)
    if not db.table_exists(con, schema, f"nmme_rain"):
         db._create_climate_forecast_table(con, schema, f"nmme_rain")

    bbox = db.get_envelope(con, schema, pad=1.)
    
    # Get the reference geotransform raster from the climatology raster
    where = f"\"month\"=1 AND variable=\\'tmean_mean\\'"
    ref_rast = "/tmp/refrast.tiff"
    transform.db_to_tiff(con, schema, f"{weather_table}_clim", where, ref_rast)
    
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
        db.delete_rasters(con, schema, table, where=where)
        db.tiff_to_db(file_path, con, schema, table, date, ens=ens)
        logger.info(
            f"\nNMME INGEST: {date.date()} nmme_rain ens {ens} for {schema} ingested\n"
        )         
    shutil.rmtree(folder)

def ingest_nmme_temp(con:pg.extensions.connection, schema:str, ens:int,
                     weather_table:str='era5'):
    """
    Ingest nmme temperature data. For that it conducts the next steps:
        1. Download and geotransform nmme data
        2. Calculate monthly bias based on the climatology in weather_table 
         climatology table
        3. Adjust daily Tmean series using the monthly bias
        4. Estimates Tmax and Tmean using the monthly average Trange.
        5. Saves Tmax and Tmin in the DB
    """
    logger = logging.getLogger(__name__)
    variables = ["tmin", "tmax"]
    for var in variables:
        if not db.table_exists(con, schema, f"nmme_{var}"):
            db._create_climate_forecast_table(con, schema, f"nmme_{var}")

    bbox = db.get_envelope(con, schema, pad=1.)
    
    # Get the reference geotransform raster from the climatology raster
    where = f"\"month\"=1 AND variable=\\'tmean_mean\\'"
    ref_rast = "/tmp/refrast.tiff"
    transform.db_to_tiff(con, schema, f"{weather_table}_clim", where, ref_rast)
    
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
        transform.db_to_tiff(con, schema, f"{weather_table}_clim", where, ref_rast)
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
        transform.db_to_tiff(con, schema, f"{weather_table}_clim", where, trange_path)
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
            db.delete_rasters(con, schema, table, where=where)
            db.tiff_to_db(file_path, con, schema, table, date, ens=ens)
            logger.info(
                f"\nNMME INGEST: {date.date()} {table} ens {ens} for {schema} ingested\n"
            )         
    shutil.rmtree(folder)
    
def ingest_nmme(con:pg.extensions.connection, schema:str, 
                weather_table:str="era5"):
    """
    Ingest the NMME Rain and temperature data
    """
    for e in range(1, 11):
        ingest_nmme_temp(con, schema, e, weather_table)
        ingest_nmme_rain(con, schema, e, weather_table)

def calculate_climatology(con:pg.extensions.connection, schema:str, 
                          weather_table:str='era5'):
    """
    It calculates and ingests the climatology using the available reanalysis 
    data in the Weather tables.
    """
    table = f"{weather_table}_clim"
    assert not db.table_exists(con, schema, table), \
        f"{schema}.{table} exists. Drop the table before running this function  "
    db._create_climatology_table(con, schema)
    ds = weather_table
    months = range(1, 13)
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
                SELECT * FROM {0}.{2}_tmax
                    WHERE date_part('month', fdate)={1}
                UNION ALL
                (SELECT * FROM {0}.{2}_tmin
                    WHERE date_part('month', fdate)={1})
            )
            SELECT fdate, ST_Union(rast, 'RANGE') as rast FROM merged
            GROUP BY fdate
        """.format(schema, month, ds)
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


def ingest_prism_series(con:pg.extensions.connection, 
                       schema:str, datefrom:datetime, dateto:datetime):
    """
    Ingest PRISM data for the requested schema, from the specified to the specified
    dates. It ingests all four PRISM variables needed to run the model. PRISM does 
    not have solar radiation data, then ERA5 SRAD data is ingested.
    """
    schema = schema.lower()
    # Check admin shapefile is in the db
    assert db.table_exists(con, schema, "admin"), \
        f"{schema}.admin does not exists. Make sure to add it using " +\
        "the add_country function"
    for var in VARIABLES_PRISM.keys():
        if not db.table_exists(con, schema, f"prism_{var}"):
            db._create_reanalysis_table(con, schema, f"prism_{var}")
    if not db.table_exists(con, schema, f"prism_srad"):
        db._create_reanalysis_table(con, schema, f"prism_srad")
    # Get the envelope for that region
    bbox = db.get_envelope(con, schema)
    logger = logging.getLogger(__name__)

    url = "prism.oregonstate.edu"
    ftp = FTP(url)
    ftp.login()

    # Map all dates to their files
    date_range = pd.date_range(datefrom, dateto)
    years = set([d.year for d in date_range])

    url_map = {}
    for var, pvar in VARIABLES_PRISM.items():
        url_map[var] = []
        for year in years:
            ftp.cwd(f"daily/{pvar}/{year}")
            url_map[var] += ftp.nlst()
            ftp.cwd(f"../../../")
        url_map[var] = {
            i.split('_')[-2]: f"{i}" 
            for i in url_map[var]
        }
    download_srad = True
    for date in date_range: 
        for var, pvar in VARIABLES_PRISM.items():
            ftp.cwd(f'daily/{pvar}/{date.year}/')
            filename = url_map[var].get(date.strftime('%Y%m%d'))
            tmpfolder = tempfile.mkdtemp()
            if not filename:
                logger.info(
                    f"\nPRISM INGEST: {date.date()} {pvar} for {schema} failed. " +\
                    "There is no data matching the request.\n"
                )
                download_srad = False
            else:
                file_path = download.download_prism(ftp, filename, tmpfolder)
                if file_path.endswith("zip"):
                    fz = zipfile.ZipFile(file_path)
                    bil_path = filter(
                        lambda s: s.endswith("bil"), fz.namelist()
                    ).__next__()
                    bil_path = os.path.join(tmpfolder, bil_path)
                    fz.extractall(tmpfolder)
                else:
                    bil_path = file_path
                tiff_path = bil_path.replace('.bil', '.tif')
                # Translate raster
                transform.translate_raster(bil_path, tiff_path, bbox)
                # Delete rasters if exists
                table = f"prism_{var}"
                db.delete_rasters(con, schema, table, date)
                db.tiff_to_db(tiff_path, con, schema, table, date)
                shutil.rmtree(tmpfolder)
                logger.info(
                    f"\nPRISM INGEST: {date.date()} {pvar} for {schema} ingested\n"
                )
                print(f"\nPRISM INGEST: {date.date()} {pvar} for {schema} ingested\n")
                download_srad = True
            ftp.cwd(f'../../../')
        if download_srad:
            nc_path = download.download_era5(date, 'srad', bbox)
            tiff_path = transform.nc_to_tiff(
                VARIABLES_ERA5_NC['srad'], date, nc_path
            )
            table = f"prism_srad"
            # Delete rasters if exists
            db.delete_rasters(con, schema, table, date)
            db.tiff_to_db(tiff_path, con, schema, table, date)
            logger.info(
                f"\nPRISM INGEST: {date.date()} {pvar} for {schema} ingested\n"
            )
            os.remove(nc_path)
            os.remove(tiff_path)
    ftp.close()