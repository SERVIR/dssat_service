"""
It contains most of the functions to create schemas, tables, and get data from
the database.
"""
import psycopg2 as pg
from sqlalchemy import create_engine
import geopandas as gpd
from pandas import date_range, Series, DataFrame
import numpy as np

import warnings
import tempfile
import subprocess
import os
from datetime import datetime
import random
import string


VARIABLES_ERA5_NC = {
    "tmax": "Temperature_Air_2m_Max_24h",
    "tmin": "Temperature_Air_2m_Min_24h",
    "rain": "Precipitation_Flux",
    "srad": "Solar_Radiation_Flux",
    # "wind": "Wind_Speed_10m_Mean",
    # "tdew": "Dew_Point_Temperature_2m_Mean"   
}
VARIABLES_PRISM = {
    'rain': 'ppt', 'tmax': 'tmax', 'tmin': 'tmin',
}

TMP = tempfile.gettempdir()

def connect(dbname):
    """
    Retuns a connection. If dbname is a connection then it returns dbname. If not,
    then it tries to return a local connection to dbname
    """
    if isinstance(dbname, pg.extensions.connection):
        return dbname
    else:
        con = pg.connect(database=dbname)
        return con

def create_schema(con, schema):
    """
    Creates a new schema. There is one schema per domain (country)
    """
    # con = connect(dbname)
    cur = con.cursor()
    query = """
        CREATE SCHEMA {0};
        """.format(schema)
    cur.execute(query)
    con.commit()
    cur.close()
    # con.close()
    return

def _create_reanalysis_table(con, schema, table):
    """
    Creates a renalysis table for the schema and variable (table) specified.
    """
    # con = connect(dbname)
    cur = con.cursor()
    query = """
        CREATE TABLE {0}.{1} (
            rast raster NOT NULL, 
            fdate date NOT NULL,
            rid serial NOT NULL
        );
        """.format(schema, table)
    cur.execute(query)
    query = """
        CREATE INDEX {1}_time ON {0}.{1} (fdate);
        """.format(schema, table)
    cur.execute(query)
    query = """
        CREATE INDEX {1}_rid ON {0}.{1} (rid);
        """.format(schema, table)
    cur.execute(query)
    query = """
        CREATE INDEX {1}_spatial ON {0}.{1} USING GIST (ST_Envelope(rast));
        """.format(schema, table)
    cur.execute(query)
    con.commit()
    cur.close()
    # con.close()
    return 

def _create_climate_forecast_table(con, schema, table):
    """
    Creates a climate forecast table for the schema and variable (table) 
    specified
    """
    # con = connect(dbname)
    cur = con.cursor()
    query = """
        CREATE TABLE {0}.{1} (
            rast raster NOT NULL, 
            fdate date NOT NULL,
            rid serial NOT NULL,
            ens integer NOT NULL
        );
        """.format(schema, table)
    cur.execute(query)
    query = """
        CREATE INDEX {1}_time ON {0}.{1} (fdate);
        """.format(schema, table)
    cur.execute(query)
    query = """
        CREATE INDEX {1}_rid ON {0}.{1} (rid);
        """.format(schema, table)
    cur.execute(query)
    query = """
        CREATE INDEX {1}_ens ON {0}.{1} (ens);
        """.format(schema, table)
    cur.execute(query)
    query = """
        CREATE INDEX {1}_spatial ON {0}.{1} USING GIST (ST_Envelope(rast));
        """.format(schema, table)
    cur.execute(query)
    con.commit()
    cur.close()
    # con.close()
    return 

def _create_static_table(con, schema):
    """
    Creates table for static rasters
    """
    # con = connect(dbname)
    cur = con.cursor()
    query = """
        CREATE TABLE {0}.static (
            rast raster NOT NULL, 
            rid serial NOT NULL,
            par character(32)
        );
        """.format(schema)
    cur.execute(query)
    query = """
        CREATE INDEX static_par ON {0}.static (par);
        """.format(schema)
    cur.execute(query)
    query = """
        CREATE INDEX static_rid ON {0}.static (rid);
        """.format(schema)
    cur.execute(query)
    query = """
        CREATE INDEX static_spatial ON {0}.static USING GIST (ST_Envelope(rast));
        """.format(schema)
    cur.execute(query)
    con.commit()
    cur.close()
    # con.close()
    return 

def _create_soil_table(con, schema):
    """
    Creates soil table
    """
    table = "soil"
    # con = connect(dbname)
    cur = con.cursor()
    query = """
        CREATE TABLE {0}.{1} (
            gid serial PRIMARY KEY, 
            geom geometry (POINT, 4326) UNIQUE,
            mask1 boolean,
            mask2 boolean,
            soil text
        );
        """.format(schema, table)
    cur.execute(query)
    query = """
        CREATE INDEX {1}_mask1 ON {0}.{1} (mask1);
        """.format(schema, table)
    cur.execute(query)
    query = """
        CREATE INDEX {1}_mask2 ON {0}.{1} (mask2);
        """.format(schema, table)
    cur.execute(query)
    query = """
        CREATE INDEX {1}_spatial ON {0}.{1} USING GIST (geom);
        """.format(schema, table)
    cur.execute(query)
    con.commit()
    cur.close()
    # con.close()
    return 

def _create_cultivars_table(con, schema):
    """
    Creates the cultivar_options table
    """
    table = "cultivar_options"
    # con = connect(dbname)
    cur = con.cursor()
    query = """
        CREATE TABLE {0}.{1} (
            id serial PRIMARY KEY,
            admin1 text,
            cultivar char(6),
            maturity_type text,
            season_length int
        );
        """.format(schema, table)
    cur.execute(query)
    query = """
        CREATE INDEX {1}_admin1 ON {0}.{1} (admin1);
        """.format(schema, table)
    cur.execute(query)
    query = """
        CREATE INDEX {1}_matType ON {0}.{1} (maturity_type);
        """.format(schema, table)
    cur.execute(query)
    con.commit()
    cur.close()
    # con.close()
    return

def _create_baseline_pars_table(con, schema):
    """
    NOT IMPLEMENTED, it was part of former stages of the service.
    """
    table = "baseline_pars"
    # con = connect(dbname)
    cur = con.cursor()
    query = """
        CREATE TABLE {0}.{1} (
            id serial PRIMARY KEY,
            admin1 text,
            cultivar char(6),
            planting_month int,
            nitrogen float,
            crps float8,
            rpss float8
        );
        """.format(schema, table)
    cur.execute(query)
    query = """
        CREATE INDEX {1}_admin1 ON {0}.{1} (admin1);
        """.format(schema, table)
    cur.execute(query)
    con.commit()
    cur.close()
    # con.close()
    return

def _create_baseline_run_table(con, schema):
    """
    NOT IMPLEMENTED, it was part of former stages of the service.
    """
    table = "baseline_run"
    # con = connect(dbname)
    cur = con.cursor()
    query = """
        CREATE TABLE {0}.{1} (
            id serial PRIMARY KEY,
            admin1 text,
            harwt float,
            obs float,
            year int
        );
        """.format(schema, table)
    cur.execute(query)
    query = """
        CREATE INDEX {1}_admin1 ON {0}.{1} (admin1);
        """.format(schema, table)
    cur.execute(query)
    query = """
        CREATE INDEX {1}_year ON {0}.{1} (year);
        """.format(schema, table)
    cur.execute(query)
    con.commit()
    cur.close()
    # con.close()
    return

def _create_climatology_table(con, schema, weather_table='era5'):
    """
    Creates a table for the monthly climatology.
    """
    ds = weather_table
    # con = connect(dbname)
    cur = con.cursor()
    query = """
        CREATE TABLE {0}.{1}_clim (
            rast raster NOT NULL, 
            rid serial NOT NULL,
            variable character(32),
            month integer
        );
        """.format(schema, ds)
    cur.execute(query)
    query = """
        CREATE INDEX {1}_clim_var ON {0}.{1}_clim (variable);
        """.format(schema, ds)
    cur.execute(query)
    query = """
        CREATE INDEX {1}_clim_rid ON {0}.{1}_clim (rid);
        """.format(schema, ds)
    cur.execute(query)
    query = """
        CREATE INDEX {1}_clim_month ON {0}.{1}_clim (month);
        """.format(schema, ds)
    cur.execute(query)
    con.commit()
    cur.close()
    # con.close()


def schema_exists(con, schema):
    """
    Check if schema exists in database.
    """
    # con = connect(dbname)
    cur = con.cursor()
    query = """
        SELECT * FROM information_schema.schemata 
        WHERE 
            schema_name='{0}';
        """.format(schema)
    cur.execute(query)
    schema_exists = bool(cur.rowcount)
    cur.close()
    # con.close()
    return schema_exists 

def table_exists(con, schema, table):
    """
    Check if table exists in the database and schema.
    """
    # con = connect(dbname)
    cur = con.cursor()
    query = """
        SELECT * FROM information_schema.tables 
        WHERE 
            table_schema='{0}' AND table_name='{1}';
        """.format(schema.lower(), table.lower())
    cur.execute(query)
    table_exists = bool(cur.rowcount)
    cur.close()
    return table_exists

def add_country(con:pg.extensions.connection, name:str, shapefile:str, 
                admin1:str="admin1"):
    """
    Add a country to the database. It will create a new schema and all of the 
    tables empty tables it'll need to support the service. The funciton loads
    the shp into the name.admin table, and it simplifies the geometries.
    
    Parameters
    ----------
    con: pg.extensions.connection 
    
    name: str 
        Name of the country 
    shapefile: str 
        Path to the shapefile with the administrative divions of the country.
        The shapefile must contain a valid geometry, and at least a "admin_1"
        field to indicate the name of the subdivisions.
    admin1: str 
        Name of the admin1 division.
    """
    gdf = gpd.read_file(shapefile)
    assert admin1 in gdf.columns, f"{admin1} column not in shapefile"
    gdf = gdf.rename(columns={admin1: "admin1"})
    gdf["geometry"] = gdf.geometry.simplify(0.01)

    tmp_dir = tempfile.TemporaryDirectory()
    tmp_shp = os.path.join(tmp_dir.name, "file.shp")
    gdf.to_file(tmp_shp, crs=4326)

    if table_exists(con, name, "admin"):
        warnings.warn(f"{name}.admin exists, it will be overwriten")
    else:
        create_schema(con, name)
    cur = con.cursor()
    cur.execute("SELECT current_database()")
    dbname = cur.fetchall()[0][0]
    cmd = f"shp2pgsql -d -s 4326 {tmp_shp} {name}.admin | psql -d {dbname}"      # TODO: This won't work when there is a remote connection. 
    proc = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, 
        stderr=subprocess.STDOUT
    )
    out, err = proc.communicate()
    tmp_dir.cleanup()

    # Create a materialized view for the envelope
    # con = connect(dbname)
   
    query = """
        CREATE MATERIALIZED VIEW {0}.bbox
        AS (
            SELECT ST_AsText(ST_Envelope(ST_Union(geom))) AS bbox 
            FROM {0}.admin
        );
        """.format(name)
    cur.execute(query)
    con.commit()

    # Create tables
    for var in VARIABLES_ERA5_NC.keys():
        if not table_exists(con, name, f"era5_{var}"):
            _create_reanalysis_table(con, name, f"era5_{var}")
    if not table_exists(con, name, f"soil"):
        _create_soil_table(con, name)
    if not table_exists(con, name, f"cultivars"):
        _create_cultivars_table(con, name)
    cur.close()
    # con.close()
    return

def get_envelope(con:pg.extensions.connection, schema:str, pad=0.1):
    """
    Get the envelope for the region. Envelope was previously created
    as a materialized view.
    """
    # con = connect(dbname)
    cur = con.cursor()
    query = "SELECT * from {0}.bbox;".format(schema)
    cur.execute(query)
    out = cur.fetchall()
    cur.close()
    # con.close()
    bbox = out[0][0]
    bbox = bbox.split("((")[-1].split("))")[0]
    bbox = list(map(lambda x: x.split(), bbox.split(",")))
    bbox = [bbox[1][1], bbox[0][0], bbox[0][1], bbox[2][0]]
    bbox = list(map(float, bbox))
    bbox = [bbox[0]+pad, bbox[1]-pad, bbox[2]-pad, bbox[3]+pad]
    return bbox

def delete_rasters(con, schema, table, date=None, where=None):
    """
    If date already exists delete associated rasters before ingesting.
    """
    # con = connect(dbname)
    cur = con.cursor()
    if (where is None):
        where = "fdate='{0}'".format(date.strftime('%Y-%m-%d'))
    query = """
        SELECT * FROM {0}.{1}
        WHERE
            {2}
        """.format(schema, table, where)
    cur.execute(query)
    if bool(cur.rowcount):
        # TODO: This has to go in a log
        warnings.warn("Overwriting raster in {0}.{1} table for {1}".format(
            schema, table, query.replace('\n', " ")
        ))
        query = """
            DELETE FROM {0}.{1} 
            WHERE
                {2}
            """.format(schema, table, where)
        cur.execute(query)
        con.commit()
    cur.close()

def tiff_to_db(tiffpath:str, con:pg.extensions.connection, schema:str,
               table:str, date:datetime=None, ens:int=None, par:str=None):
    """
    Saves tiff to the database.

    Parameters
    ----------
    tiffpath: str
        Path to the tiff file
    con: pg.extensions.connection
        Database connection
    schema: str
        Schema where the raster will be saved
    table: str
        Table where the raster will be saved
    date: datetime
        Date for the timeseries rasters (Weather).
    ens: int
        Ensemble number. It is used when ensemble-based datasets are used.
    par: str
        Name of the parameter. Only applies for the static rasters.
    """ 
    assert any((date is not None, par is not None)), \
        "date must be set for timeseries data. par must be set for static data" 
    assert not all((date is not None, par is not None)), \
        "par argument only applies for static data. You must not define date for static data." 
    if par is not None:
        assert table == "static", "table must be equal to 'static' for static data."
    # con = connect(dbname)

    temptable = ''.join(
        random.SystemRandom().choice(string.ascii_letters) for _ in range(8)
    )
    
    cur = con.cursor()
    cur.execute("SELECT current_database()")
    dbname = cur.fetchall()[0][0]
    cmd = f"raster2pgsql -d -s 4326 -t 10x10 {tiffpath} {temptable}" + \
        f"| psql -d {dbname}"
    proc = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    out, err = proc.communicate()
    # cur = con.cursor()
    try:
        columns = ["rid", "rast"]
        # TODO: Need to create log
        # Spatial index
        query = """
            CREATE INDEX {0}_rid ON {1} (rid);
            """.format(table, temptable)
        cur.execute(query)
        query = """
            CREATE INDEX {0}_spatial ON {1} USING GIST (ST_Envelope(rast));
            """.format(table, temptable)
        cur.execute(query)
        # Time index
        if date is not None:
            columns.append("fdate")
            query = """
                ALTER TABLE {0} ADD COLUMN fdate DATE
                """.format(temptable)
            cur.execute(query)
            query = """
                UPDATE {0} SET fdate = date '{1}'
                """.format(temptable, date.strftime("%Y-%m-%d"))
            cur.execute(query)
            query = """
                CREATE INDEX {0}_time ON {1} (fdate);
                """.format(table, temptable)
            cur.execute(query)
        # Parameter index
        if par is not None:
            columns.append("par")
            query = """
                ALTER TABLE {0} ADD COLUMN par CHARACTER(32)
                """.format(temptable)
            cur.execute(query)
            query = """
                UPDATE {0} SET par = '{1}'
                """.format(temptable, par)
            cur.execute(query)
            query = """
                CREATE INDEX {0}_par ON {1} (par);
                """.format(table, temptable)
            cur.execute(query)
        if ens is not None:
            columns.append("ens")
            query = """
                ALTER TABLE {0} ADD COLUMN ens integer
                """.format(temptable)
            cur.execute(query)
            query = """
                UPDATE {0} SET ens = {1}
                """.format(temptable, ens)
            cur.execute(query)
            query = """
                CREATE INDEX {0}_ens ON {1} (ens);
                """.format(table, temptable)
            cur.execute(query)
        # Copy to permanent table
        query = """
            INSERT INTO {0}.{1}({2}) (SELECT {2} FROM {3});
            """.format(schema, table, ",".join(columns), temptable)
        cur.execute(query)
        con.commit()
        cur.close()
    finally:
        # con.close() # Closed in case something failed on the try
        # con = connect(dbname)
        cur = con.cursor()
        query = """
            DROP TABLE {0};
            """.format(temptable)
        cur.execute(query)
        con.commit()
        cur.close()
        # con.close()
    return

def verify_static_par_exists(con:pg.extensions.connection, schema:str,
                             parname:str):
    """
    It will raise an error if the static parameter already exists.
    """

    if not table_exists(con, schema, "static"):
        return False
    cur = con.cursor()
    query = """
        SELECT 1 FROM {0}.static WHERE par = '{1}';
        """.format(schema, parname)
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    return len(rows) > 0
        

def verify_series_continuity(con, schema:str, table:str, 
                             datefrom:datetime, dateto:datetime):
    """
    Verify if there is continous record of weather series. It returns the 
    missing dates on the requested period
    """
    dates = date_range(start=datefrom, end=dateto).date
    cur = con.cursor()
    query = """
        SELECT DISTINCT(fdate) FROM {0}.{1}
        WHERE
            fdate>=date '{2}' 
            AND fdate<=date '{3}';
        """.format(schema, table, datefrom.strftime("%Y-%m-%d"), 
                   dateto.strftime("%Y-%m-%d"))
    cur.execute(query)
    dates_db = cur.fetchall()
    dates_db = [row[0] for row in dates_db]
    dates_notin_db = list(filter(lambda x: x not in dates_db, dates))
    cur.close()
    return dates_notin_db
    
def latest_date(con, schema:str, table:str):
    """
    Returns the latest available date in that table
    """
    cur = con.cursor()
    query = f"SELECT max(fdate) FROM {schema}.{table};"
    cur.execute(query)
    dt = cur.fetchall()[0][0]
    cur.close()
    return datetime(dt.year, dt.month, dt.day)
    

def get_era5_for_point(con, schema:str, lon:float, lat:float,
                       datefrom:datetime, dateto:datetime):
    """
    Get the EAR5 weather series for the requested point and time period. It 
    returns a df with the time series for that point.
    """
    for var in VARIABLES_ERA5_NC.keys():
        table = f"era5_{var}"
        dates_notin_db = verify_series_continuity(
            con, schema, table, datefrom, dateto
        )
        assert len(dates_notin_db) == 0, \
            f"Dates are missing in {table}: {dates_notin_db}. Ingest that data first"
    
    variables = list(VARIABLES_ERA5_NC.keys())
    cur = con.cursor()
    df = DataFrame()
    for var in variables:
        query = """
        SELECT fdate, ST_value(ra.rast, pn.pt_geom) AS val
        FROM {0}.era5_{1} AS ra,
            (
                SELECT ST_SetSRID(ST_Point({2}, {3}), 4326) AS pt_geom
            ) AS pn
        WHERE
            ST_Within(pn.pt_geom, ST_Envelope(rast))
            AND fdate>=date '{4}' AND fdate<=date '{5}'
        """.format(schema, var, lon, lat, datefrom.strftime("%Y-%m-%d"),
                   dateto.strftime("%Y-%m-%d"))
        cur.execute(query)
        rows = np.array(cur.fetchall())
        if len(rows) < 1:
            warnings.warn(f"{var} data is NULL at location {lon}, {lat}")
            continue
        df[var] = Series(rows[:, 1], index=rows[:, 0])
    
    cur.close()
    if df.isna().any().any():
        warnings.warn(f"Data is NULL at location {lon}, {lat}")
        return
    return df.sort_index()

def get_prism_for_point(con, schema:str, lon:float, lat:float,
                       datefrom:datetime, dateto:datetime):
    """
    Get the EAR5 weather series for the requested point and time period. It 
    returns a df with the time series for that point.
    """
    for var in VARIABLES_PRISM.keys():
        table = f"prism_{var}"
        dates_notin_db = verify_series_continuity(
            con, schema, table, datefrom, dateto
        )
        assert len(dates_notin_db) == 0, \
            f"Dates are missing in {table}: {dates_notin_db}. Ingest that data first"
    
    variables = list(VARIABLES_PRISM.keys())
    cur = con.cursor()
    df = DataFrame()
    for var in variables:
        query = """
        SELECT fdate, ST_value(ra.rast, pn.pt_geom) AS val
        FROM {0}.prism_{1} AS ra,
            (
                SELECT ST_SetSRID(ST_Point({2}, {3}), 4326) AS pt_geom
            ) AS pn
        WHERE
            ST_Within(pn.pt_geom, ST_Envelope(rast))
            AND fdate>=date '{4}' AND fdate<=date '{5}'
        """.format(schema, var, lon, lat, datefrom.strftime("%Y-%m-%d"),
                   dateto.strftime("%Y-%m-%d"))
        cur.execute(query)
        rows = np.array(cur.fetchall())
        if len(rows) < 1:
            warnings.warn(f"{var} data is NULL at location {lon}, {lat}")
            continue
        df[var] = Series(rows[:, 1], index=rows[:, 0])
    
    cur.close()
    # Temperature to Kelvin (match ERA5 units)
    df['tmax'] += 273.15
    df['tmin'] += 273.15
    if df.isna().any().any():
        warnings.warn(f"Data is NULL at location {lon}, {lat}")
        return
    return df.sort_index()

def get_nmme_for_point(con, schema:str, lon:float, lat:float,
                       datefrom:datetime, dateto:datetime, ens:int):
    """
    Get the NMME weather series for the requested point, ensemble and time 
    period. returns a df with the time series for that point.
    """
    for var in VARIABLES_ERA5_NC.keys():
        if var == "srad":
            continue
        table = f"nmme_{var}"
        dates_notin_db = verify_series_continuity(
            con, schema, table, datefrom, dateto
        )
        assert len(dates_notin_db) == 0, \
            f"Dates are missing in {table}: {dates_notin_db}. Ingest that data first"
    
    # Get rain data. This will also get the rid to make next queries using 
    # rid instead of spatial relations
    variables = ["tmax", "tmin", "rain"]
    var = variables[0]

    cur = con.cursor()
    
    df = DataFrame()
    for var in variables:
        query = """
        SELECT fdate, ST_value(ra.rast, pn.pt_geom) AS val
        FROM {0}.nmme_{1} AS ra,
            (
                SELECT ST_SetSRID(ST_Point({2}, {3}), 4326) AS pt_geom
            ) AS pn
        WHERE
            ST_Within(pn.pt_geom, ST_Envelope(rast))
            AND fdate>=date '{4}' AND fdate<=date '{5}' AND ens={6}
        """.format(schema, var, lon, lat, datefrom.strftime("%Y-%m-%d"),
                   dateto.strftime("%Y-%m-%d"), ens)
        cur.execute(query)
        rows = np.array(cur.fetchall())
        if len(rows) < 1:
            warnings.warn(f"{var} data is NULL at location {lon}, {lat}")
            continue
        df[var] = Series(rows[:, 1], index=rows[:, 0])
    
    cur.close()
    if df.isna().any().any():
        warnings.warn(f"Data is NULL at location {lon}, {lat}")
        return
    return df.sort_index()

def get_soils(con, schema:str, admin1:str, mask:int=None):
    """
    Return the soils for a region (admin1). If mask is 1, then it'll return
    only the soil points included in mask1, and same case if mask is 2 but
    with mask2. If mask is None thenit returns all points. Returns a DataFrame.
    """
    if mask is None:
        mask_query = ""
    else:
        mask_query = f"AND so.mask{mask}=TRUE"
    cur = con.cursor()
    query = """
        SELECT ST_X(so.geom), ST_Y(so.geom), so.soil, so.mask1, so.mask2 
        FROM {0}.soil AS so, {0}.admin AS ad
        WHERE
            ST_Contains(ad.geom, so.geom)
            AND ad.admin1='{1}'
            {2};
        """.format(schema, admin1.replace("'", "''"), mask_query)
        # Single quote is represented as double quotes in the SQL query
    cur.execute(query)
    rows = cur.fetchall()
    df = DataFrame(rows, columns=["lon", "lat", "soil", "mask1", "mask2"])
    cur.close()
    return df

def get_static_par(con, schema:str, lon:float, lat:float, par:str):
    """
    Get a static parameter value for a location
    """
    cur = con.cursor()
    query = """
        SELECT ST_value(ra.rast, pn.pt_geom) AS val
        FROM {0}.static AS ra,
            (
                SELECT ST_SetSRID(ST_Point({1}, {2}), 4326) AS pt_geom
            ) AS pn
        WHERE
            ST_Within(pn.pt_geom, ST_Envelope(rast))
            AND par = '{3}'
        """.format(schema, lon, lat, par)
    cur.execute(query)
    rows = np.array(cur.fetchall())
    cur.close()
    if len(rows) < 1:
        return None 
    else:
        return rows[0][0]
    
def check_admin1_in_country(con, schema, admin1):
    """
    Check if the admin1 unit is on the country geometry table.
    """
    cur = con.cursor()
    query ="""
        SELECT admin1 FROM {0}.admin
        WHERE
            admin1='{1}';
    """.format(schema, admin1.replace("'", "''"))
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    assert len(rows) > 0, f"{admin1} not in {schema} schema"
    assert len(rows) == 1, f"Multiple {admin1} in {schema} schema"
    
    
def fetch_admin1_list(con, schema):
    """
    NOT IMPLEMENTED, it was part of former stages of the service.
    Returns a list of admin units that are set to simulate. I.E. they must have
    a baseline defined in the baseline_pars table. If there is not baseline 
    defined it is because the admin unit is too small to get more than 4 pixels
    from its bound, or because there was not observed data to generate a 
    baseline.
    """
    cur = con.cursor()
    query = """
        SELECT ad.admin1 FROM {0}.admin AS ad 
        INNER JOIN {0}.baseline_pars AS bl
            ON ad.admin1=bl.admin1;
        """.format(schema)
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    return [r[0] for r in rows]

def fetch_baseline_pars(con, schema, admin1):
    """
    NOT IMPLEMENTED, it was part of former stages of the service.
    """
    cur = con.cursor()
    query = """
        SELECT cultivar, planting_month, nitrogen, crps, rpss 
        FROM {0}.baseline_pars
        WHERE
            admin1='{1}';
        """.format(schema, admin1.replace("'", "''"))
    cur.execute(query)
    rows = cur.fetchall()
    assert len(rows) > 0, \
        f"No baseline available for {admin1} in {schema}.baseline_pars"
    assert len(rows) == 1, \
        f"More than one baseline defined for {admin1} in {schema}.baseline_pars"
    pars_dict = {
        "cultivar": rows[0][0], "planting_month": int(rows[0][1]),
        "nitrogen": float(rows[0][2]), "crps": float(rows[0][3]),
        "rpss": float(rows[0][4])
        }
    cur.close()
    return pars_dict

def fetch_baseline_run(con, schema, admin1):
    """
    NOT IMPLEMENTED, it was part of former stages of the service.
    """
    cur = con.cursor()
    query = """
        SELECT year, harwt, obs  
        FROM {0}.baseline_run
        WHERE
            admin1='{1}'
        ;""".format(schema, admin1.replace("'", "''"))
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    assert len(rows) > 0, \
        f"No baseline run available for {admin1} in {schema}.baseline_run"
    return DataFrame(rows, columns=["year", "sim", "obs"])

def fetch_cultivars(con, schema, admin1):
    """
    Returns the cultivar options for the admin1 unit.
    """
    cur = con.cursor()
    query = """
        SELECT cultivar, maturity_type, season_length
        FROM {0}.cultivar_options
        WHERE
            admin1=%s
        ;""".format(schema)
    cur.execute(query, (admin1,))
    rows = cur.fetchall()
    cur.close()
    assert len(rows) > 0, \
        f"No Cultivars in {admin1} - {schema}.baseline_run"
    out_df = DataFrame(
        rows, 
        columns=["cultivar", "maturity_type", "season_length"]
    )
    return out_df
    
def add_latest_forecast(con:pg.extensions.connection, schema:str, geojson:str):
    """
    Add the latest forecast to the DB. The geojson/shp must contain the next 
    columns: admin1, pred_cat, pred, obs_avg, planting_p, ref_period, nitro_rate,
    urea_rate.
    """
    gdf = gpd.read_file(geojson)
    gdf["geometry"] = gdf.geometry.simplify(0.001)

    tmp_dir = tempfile.TemporaryDirectory()
    tmp_shp = os.path.join(tmp_dir.name, "file.shp")
    gdf.to_file(tmp_shp, crs=4326)

    if table_exists(con, schema, "latest_forecast"):
        warnings.warn(f"{schema}.latest_forecast exists, it will be overwriten")
    cur = con.cursor()
    cur.execute("SELECT current_database()")
    dbname = cur.fetchall()[0][0]
    cmd = f"shp2pgsql -d -s 4326 {tmp_shp} {schema}.latest_forecast | psql -d {dbname}" 
    proc = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, 
        stderr=subprocess.STDOUT
    )
    out, err = proc.communicate()
    tmp_dir.cleanup()
    return

def dataframe_to_table(con:pg.extensions.connection, df, schema, table, index_label):
    """
    Uploads a dataframe to the database. It is used to upload the latest forecast
    results.
    """
    if con.info.host == '/var/run/postgresql':
        host = 'localhost'
    else:
        host = con.info.host
    connectionstr = f"postgresql+psycopg2://{con.info.user}:{con.info.password}@{host}:{con.info.port}/{con.info.dbname}"
    engine = create_engine(connectionstr)
    df = df.set_index(index_label)
    df.to_sql(
        name=table, schema=schema, con=engine, 
        if_exists="replace", index=True, index_label=index_label
    )
    engine.dispose()
    
def fetch_forecast_tables(con, schema, admin1):
    """
    Returns the latest forecast results for the required admin1 unit: DSSAT end
    of season output, and DSSAT overview file.
    """
    cur = con.cursor()
    # Get forecast results
    query = """
        SELECT * FROM {0}.latest_forecast_results
        WHERE admin1=%s 
        """.format(schema)
    cur.execute(query, (admin1, ))
    rows = cur.fetchall()
    query_cols = """
        SELECT *
        FROM information_schema.columns
        WHERE table_schema=%s
        AND table_name=%s;
    """
    cur.execute(query_cols, (schema, 'latest_forecast_results'))
    cols = cur.fetchall()
    cols = [c[3] for c in cols]
    results_df = DataFrame(rows, columns=cols)
    
    # Get overview
    query = """
        SELECT * FROM {0}.latest_forecast_overview
        WHERE admin1=%s 
        """.format(schema)
    cur.execute(query, (admin1, ))
    rows = cur.fetchall()
    cur.execute(query_cols, (schema, 'latest_forecast_overview'))
    cols = cur.fetchall()
    cols = [c[3] for c in cols]
    overview_df = DataFrame(rows, columns=cols)
    return results_df, overview_df

def fetch_historical_data(con, schema, admin1):
    """
    NOT IMPLEMENTED, it was part of former stages of the service.
    """
    cur = con.cursor()
    # Get forecast results
    query = """
        SELECT * FROM {0}.historical_data
        WHERE admin1=%s 
        """.format(schema)
    cur.execute(query, (admin1, ))
    rows = cur.fetchall()
    query_cols = """
        SELECT *
        FROM information_schema.columns
        WHERE table_schema=%s
        AND table_name=%s;
    """
    cur.execute(query_cols, (schema, 'historical_data'))
    cols = cur.fetchall()
    cols = [c[3] for c in cols]
    df = DataFrame(rows, columns=cols)
    return df

def fetch_observed_reference(con, schema, admin1):
    """
    Get the observed minimum, mean, and maximum yield for that admin1.
    """
    cur = con.cursor()
    # Get forecast results
    query = """
        SELECT obs_min, obs_avg, obs_max FROM {0}.latest_forecast
        WHERE admin1=%s 
        """.format(schema)
    cur.execute(query, (admin1, ))
    rows = cur.fetchall()
    return tuple(map(np.float32, rows[0]))