"""
All the database operations are included here. 
"""
import psycopg2 as pg
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

TMP = tempfile.gettempdir()

def connect(dbname):
    con = pg.connect(database=dbname)
    return con

def create_schema(dbname, schema):
    """Creates a new schema"""
    con = connect(dbname)
    cur = con.cursor()
    query = """
        CREATE SCHEMA {0};
        """.format(schema)
    cur.execute(query)
    con.commit()
    cur.close()
    con.close()
    return

def _create_reanalysis_table(dbname, schema, table):
    """Creates a renalysis table"""
    con = connect(dbname)
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
    con.close()
    return 

def create_static_table(dbname, schema):
    """Creates table for static rasters"""
    con = connect(dbname)
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
    con.close()
    return 

def _create_soil_table(dbname, schema):
    """Creates soil table"""
    table = "soil"
    con = connect(dbname)
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
    con.close()
    return 

def schema_exists(dbname, schema):
    """Check if schema exists in database."""
    con = connect(dbname)
    cur = con.cursor()
    query = """
        SELECT * FROM information_schema.schemata 
        WHERE 
            schema_name='{0}';
        """.format(schema)
    cur.execute(query)
    schema_exists = bool(cur.rowcount)
    cur.close()
    con.close()
    return schema_exists 

def table_exists(dbname, schema, table):
    """Check if table exists in the database."""
    con = connect(dbname)
    cur = con.cursor()
    query = """
        SELECT * FROM information_schema.tables 
        WHERE 
            table_schema='{0}' AND table_name='{1}';
        """.format(schema.lower(), table.lower())
    cur.execute(query)
    table_exists = bool(cur.rowcount)
    cur.close()
    con.close()
    return table_exists

def add_country(dbname:str, name:str, shapefile:str, 
                admin1:str="admin1"):
    """
    Add a country to the database. It will create a new schema and all of the 
    tables empty tables it'll need to support the service. The funciton loads
    the shp into the name.admin table, and it simplifies the geometries.

    Arguments
    ----------
    dbname: str
        Name of the database
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

    if table_exists(dbname, name, "admin"):
        warnings.warn(f"{name}.admin exists, it will be overwriten")
    else:
        create_schema(dbname, name)

    cmd = f"shp2pgsql -d -s 4326 {tmp_shp} {name}.admin | psql -d {dbname}"
    proc = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, 
        stderr=subprocess.STDOUT
    )
    out, err = proc.communicate()
    tmp_dir.cleanup()

    # Create a materialized view for the envelope
    con = connect(dbname)
    cur = con.cursor()
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
        if not table_exists(dbname, name, f"era5_{var}"):
            _create_reanalysis_table(dbname, name, f"era5_{var}")
    if not table_exists(dbname, name, f"soil"):
        _create_soil_table(dbname, name)
    cur.close()
    con.close()
    return

def get_envelope(dbname:str, schema:str, pad=0.1):
    """
    Get the envelope for the region. Envelope was previously created
    as a materialized view
    """
    con = connect(dbname)
    cur = con.cursor()
    query = "SELECT * from {0}.bbox;".format(schema)
    cur.execute(query)
    out = cur.fetchall()
    cur.close()
    con.close()
    bbox = out[0][0]
    bbox = bbox.split("((")[-1].split("))")[0]
    bbox = list(map(lambda x: x.split(), bbox.split(",")))
    bbox = [bbox[1][1], bbox[0][0], bbox[0][1], bbox[2][0]]
    bbox = list(map(float, bbox))
    bbox = [bbox[0]+pad, bbox[1]-pad, bbox[2]-pad, bbox[3]+pad]
    return bbox

def delete_rasters(dbname, schema, table, date):
    """If date already exists delete associated rasters before ingesting."""
    con = connect(dbname)
    cur = con.cursor()
    query = """
        SELECT * FROM {0}.{1}
        WHERE
            fdate='{2}'
        """.format(schema, table, date.strftime('%Y-%m-%d'))
    cur.execute(query)
    if bool(cur.rowcount):
        # TODO: This has to go in a log
        warnings.warn("Overwriting raster in {0}.{1} table for {1}".format(
            schema, table, date.strftime("%Y-%m-%d")
        ))
        query = """
            DELETE FROM {0}.{1} 
            WHERE
                fdate='{2}'
            """.format(schema, table, date.strftime("%Y-%m-%d"))
        cur.execute(query)
        con.commit()
    cur.close()
    con.close()

def tiff_to_db(tiffpath:str, dbname:str, schema:str, table:str, 
               date:datetime=None, ens:int=None, par:str=None):
    """
    Saves tiff to the database.

    Arguments
    ----------
    tiffpath: str
        Path to the tiff file
    dbname: str
        Name of the database
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
    con = connect(dbname)

    temptable = ''.join(random.SystemRandom().choice(string.ascii_letters) for _ in range(8))
    cmd = f"raster2pgsql -d -s 4326 -t 10x10 {tiffpath} {temptable} | psql -d {dbname}"
    proc = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    out, err = proc.communicate()
    cur = con.cursor()
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
        # TODO: Add ens index when necessary
        # Copy to permanent table
        query = """
            INSERT INTO {0}.{1}({2}) (SELECT {2} FROM {3});
            """.format(schema, table, ",".join(columns), temptable)
        cur.execute(query)
        con.commit()
        cur.close()
    finally:
        con.close() # Closed in case something failed on the try
        con = connect(dbname)
        cur = con.cursor()
        query = """
            DROP TABLE {0};
            """.format(temptable)
        cur.execute(query)
        con.commit()
        cur.close()
        con.close()
    return

def verify_static_par_exists(dbname:str, schema:str, parname:str):
    """It will raise an error if the static parameter already exists"""
    if not table_exists(dbname, schema, "static"):
        return False
    con = connect(dbname)
    cur = con.cursor()
    query = """
        SELECT 1 FROM {0}.static WHERE par = '{1}';
        """.format(schema, parname)
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    con.close()
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
    

def get_era5_for_point(con, schema:str, lon:float, lat:float,
                       datefrom:datetime, dateto:datetime):
    """
    Get the weather series for the requested point and time period. It returns 
    a dict containing the series for each weather variable. It returns a df with
    the time series for that point.
    """
    for var in VARIABLES_ERA5_NC.keys():
        table = f"era5_{var}"
        dates_notin_db = verify_series_continuity(
            con, schema, table, datefrom, dateto
        )
        assert len(dates_notin_db) == 0, \
            f"Dates are missing in {table}: {dates_notin_db}. Ingest that data first"
    
    # Get rain data. This will also get the rid to make next queries using 
    # rid instead of spatial relations
    variables = list(VARIABLES_ERA5_NC.keys())
    var = variables[0]

    cur = con.cursor()
    import time
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
    """Get the static parameter value for a location"""
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

    return rows[0][0]
    
def check_admin1_in_country(con, schema, admin1):
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