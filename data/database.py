"""
All the database operations are included here. 
"""
import psycopg2 as pg
import geopandas as gpd

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

def _create_soil_table(dbname, schema):
    """Creates soil table"""
    table = "soil"
    con = connect(dbname)
    cur = con.cursor()
    query = """
        CREATE TABLE {0}.{1} (
            gid serial PRIMARY KEY, 
            geom geometry (POINT, 4326),
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
               date:datetime, column:str="rast", ens:int=None):
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
    column: str
        Name of the column where the raster will be located. This is useful
        for cases when there is more than one raster per row, which would be the 
        case of weather data when the raster domain is the same. If None, then 
        column would be named rast.
    ens: int
        Ensemble number. It is used when ensemble-based datasets are used.
    """ 
    con = connect(dbname)
    cur = con.cursor()

    temptable = ''.join(random.SystemRandom().choice(string.ascii_letters) for _ in range(8))
    cmd = f"raster2pgsql -d -s 4326 -t 10x10 {tiffpath} {temptable} | psql -d {dbname}"
    proc = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    out, err = proc.communicate()
    try:
        # TODO: Need to create log
        query = """
            ALTER TABLE {0} ADD COLUMN fdate DATE
            """.format(temptable)
        cur.execute(query)
        # TODO: Add ens column when necessary
        query = """
            UPDATE {0} SET fdate = date '{1}'
            """.format(temptable, date.strftime("%Y-%m-%d"))
        cur.execute(query)
        # Create raster, spatial and time indexes
        query = """
            CREATE INDEX {0}_rid ON {1} (rid);
            """.format(table, temptable)
        cur.execute(query)
        query = """
            CREATE INDEX {0}_spatial ON {1} USING GIST (ST_Envelope(rast));
            """.format(table, temptable)
        cur.execute(query)
        query = """
            CREATE INDEX {0}_time ON {1} (fdate);
            """.format(table, temptable)
        cur.execute(query)
        # Copy to 
        query = """
            INSERT INTO {0}.{1}(rid, rast, fdate) (SELECT rid, rast, fdate  FROM {2});
            """.format(schema, table, temptable)
        cur.execute(query)
        con.commit()
    finally:
        query = """
            DROP TABLE {0};
            """.format(temptable)
        cur.execute(query)
        con.commit()
    cur.close()
    con.close()
    return

def verify_series_continuity(dbname:str, shcema:str, table:str, 
                             datefrom:datetime, dateto:datetime):
    """
    Verify if there is continous record of weather series. It returns the 
    missing dates on the requested period
    """

def ingest_soils(dbname:str, schema:str, solfile:str, cropmask1:str,
                 cropmask2:str):
    """
    
    """