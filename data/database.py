import psycopg2 as pg
import geopandas as gpd

import warnings
import tempfile
import subprocess
import os
import random
import string

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

def create_table(schema, table):
    return 

def add_column(schema, table, column, type):
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
    """
    gdf = gpd.read_file(shapefile)
    assert admin1 in gdf.columns, f"{admin1} column not in shapefile"
    gdf = gdf.rename(columns={admin1: "admin1"})
    gdf["geometry"] = gdf.geometry.simplify(0.01)

    # tmp_folder = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
    # tmp_folder = os.path.join(TMP, tmp_folder)
    tmp_dir = tempfile.TemporaryDirectory()
    # os.mkdir(tmp_folder)
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
    return  


