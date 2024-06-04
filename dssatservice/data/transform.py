
"""
This module contains funtions to transform data. Transformation implies converting
between data formats, or extrating data from some file to create a new file.
"""
from netCDF4 import Dataset, num2date
import numpy as np
import pandas as pd

import tempfile
from datetime import datetime
import re 

from osgeo import gdal
from osgeo import osr

def write_tiff(lat, lon, res, data, tiffpath=None, epsg=4326):
    """
    Writes Geotif in temporary directory so it can be imported into the PostGIS database.
    This function was addapted from RHEAS. It returns the path to the tiff file.

    Arguments
    ----------
    lat: list
        Latitude in 1D list or array
    lon: list
        Longitude in 1D list or array
    res: float
        Data resolution in the units of lat or lon (degrees usually)
    data: 2D array
        2D array nor masked array containing the data
    tiffpath: str
        Path to the tiff file to write. If None then a tmpfile is created.
    epsg: int
        EPSG code for CRS
    """
    if isinstance(data, np.ma.masked_array):
        nodata = np.double(data.fill_value)
        data = data.data
    else:
        nodata = -9999.

    nrows, ncols = data.shape
    out = data
    if tiffpath is None:
        f = tempfile.NamedTemporaryFile(suffix=".tif", delete=False)
        tiffpath = f.name
        f.close()
    driver = gdal.GetDriverByName("GTiff")
    ods = driver.Create(tiffpath, ncols, nrows, 1, gdal.GDT_Float32)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(epsg)
    ods.SetProjection(srs.ExportToWkt())
    ods.SetGeoTransform(
        [min(lon) - res / 2.0, res, 0, 
         max(lat) + res / 2.0, 0, -res]
    )
    ods.GetRasterBand(1).WriteArray(out)
    ods.GetRasterBand(1).SetNoDataValue(nodata)
    ods = None
    return tiffpath

def nc_to_tiff(variable:str, date:datetime, ncpath:str, tiffpath:str=None, **kwargs):
    """
    Convert netcdf file to tiff. Returns path to tiff.

    Arguments
    ----------
    variable: str
        Name of the netcdf variable
    date: datetime
        Date to fetch from the netcdf file 
    ncpath: str
        Path to the netcdf file
    tiffpath: str
        Path to the tiff to write. If None then a tmpfile is created
    **kwargs:
        Other kwargs can be passed. Those kwargs are lat, lon, and time , they 
        map each variable to the netcdf variable that represents each. If not 
        provided then default values from AgERA5 are taken
    """
    timevar = kwargs.get("time", "time")
    latvar = kwargs.get("lat", "lat")
    lonvar = kwargs.get("lon", "lon")

    nc = Dataset(ncpath)
    
    time = nc.variables[timevar]
    time = [datetime(t.year, t.month, t.day) for t in num2date(time[:], time.units)]
    assert date in time, f"{date.strftime('%Y-%m-%d')} not in {ncpath} file"
    time_idx = time.index(date)

    data = nc.variables[variable][time_idx, : ,:].data
    lon = nc.variables[lonvar][:].data
    lat = nc.variables[latvar][:].data
    res = (lat.max() - lat.min())/len(lat)

    tiffpath = write_tiff(lat, lon, res, data, tiffpath=None, epsg=4326)
    return tiffpath


ENV_PARSE_INDEX = (
    "Emergence-End Juvenile", "End Juvenil-Floral Init",
    "Floral Init-End Lf Grow", "End Lf Grth-Beg Grn Fil",
    "Grain Filling Phase", "Planting to Harvest"
)
ENV_STRESS_COLNAMES = (
    "devPhase", "timeSpan", "avgTmax", "avgTmin", "avgTmean", "avgSrad", "avgPhotper", "avgCO2",
    "cummRain", "cummET", "cummETp", "ndaysTminLt0", "ndaysTminLt2", "ndaysTmaxGt30",
    "ndaysTmaxGt32", "ndaysTmaxGt34", "ndaysRainGt0", "stressWatPho", "stressWatGro",
    "stressNitPhto", "stressNitGro", "stressPhoPho", "stressPhoGro" 
)
def parse_overview(overview_str):
    """
    Parse the overview file to get environmental and stress factors
    """
    lines = []
    for key in ENV_PARSE_INDEX:
        for n, l in enumerate(re.findall(f"({key}.+)\n", overview_str), 1):
            lines.append(
                [n, key] + l.replace(key, "").split()
            )
    df = pd.DataFrame(lines, columns=["RUN"]+list(ENV_STRESS_COLNAMES))
    return df   