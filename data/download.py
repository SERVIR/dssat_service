"""
This module contains the functions to download data.
"""
import cdsapi
import zipfile
import tempfile
import os
import random
import string

from datetime import datetime

TMP = tempfile.gettempdir()


c = cdsapi.Client()

VARIABLES_ERA5_API = {
    "tmax": ('2m_temperature', '24_hour_maximum'),
    "tmin": ("2m_temperature", "24_hour_minimum"),
    "srad": ("solar_radiation_flux", False),
    "rain": ("precipitation_flux", False),
    "tdew": ("2m_dewpoint_temperature", "24_hour_mean"),
    "wind": ("10m_wind_speed", "24_hour_mean")
}

def download_era5(date:datetime, variable:str, area:list[float], folder:str=TMP):
    """
    Download era5 data for a single day. It'll download the requested variable. 
    It returns the path to the netCDF file.

    Arguments
    ----------
    date: datetime.datetime
        Day to download data for
    variable: str
        Variable to download. One among TMAX, TMIN, SRAD, TDEW, WIND, and RAIN
    area: list of float
        Bounding box for the area to download (north, west, south, east)
    folder: str
        Folder where the netCDF file will be saved. Default to /tmp folder
    """
    zip_path = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))
    zip_path = os.path.join(folder, f"{zip_path}.zip")

    variable, statistic = VARIABLES_ERA5_API[variable]
    request_pars = {
            'format': 'zip',
            'variable': variable,
            'year': f"{date.year:04d}",
            'month': f"{date.month:02d}",
            'day': f"{date.day:02d}",
            'area': area,
            'version': '1_1'
        }
    if statistic:
        request_pars["statistic"] = statistic
    else:
        statistic = "24_hour_total"
    c.retrieve(
        'sis-agrometeorological-indicators',
            request_pars, zip_path
    )
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        file = zip_ref.namelist()[0]
        file_path = os.path.join(folder, file)
        with open(file_path, "wb") as f:
            f.write(zip_ref.read(file))
    os.remove(zip_path)
    return file_path