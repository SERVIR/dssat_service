"""
This module contains the functions to download data.
"""
import cdsapi
import zipfile
import tempfile
import os
import random
import string
import climateserv.api
from datetime import datetime, timedelta
import requests
from . import transform

TMP = tempfile.gettempdir()

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

    c = cdsapi.Client(progress=False)
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

def download_nmme(variable:str, ens:int, area:list[float], folder:str=None,
                  geotrans_ref=None):
    """
    Download CCSM4 NMME data for the entire forecast window for the requested 
    variable. It will download the available run, which will be assumed to be
    the most recent run. It returns the path to the tiffs files, and list of 
    files.
    
    Arguments
    ----------
    variable: str
        Variable to download. One among Precipitation or Temperature
    ens: int
        Ensemble member to download
    area: list of float
        Bounding box for the area to download (north, west, south, east)
    folder: str
        Folder where the tiff files file will be saved. Default to /tmp folder
    geotrans_ref: str
        Path to a raster to have as a reference to create geotransformed rasters
        from the downloaded rasters. The geotransformed rasters will have the 
        same name as the original rasters but adding the 'gtr' prefix 
    """
    # Get info on the latest forecast data
    r = requests.get(
        "https://climateserv.servirglobal.net/api/getClimateScenarioInfo/"
    )
    forecast_info = r.json()["climate_DataTypeCapabilities"][0]["current_Capabilities"]
    start_date = datetime.strptime(forecast_info["startDateTime"], "%Y-%m-%d")
    end_date = datetime.strptime(forecast_info["endDateTime"], "%Y-%m-%d")
    coords = [
        [area[1], area[0]], [area[3], area[0]], [area[3], area[2]], 
        [area[1], area[2]], [area[1], area[0]]
    ]
    tmp_dir = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))
    if folder is None:
        folder = os.path.join(TMP, f"{tmp_dir}")
    zip_path = f"{tmp_dir}.zip" 
    if not os.path.exists(folder):
        os.mkdir(folder)
    
    climateserv.api.request_data(
        "CCSM4", "Download", start_date.strftime("%m/%d/%Y"), 
        end_date.strftime("%m/%d/%Y"), coords, f"ens{ens:02d}", 
        variable, zip_path
    )
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        files = zip_ref.namelist()
        for file in files:
            file_path = os.path.join(folder, file)
            with open(file_path, "wb") as f:
                f.write(zip_ref.read(file))
            # If a refernce to geotransform is provided
            if geotrans_ref is not None:
                reproj_raster_path = os.path.join(folder, f"gtr{file}")
                transform.reproject_raster(
                    os.path.join(folder, file),
                    reproj_raster_path,
                    geotrans_ref
                )
    os.remove(zip_path)      
    return folder, files
     