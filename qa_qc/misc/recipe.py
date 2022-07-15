import pandas as pd
from google.cloud import storage
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes import XarrayZarrRecipe
from pangeo_forge_recipes.storage import (
    CacheFSSpecTarget,
    FSSpecTarget,
    MetadataTarget,
    StorageConfig,
)

storage_client = storage.Client("climacell-research")
bucket = storage_client.get_bucket("wxapps-dev-static-files")

start_date = "2022-06-17"
end_date = "2022-06-18"
dates = pd.date_range(start_date, end_date, freq="6H")
variables = ["Wind_temp"]
target_chunks = {'isobaricInhPa': 17, 'latitude': 145, 'longitude': 288, 'time': 1}

def format_function(variable, time):  # these must match the arguments to FilePattern()
    datetime_base = pd.Timestamp(start_date)
    datetime = datetime_base + pd.Timedelta(hours=time)
    input_url_pattern = (
        f"https://storage.cloud.google.com/wxapps-dev-static-files/wafs/"
        f"ingress/met_model/WAF/{datetime:%Y}/{datetime:%m}/{datetime:%d}/"
        f"KWBC/{datetime:%H}/{variable}/ingress_met_model_WAF_{datetime:%Y}"
        f"_{datetime:%m}_{datetime:%d}_KWBC_{datetime:%H}_{variable}_{datetime:%Y}"
        f"{datetime:%m}{datetime:%d}_{datetime:%H}00f06.grib2"
    )
    return input_url_pattern


# MergeDim for variables
variable_merge_dim = MergeDim(
    name="variable", keys=variables  # this must match the argument to format_function()
)

# ConcatDim for what to concatenate over
time_concat_dim = ConcatDim(
    name="time",  # this must match the argument to format_function()
    keys=range(len(dates)),
    nitems_per_file=1,
)

pattern = FilePattern(
    format_function,
    variable_merge_dim,
    time_concat_dim,
)

for key, filename in pattern.items():
    break
print(key, filename)

# XarrayZarrRecipe: copies data to xarray ds in zarr format
# NetCDF, OPeNDAP, GRIB, Zarr, GeoTIFF (via rasterio)
recipe = XarrayZarrRecipe(
    pattern,
    target_chunks=target_chunks,
    cache_inputs=True,
    xarray_open_kwargs={
        "engine": "cfgrib",
        "backend_kwargs": {
            "filter_by_keys": {"typeOfLevel": "isobaricInhPa"}
        },
    },
)
