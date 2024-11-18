from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
from dagster_duckdb_pandas import DuckDBPandasIOManager

from . import assets  # noqa: TID252

all_assets = load_assets_from_modules([assets])
all_asset_checks = load_asset_checks_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks,
    resources={"io_manager": DuckDBPandasIOManager(
            database=("example.duckdb")
        ),},
    
)
