

import pandas as pd
from dagster import (
    MetadataValue,
    AssetExecutionContext,
    AssetCheckExecutionContext,
    DailyPartitionsDefinition,
    asset,
    asset_check,
    AssetCheckResult,
    RunsFilter
)
def compare_dataframes_by_pk(df1, df2, pk_column):
    """
    Compares two DataFrames row by row based on a primary key column.

    Args:
        df1 (pd.DataFrame): The first DataFrame.
        df2 (pd.DataFrame): The second DataFrame.
        pk_column (str): The name of the primary key column.

    Returns:
        pd.DataFrame: A DataFrame containing the differences between the two DataFrames.
    """
    # Check if the primary key column exists in both DataFrames
    if pk_column not in df1.columns or pk_column not in df2.columns:
        raise ValueError("Primary key column not found in one or both DataFrames.")
    
    # Merge DataFrames on the primary key column
    merged_df = pd.merge(df1, df2, on=pk_column, how='outer', suffixes=('_df1', '_df2'))

    # Identify rows with differences
    diff_rows = merged_df[merged_df.isnull().any(axis=1)]

    # Create a DataFrame with the differences
    diff_df = diff_rows.copy()
   
    for column in df1.columns:
        if column != pk_column and column != 'as_of_date':
            diff = merged_df[f"{column}_df1"] != merged_df[f"{column}_df2"]
            diff_rows = diff_rows._append(merged_df[diff & ~merged_df.isnull().any(axis=1)])
    # Create a DataFrame with the differences
    diff_df = diff_rows.drop_duplicates().copy()
    # Reorder columns
    ordered_columns = [pk_column]
    for column in df1.columns:
        if column != pk_column:
            ordered_columns.append(f"{column}_df1")
            ordered_columns.append(f"{column}_df2")
    
    diff_df = diff_df[ordered_columns]
    return diff_df

# Define the asset with partitioning by as_of_date
@asset(partitions_def=DailyPartitionsDefinition(start_date="2024-11-12"),
       metadata={"partition_expr": "as_of_date"},)
def my_partitioned_asset(context: AssetExecutionContext) -> pd.DataFrame:
    as_of_date = context.partition_key
    # Read the CSV file for the specific partition date
    df = pd.read_csv(f"partitioned_pandas_compare/data/data_{as_of_date}.csv")
    context.log.info(df.head())
    return df

# Define an asset check to compare results per primary key
@asset_check(asset=my_partitioned_asset, description="Check for unique primary keys")
def check_primary_key_uniqueness(context: AssetCheckExecutionContext, partitioned_asset: pd.DataFrame) -> AssetCheckResult:
    # Assuming 'primary_key' is the column to check for uniqueness
    is_unique = partitioned_asset['primary_key'].is_unique
    context.log.info(f"Primary key is unique: {partitioned_asset['primary_key']}")
    return AssetCheckResult(
        passed=is_unique,
        metadata={"is_unique": is_unique}
    )

# Define an asset check to compare results per primary key
@asset_check(asset=my_partitioned_asset, description="Check for unique primary keys")
def check_partition_equality(context: AssetCheckExecutionContext, partitioned_asset: pd.DataFrame) -> AssetCheckResult:
    # Assuming 'primary_key' is the column to check for uniqueness
    run_id = context.run.run_id
    #list of run records
    context.run.run_id
    run_record = context.instance.get_run_records(
        filters = RunsFilter(
        run_ids = [run_id],
        )
    )
    context.log.info(f"run_records: {run_record}")
    context.log.info(f"run_records: {run_record[0]}")
    partition_key = run_record[0].dagster_run.tags["dagster/partition"] 

    context.log.info(f"partition_keys: {partition_key}")
    partition_key_as_date = pd.to_datetime(partition_key)
    
    # Get yesterday's partition
    previous_partition = partition_key_as_date - pd.DateOffset(days=1)
    context.log.info(f"previous_partition: {previous_partition}")
    previous_partition_str = previous_partition.strftime("%Y-%m-%d")

    partition_today = partitioned_asset[partitioned_asset['as_of_date'] == partition_key]
    partition_yesterday = partitioned_asset[partitioned_asset['as_of_date'] == previous_partition_str]
    context.log.info(pd.unique(partitioned_asset['as_of_date']) )
   
    df_differences = compare_dataframes_by_pk(partition_today, partition_yesterday, "primary_key")
    is_equal = df_differences.empty
    return AssetCheckResult(
        passed=is_equal,
        metadata={"df_differences": MetadataValue.md(df_differences.to_markdown())}
    )