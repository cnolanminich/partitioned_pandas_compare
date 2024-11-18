# partitioned_pandas_compare

This demonstrates how to test time-based partition for an asset within a run against the previous partition for equality.

Since the assets are partitioned by time, if another job kicks off with a different partition, there will be no contention, and you could safely re-process the data for a specific as of date without fear that it use data from the older as of dates. This code could be modified to partition on hour and not day (e.g., have a "5 pm as of date run" that can run even if items kick off in the 6pm timeframe)

The current setup does the following:

* loads csv files, with one file per partition, into a local duckdb database
* asset check that checks whether the partition processed in that run is the the same as the partition from the prior day.
    * NOTE: this uses pandas and comparing data frames in memory. Dependning on the size, the `compare_dataframes_by_pk` function would need to be changed to work on elastic compute (e.g. Spark or a relational database).
* the output can be viewed in the "output metadata" as a markdown table
    * NOTE: this is for demonstration purpsoses -- for a larger dataset some kind of aggregation would be necesssary.

## Getting started

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `partitioned_pandas_compare/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.
