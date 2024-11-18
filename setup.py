from setuptools import find_packages, setup

setup(
    name="partitioned_pandas_compare",
    packages=find_packages(exclude=["partitioned_pandas_compare_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-pandas",
        "pandas",
        "dagster-duckdb-pandas"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
