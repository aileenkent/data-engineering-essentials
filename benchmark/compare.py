import os
import duckdb
import pandas
import polars
import time

input_file = "https://s3.amazonaws.com/uvasds-systems/data/synthdata.parquet"
local_parquet = "synthdata.parquet"

def compare_duckdb_local():
    con = None
    try:
        # Connect to local DuckDB
        con = duckdb.connect(database='benchmark.db', read_only=False)

        con.execute(f"""
            -- SQL goes here
            DROP TABLE IF EXISTS benchmark;
            CREATE TABLE benchmark
                AS
            SELECT * FROM read_parquet('{input_file}');

        """)
        start_time = time.time()
        stdev = con.execute(f"""
            -- Calculate standard deviation of the 'score' column
            SELECT STDDEV(score) FROM benchmark;
        """)
        print(f"Standard deviation of the 'score' column: {stdev.fetchone()[0]}")

        # Show how much time it took to run this function
        print(f"DuckDB time taken: {time.time() - start_time:.2f} seconds")

    except Exception as e:
        print(f"An error occurred: {e}")

def compare_pandas_dataframe():
    start_time = time.time()
    df = pandas.read_parquet(local_parquet)
    print(f"Standard deviation of the 'score' column: {df['score'].std()}")
    print(f"Pandas time taken: {time.time() - start_time:.2f} seconds")

def compare_polars_dataframe():
    start_time = time.time()
    df = polars.read_parquet(local_parquet)
    print(f"Standard deviation of the 'score' column: {df['score'].std()}")
    print(f"Polars time taken: {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    compare_duckdb_local()
    compare_pandas_dataframe()
    compare_polars_dataframe()

