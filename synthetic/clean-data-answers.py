import os
import duckdb

input_file = "https://s3.amazonaws.com/uvasds-systems/data/synthdata.parquet"

def clean_parquet():

    con = None

    try:
        # Connect to local DuckDB
        con = duckdb.connect(database='synthdata.duckdb', read_only=False)

        # Clear and import
        con.execute(f"""
            DROP TABLE IF EXISTS synthdata;
            CREATE TABLE synthdata AS SELECT * FROM read_parquet('{input_file}');
        """)

        con.execute(f"""
            ALTER TABLE synthdata 
            ADD COLUMN age INTEGER;

            UPDATE synthdata 
            SET age = date_diff('year', birth_date, CURRENT_DATE);
        """)

        con.execute(f"""
            DELETE FROM synthdata
            WHERE score IS NULL;
        """)

        con.execute(f"""
            CREATE TABLE synthdata_clean AS 
            SELECT DISTINCT * FROM synthdata;

            DROP TABLE synthdata;
            ALTER TABLE synthdata_clean RENAME TO synthdata;
        """)

        maxage = con.execute(f"""
            SELECT MAX(age) FROM synthdata;
        """)
        print(f"Max age: {maxage.fetchone()[0]}")
        minage = con.execute(f"""
            SELECT MIN(age) FROM synthdata;
        """)
        print(f"Min age: {minage.fetchone()[0]}")

        too_old = con.execute(f"""
            SELECT COUNT(*) FROM synthdata
            WHERE age > 100;
        """)
        print(f"Over 100: {too_old.fetchone()[0]}")
        
        count = con.execute(f"""
            SELECT COUNT(*) FROM synthdata;
        """)
        print(f"Total count: {count.fetchone()[0]}")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    clean_parquet()

