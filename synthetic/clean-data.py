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
            -- SQL goes here
            DROP TABLE IF EXISTS synthdata;
            CREATE TABLE synthdata
                AS
            SELECT * FROM read_parquet('{input_file}');
        """)
            
        """ More cleaning steps
        1. Add age column and populate it (see transform/README.md)
        2. Delete rows with NULL for the 'score' column
        3. Deduplicate the data set (see transform/CLEANING.md)
        4. Max age? Min age? How many over 100?
        5. How many records are left?
        """
        con.execute(f"""
            ALTER TABLE synthdata 
            ADD COLUMN age INTEGER;
            UPDATE sythdata 
            SET age = date_diff('year', birth_date, CURRENT_DATE);
        """)
        
        con.execute(f"""
            DELETE FROM synthdata
                WHERE score IS NULL;
        """)

        con.execute(f"""
            -- Create a new table with unique rows
            CREATE TABLE synthdata_clean AS 
            SELECT DISTINCT * FROM synthdata;
                    
            -- Drop original and rename
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

        over100 = con.execute(f"""
            COUNT FROM synthdata WHERE age OVER 100;
            """)
        print(f"Over 100: {over100.fetchone()[0]}")

        totalrecords = con.execute(f"""
            SELECT COUNT(*) FROM synthdata;
            """)
        print(f"Total records: {totalrecords.fetchone()[0]}")
        

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    clean_parquet()

