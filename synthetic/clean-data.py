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
                SET age = date_diff('year', birth_date, CURRENT_DATE);""")
        
        con.execute(f"""
            DELETE FROM synthdata
                WHERE score IS NULL;""")

        con.execute(f"""
            -- Create a new table with unique rows
            CREATE TABLE sythdata_clean AS 
            SELECT DISTINCT * FROM sythdata;
            -- Drop original and rename
            DROP TABLE sythdata;
            ALTER TABLE sythdata_clean RENAME TO sythdata;""")
        
        con.execute(f"""
            --max age? min age? trying to print them
            SELECT MAX(age) as max_age, MIN(age) as min_age;
            COUNT AS over_100 
                FROM sythdata 
                WHERE age OVER 100;
            RETURN(f" Max age: {max_age}");
            RETURN(f" Min age: {min_age}");
            RETURN(f" Over 100: {over_100}");
            """)
        
        con.execute(f"""
            SELECT COUNT(*) AS total_records
            RETURN(f" Records left: {total_records}");
            """)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    clean_parquet()

