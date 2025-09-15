import os
import duckdb

RDS_PASSWORD = os.getenv('RDS_PASSWORD')

def connect():
    con = None
    try:
        con = duckdb.connect(database=':memory:', read_only=False)

        # Create a persistent MySQL secret
        con.execute(f"""
        CREATE OR REPLACE PERSISTENT SECRET rds (
            TYPE mysql,
            HOST 'db1.cqee4iwdcaph.us-east-1.rds.amazonaws.com',
            PORT 3306,
            DATABASE 'nem2p',
            USER 'admin',
            PASSWORD '{RDS_PASSWORD}'
        );
        """)

        print("Secret created successfully.")
        return con
    
    except Exception as e:
        print(f"Error connecting to DuckDB: {e}")


if __name__ == "__main__":
    connect()