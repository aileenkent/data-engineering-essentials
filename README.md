# Data ETL/ELT/Lakes

## DuckDB

Install in your terminal:
```bash
curl https://install.duckdb.org | sh
```

Having trouble calling up DuckDB with a simple `duckdb` command? Find the full path to the program, and add it as an alias to your shell's resource file (`~/.bashrc` or `~/.zshrc`, etc.)

```
# alias below
alias duckdb='/Users/MY-ACCOUNT-NAME/.local/bin/duckdb'
```
Then open a new terminal and the simple command is available to you!

## Data Formats

### CSV

### Parquet

Parquet is a compressed, schema-embedded, column-formatted data file. See the [parquet](parquet/) directory for more.

### Avro

Avro is very similar to Parquet, but retains records in ROWs. It too is compressed and contains a schema.

## Data Sources

- https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
