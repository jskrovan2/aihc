import duckdb
import time

con = duckdb.connect()

# Create a temporary view of the CSV file

con.execute(f"""
    CREATE TABLE NOTEEVENTS AS SELECT * FROM 'NOTEEVENTS.parquet'
""")

# Export the filtered data to a smaller parquet file
# weight, height, systolic BP, diastolic BP
con.execute("""
    COPY (SELECT * FROM NOTEEVENTS
          WHERE text ILIKE '%metformin%')
    TO 'noteevents_metformin.parquet' (FORMAT PARQUET)
""")
