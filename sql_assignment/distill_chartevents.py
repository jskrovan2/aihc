import duckdb
import time

con = duckdb.connect()

# Create a temporary view of the CSV file

con.execute(f"""
    CREATE TABLE CHARTEVENTS AS SELECT * FROM 'CHARTEVENTS.parquet'
""")

# Export the filtered data to a smaller parquet file
# weight, height, systolic BP, diastolic BP
con.execute("""
    COPY (SELECT * FROM CHARTEVENTS
          WHERE ITEMID IN ('226707', '226512', '220179', '220180')
    TO 'chartevents_height_weight_systolic_diastolic.parquet' (FORMAT PARQUET)
""")
