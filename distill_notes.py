import duckdb

con = duckdb.connect()

# Create a temporary view of the CSV file
con.execute("""
    CREATE TEMPORARY VIEW noteevents_view AS
    SELECT * FROM
    read_csv_auto('NOTEEVENTS.csv.gz')
""")

# Read from huge table and save filtered table in small parquet file.
# Modify the WHERE clause to find the notes you're interested in.
new_table = 'noteevents_metformin.parquet'
con.execute(f"""
    COPY (SELECT * FROM noteevents_view
          WHERE text ILIKE '%metformin%')
    TO '{new_table}' (FORMAT PARQUET)
""")
print(f"Filtered data saved to '{new_table}'")
