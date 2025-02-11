import os
import duckdb

source_dir = 'mimic-iii-clinical-database-1.4'
destination_dir = 'parquet-mimic-iii-clinical-database-1.4'

os.makedirs(destination_dir, exist_ok=True)

# Function to convert CSV to Parquet using DuckDB
def convert_csv_to_parquet(source_file, destination_file):
    if os.path.basename(source_file) == 'CHARTEVENTS.csv.gz':
        # Special type handling for 'VALUE' column
        query = f"""
            COPY (SELECT * FROM read_csv_auto('{source_file}', types={{'VALUE': 'VARCHAR'}}))
            TO '{destination_file}' (FORMAT 'parquet')
        """
    elif os.path.basename(source_file) == 'CPTEVENTS.csv.gz':
        # Special type handling for 'VALUE' column
        query = f"""
            COPY (SELECT * FROM read_csv_auto('{source_file}', types={{'CPT_CD': 'VARCHAR'}}))
            TO '{destination_file}' (FORMAT 'parquet')
        """
    else:
        # Standard conversion for other files
        query = f"""
            COPY (SELECT * FROM read_csv_auto('{source_file}'))
            TO '{destination_file}' (FORMAT 'parquet')
        """

    # Execute the query
    duckdb.query(query)
    print(f"Converted {source_file} to {destination_file}")

# Loop over all CSV.gz files in the source directory
for file_name in os.listdir(source_dir):
    if file_name.endswith('.csv.gz'):
        source_file_path = os.path.join(source_dir, file_name)
        destination_file_path = os.path.join(destination_dir, file_name.replace('.csv.gz', '.parquet'))

        # Convert the file to Parquet
        convert_csv_to_parquet(source_file_path, destination_file_path)

print("All files have been converted.")

