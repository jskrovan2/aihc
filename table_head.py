import os
import sys
import pandas as pd
import csv
import gzip

db_dir = 'mimic-iii-clinical-database-1.4'

table_file = sys.argv[1]
if not table_file.endswith('.csv.gz'):
    table_file += '.csv.gz'

if not os.path.exists(table_file):
    if not table_file.startswith('mimic'):
        table_file = f'{db_dir}/{table_file}'

with gzip.open(table_file, mode='rt', newline='') as gz_file:
    csv_reader = csv.reader(gz_file)
    for line_number, row in enumerate(csv_reader):
        print(row)
        if line_number > 20:
            break
