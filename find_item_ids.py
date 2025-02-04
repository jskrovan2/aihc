import sys
import re
import pandas as pd
import csv
import gzip

db_dir = 'mimic-iii-clinical-database-1.4'

d_chart = f'{db_dir}/D_ITEMS.csv.gz'
chart = f'{db_dir}/D_ITEMS.csv.gz'
lab_items = pd.read_csv(f'{db_dir}/D_LABITEMS.csv.gz')
tables = [
    (f'{db_dir}/D_ITEMS.csv.gz',    f'{db_dir}/CHARTEVENTS.csv.gz'),
    (f'{db_dir}/D_LABITEMS.csv.gz', f'{db_dir}/LABEVENTS.csv.gz'),
]

#items_of_interest = ['height', 'weight', 'creatinine']
#items_of_interest = ['hdl', 'triglycerides', 'cholesterol', 'lipids']
items_of_interest = sys.argv[1:]

for d_chart_name, chart_name in tables:
    print(f'    chart: {chart_name}')
    print(f'    items_of_interest:')
    chart = pd.read_csv(d_chart_name)
    for item in items_of_interest:
        items = chart[chart['LABEL'].str.contains(item, case=False, na=False)]
        for index, row in items.iterrows():
            item_id = row['ITEMID']
            label = row['LABEL']
            key = re.sub(r'\W+', '_', label.upper()).strip('_')
            print(f"        '{item_id}' : ['{label}', '{key}', 'mean']")
    print()
