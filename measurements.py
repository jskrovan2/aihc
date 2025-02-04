import argparse
import sys
import re
import yaml
import csv
import gzip
import numpy as np
from collections import namedtuple

db_dir = 'mimic-iii-clinical-database-1.4'
chartevents = f'{db_dir}/CHARTEVENTS.csv.gz'
labevents = f'{db_dir}/LABEVENTS.csv.gz'

# Item = namedtuple('Item', ['label', 'header', 'policy'])
# policy is one of: 'same', 'mean', 'median', 'first', 'max', 'min'

#items_of_interest = {
#    '226512' : Item('Admission Weight (Kg)', 'WEIGHT',     'same'),
#    '226707' : Item('Height',                'HEIGHT',     'same'),
#    '226537' : Item('Glucose (whole blood)', 'GLUCOSE_WHOLE_BLOOD', 'mean'),
#    '220615' : Item('Creatinine',            'CREATININE', 'mean'),
#    '220179' : Item('Non Invasive Blood Pressure systolic', 'NON_INVASIVE_BLOOD_PRESSURE_SYSTOLIC', 'mean'),
#    '220180' : Item('Non Invasive Blood Pressure diastolic', 'NON_INVASIVE_BLOOD_PRESSURE_DIASTOLIC', 'mean'),
#}

#items_of_interest = {
#    '50904' : Item('Cholesterol, HDL', 'CHOLESTEROL_HDL', 'mean'),
#    '50907' : Item('Cholesterol, Total', 'CHOLESTEROL_TOTAL', 'mean'),
#    '51000' : Item('Triglycerides', 'TRIGLYCERIDES', 'mean'),
#    '50852' : Item('% Hemoglobin A1c', 'HEMOGLOBIN_A1C', 'mean'),
#}

def main():
    '''
        (1) Parse arugments
        (2) Read chartevents or labevents looking for items_of_interest
        (3) Write measurements with items_of_interest
        (4) Print information about the search for items_of_interest
    '''
    # (1) Parse arugments
    items_of_interest, chart, measurements, max_rows, show_conflicts, debug = parse_arguments()

    # (2) Read chartevents or labevents looking for items_of_interest
    item_values, items_found, num_records = read_chart(chart, items_of_interest, max_rows)

    # (3) Write measurements with items_of_interest
    hadm_conflicts, hadm_incomplete = write_measurements(measurements, item_values,
                                                         items_of_interest, items_found)

    # (4) Print information about the search for items_of_interest
    report(items_of_interest, hadm_conflicts, item_values, items_found, hadm_incomplete,
           num_records, show_conflicts)

def read_chart(events, items_of_interest, max_rows):
    ''' Read chartevents csv file looking for items_of_interest. '''
    item_values = {}
    items_found = []
    num_records = 0

    with gzip.open(events, mode='rt', newline='') as gz_file:
        csv_reader = csv.reader(gz_file)
        header = next(csv_reader)
        valuenum_index = header.index('VALUENUM')
        hadm_id_index = header.index('HADM_ID')
        item_id_index = header.index('ITEMID')
        error_index = header.index('ERROR') if 'ERROR' in header else None
        units_index = header.index('VALUEUOM')

        errors = []
        # Iterate over each line in the CSV file
        for line_number, row in enumerate(csv_reader):
            if error_index is not None:
                error = row[error_index] or '0'
                if error != '0':
                    errors.append(error)
                    continue

            num_records += 1
            item_id = row[item_id_index]
            if item_id in items_of_interest:
                info = items_of_interest[item_id]
                value_units = row[units_index] or ''
                value_units = value_units.lower()
                value_units = re.sub(f'\.', '', value_units)
                value_units = re.sub(f' ', '', value_units)
                value = (row[valuenum_index], value_units)

                hadm_id = row[hadm_id_index]
                hadm_items = item_values.setdefault(hadm_id, {})
                if item_id in hadm_items:
                    if not isinstance(hadm_items[item_id], list):
                        hadm_items[item_id] = [hadm_items[item_id]]
                    hadm_items[item_id].append(value)
                else:
                    if item_id not in items_found:
                        items_found.append(item_id)
                    hadm_items[item_id] = value
            if max_rows and line_number >= max_rows - 1:
                break

    return item_values, items_found, num_records

def write_measurements(measurements, item_values, items_of_interest, items_found):
    ''' Write csv file with requested measurements. '''
    header = ['HADM_ID']
    for item_id in items_found:
        header.append(items_of_interest[item_id].header)
        header.append(items_of_interest[item_id].header + '_UNITS')

    hadm_conflicts = {}
    hadm_incomplete = {}
    rows = []
    for hadm_id in item_values:
        row = [hadm_id]
        for item_id in items_found:
            if item_id in item_values[hadm_id]:
                value = item_values[hadm_id][item_id]
                if isinstance(value, list):
                    policy = items_of_interest[item_id].policy
                    value, error = merge_policy(value, policy)
                    if error:
                        items = hadm_conflicts.setdefault(hadm_id, {})
                        items[item_id] = (error, value)
                        continue
                if value is not None:
                    row.extend(value)
            else:
                hadm_incomplete.setdefault(hadm_id, [])
                hadm_incomplete[hadm_id].append(item_id)
        if hadm_id not in hadm_conflicts and hadm_id not in hadm_incomplete:
            # This hadm_id has all desired measurements
            rows.append(row)

    with gzip.open(measurements, 'wt', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    return hadm_conflicts, hadm_incomplete

def merge_policy(value, policy):
    ''' Apply policy when more than one measurement is found. '''
    units = ''
    for value_unit in value:
        if value_unit[1]:
            units = value_unit[1]
            if not all([v[1] == units for v in value[1:] if v[1]]):
                return value, 'Different units'

    value_nums = [float(v[0]) for v in value if v[0] != '']
    if policy == 'same':
        if not all([v == value_nums[0] for v in value_nums[1:]]):
            return value, 'Different values for "same" policy.'
        new_value = value_nums[0]
    elif policy == 'mean':
        new_value = np.mean(value_nums)
    elif policy == 'median':
        new_value = np.median(value_nums)
    elif policy == 'min':
        new_value = min(value_nums)
    elif policy == 'max':
        new_value = max(value_nums)
    elif policy == 'first':
        new_value = value_nums[0]
    return (new_value, units), None

def report(items_of_interest, hadm_conflicts, item_values, items_found, hadm_incomplete,
           num_records, show_conflicts):
    ''' Print report of found items and conficting values for the same measurements. '''
    if hadm_conflicts:
        if show_conflicts:
            print(f'{len(hadm_conflicts)} hadm_ids with conficts:')
            for hadm_id, item_conflicts in hadm_conflicts.items():
                del item_values[hadm_id]
                for item_id, item_conflict in item_conflicts.items():
                    error, conflicts = item_conflict
                    print(f'    {error}: hadm_id:{hadm_id:10} item_id:{item_id:10} '
                        f'{items_of_interest[item_id]}')
                    for conflict in conflicts:
                        print(f'        {conflict}')
            print(f'Removed {len(hadm_conflicts)} hadm_ids containing conflicting measurements.')
        else:
            print(f'{len(hadm_conflicts)} hadm_ids with conficts.')
    if hadm_incomplete:
        incomplete_count = 0
        for hadm_id in hadm_incomplete:
            if hadm_id in item_values: # Not removed for conflicts
                del item_values[hadm_id]
                incomplete_count += 1
        if incomplete_count:
            print(f'Removed {incomplete_count} hadm_ids missing some measurements.')

    print(f'Scaned {num_records} records.')

    found = ', '.join([items_of_interest[item_id].header for item_id in items_found])
    print(f'{len(item_values)} hadms records have all values for: {found}')

def parse_arguments():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    example_config = '''
    chart : 'mimic-iii-clinical-database-1.4/CHARTEVENTS.csv.gz'
    items_of_interest : {
        '226512' : Item('Admission Weight (Kg)', 'WEIGHT',              'same'),
        '226707' : Item('Height',                'HEIGHT',              'same'),
        '226537' : Item('Glucose (whole blood)', 'GLUCOSE_WHOLE_BLOOD', 'mean'),
        '220179' : Item('Non Invasive Blood Pressure systolic', 'NON_INVASIVE_BLOOD_PRESSURE_SYSTOLIC', 'mean'),
        '220180' : Item('Non Invasive Blood Pressure diastolic', 'NON_INVASIVE_BLOOD_PRESSURE_DIASTOLIC', 'mean'),
    }
    # An Item has three fields: label, output field, and merge policy.
    # Merge policy is one of: 'same', 'mean', 'median', 'first', 'max', 'min'
    # "same" will discard measurement if values conflict.
    '''
    parser.add_argument("config",
                        help='yaml config file or "-" to read yaml from stdin.\n'
                             f'Example config:\n{example_config}')
    parser.add_argument("--show_conflicts", action='store_true',
                        help='Output conflicts for measurements that require "same" values.')
    parser.add_argument("--debug", action='store_true', help='Output to DEBUG.csv.gz')
    parser.add_argument("--max_rows", type=int)
    args = parser.parse_args()

    if args.config == '-':
        raw_data = sys.stdin.read()
    else:
        raw_data = open(args.config).read()
   
    data = yaml.safe_load(raw_data)
    chart = data['chart']
    items_of_interest = data['items_of_interest']

    Item = namedtuple('Item', ['label', 'header', 'policy'])
    for key, value in dict(items_of_interest).items():
        items_of_interest[key] = Item(*value)

    measurements = f'{db_dir}/'
    for item in items_of_interest.values():
        measurements += item.header + '.'
    measurements += 'csv.gz'
    if args.debug:
        measurements = f'{db_dir}/DEBUG.csv.gz'
    elif args.max_rows:
        measurements = f'{args.max_rows}_{measurements}'

    return items_of_interest, chart, measurements, args.max_rows, args.show_conflicts, args.debug

main()
