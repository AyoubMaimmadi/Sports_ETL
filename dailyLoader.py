
import pandas as pd
from sqlalchemy import create_engine

# Creating a database engine
engine = create_engine("postgresql://postgres:postgres@localhost/student_sport_academics_DW")

# Function to read data from a CSV file
def read_csv(file_path):
    return pd.read_csv(file_path)

# Function to read existing data from the database
def read_db_table(table_name):
    return pd.read_sql_table(table_name, engine)

# Function to find new records
def find_new_records(existing_data, new_data, unique_columns):
    # Creating a set of tuples from the existing data for comparison
    existing_ids = set(tuple(row) for row in existing_data[unique_columns].to_records(index=False))
    
    # Function to check if a record in new data is new
    def is_new_record(row):
        return tuple(row[unique_columns].values) not in existing_ids
    
    # Applying the function to filter new records
    new_data['is_new'] = new_data.apply(is_new_record, axis=1)
    return new_data[new_data['is_new']]

# Main function to load data
def load_data(csv_file_path, db_table_name):
    existing_data = read_db_table(db_table_name)
    new_data = read_csv(csv_file_path)
    new_records = find_new_records(existing_data, new_data)
    if not new_records.empty:
        new_records.to_sql(db_table_name, engine, if_exists='append', index=False)
        print(f'Inserted {len(new_records)} new records into {db_table_name}.')
    else:
        print('No new records to insert.')

# Mapping of CSV file paths to database table names
csv_to_table_mapping = {
    'List_of_NCAA_Division_I_institutions.csv': 'school_dim',
    'contact_sports.csv': 'sport_dim',
    'List_of_NCAA_Division_I_institutions.csv': 'location_dim', 
    'NCAA_school_academic_performance.csv': 'date_dim',
    'NCAA_school_academic_performance.csv': 'academic_score_snapshot_fact'
}

# Looping over the mapping and loading data for each table
for csv_file, table_name in csv_to_table_mapping.items():
    load_data(csv_file, table_name)

