
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import os

# Database connection details (to be filled with actual credentials)
DB_USERNAME = 'your_username'
DB_PASSWORD = 'your_password'
DB_HOST = 'your_host'
DB_PORT = 'your_port'
DB_NAME = 'your_database'

# Creating a database engine
engine = create_engine(f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# Function to read data from a CSV file
def read_csv(file_path):
    return pd.read_csv(file_path)

# Function to read existing data from the database
def read_db_table(table_name):
    return pd.read_sql_table(table_name, engine)

# Function to find new records
def find_new_records(existing_data, new_data):
    # Assuming that there is a unique identifier to compare. This will need to be adjusted based on actual data structure
    existing_ids = set(existing_data['unique_id'])
    new_data['is_new'] = new_data['unique_id'].apply(lambda x: x not in existing_ids)
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

# Example usage (to be adjusted for actual file paths and table names)
# load_data('/path/to/your/csv_file.csv', 'your_table_name')
