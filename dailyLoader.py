import pandas as pd
import sqlalchemy
import random
from datetime import datetime, timedelta
from ETL_pipeline import loader_data, sqldf
from datetime import datetime, timedelta

# Current date
current_date = datetime(2023, 1, 1)

# Example timestamps for each case
academic_year_timestamp = current_date - timedelta(days=365)  # One year ago
location_timestamp = current_date - timedelta(days=180)  # Six months ago
school_timestamp = current_date - timedelta(days=90)  # Three months ago
sport_timestamp = current_date - timedelta(days=30)  # One month ago
fact_table_timestamp = current_date - timedelta(days=7)  # One week ago


# Function to generate surrogate keys
def generate_surrogate_keys(start, end, count):
    return random.sample(range(start, end), count)

# Function to get the latest timestamp from a table
def get_latest_timestamp(table, column, engine):
    query = f"SELECT MAX({column}) FROM {table};"
    result = pd.read_sql(query, engine)
    return result.iloc[0, 0]  # Assuming a single result

# Function to add new rows to a dimension table
def insert_new_rows(engine, table, df, surrogate_key_col, timestamp_col):
    latest_timestamp = get_latest_timestamp(table, timestamp_col, engine)
    
    if pd.isnull(latest_timestamp):
        latest_timestamp = datetime(1970, 1, 1)  # A default date in the past

    new_rows = df[df[timestamp_col] > latest_timestamp]
    
    if not new_rows.empty:
        new_rows[surrogate_key_col] = generate_surrogate_keys(10000, 99999, len(new_rows))
        new_rows.to_sql(name=table, con=engine, schema="student_ath", if_exists="append", index=False)
        print(f"Inserted {len(new_rows)} new rows into {table}.")
    else:
        print(f"No new rows to insert into {table}.")

# Function to load new data from CSV files
def load_new_data():
    # Load existing dataframes using ETL_pipeline
    sap_red_df, school_location_df, contact_sports_df = loader_data()

    # Load CSV files with new data (replace 'your_file_path' with the actual file path)
    new_sap_red_df = pd.read_csv('NCAA_school_academic_performance_new_data.csv')
    new_school_location_df = pd.read_csv('List_of_NCAA_Division_I_institutions_new_data.csv')
    new_contact_sports_df = pd.read_csv('contact_sports_new_data.csv')

    return sap_red_df, school_location_df, contact_sports_df, new_sap_red_df, new_school_location_df, new_contact_sports_df

# Load existing dataframes using ETL_pipeline
sap_red_df, school_location_df, contact_sports_df = loader_data()

# Load new data from CSV files
new_sap_red_df, new_school_location_df, new_contact_sports_df = load_new_data()

# Create or connect to the PostgreSQL database engine
engine = sqlalchemy.create_engine("postgresql://postgres:postgres@localhost/student_sport_academics_DW")

# Insert new rows into the date dimension
insert_new_rows(engine, "date_dim", new_sap_red_df[['ACADEMIC_YEAR']].drop_duplicates(), 'date_key', academic_year_timestamp)

# Insert new rows into the location dimension
insert_new_rows(engine, "location_dim", new_school_location_df[['School', 'Common Name', 'Nickname', 'City', 'State', 'Type', 'Subdivision', 'Primary Conference']].drop_duplicates(), 'location_key', location_timestamp)

# Insert new rows into the school dimension
insert_new_rows(engine, "school_dim", new_sap_red_df[['SCHOOL_NAME']].drop_duplicates(), 'school_key', school_timestamp)

# Insert new rows into the sport dimension
insert_new_rows(engine, "sport_dim", new_sap_red_df[['SPORT_NAME']].drop_duplicates(), 'sport_key', sport_timestamp)

# Insert new rows into the fact table
insert_new_rows(engine, "academic_score_snapshot_fact", new_sap_red_df, ['SCHOOL_ID', 'ACADEMIC_YEAR', 'SPORT_CODE'], fact_table_timestamp)

# Note: Replace 'your_timestamp_column' with the actual timestamp column in your dataframes
