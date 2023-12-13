import pandas as pd
import sqlalchemy
import random
from ETL_pipeline import loader_data

# Function to generate surrogate keys
def generate_surrogate_keys(start, end, count):
    return random.sample(range(start, end), count)

'''
# Function to get the primary key column of a table
The SQL query retrieves the column names that constitute the primary key of a specified PostgreSQL table using the system catalog tables pg_index and pg_attribute. It filters columns based on their inclusion in the primary key index (indisprimary). The result is a list of attribute names representing the primary key columns of the specified table.
'''
def get_primary_key_column(engine, table):
    query = f"SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attnum = ANY(i.indkey) WHERE i.indrelid = '{table}'::regclass AND i.indisprimary;"
    result = pd.read_sql(query, engine)
    return result.iloc[0, 0] if not result.empty else None

# Function to add new rows to a dimension table
def insert_new_rows(engine, table, df, surrogate_key_col):
    primary_key_col = get_primary_key_column(engine, table)
    
    if primary_key_col is None:
        print(f"Primary key column not found for table {table}. Unable to insert new rows.")
        return

    existing_data = pd.read_sql(f"SELECT {primary_key_col} FROM {table};", engine)
    existing_keys = set(existing_data[primary_key_col])

    new_rows = df[~df[primary_key_col].isin(existing_keys)]

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
insert_new_rows(engine, "date_dim", new_sap_red_df[['ACADEMIC_YEAR']].drop_duplicates(), 'date_key')

# Insert new rows into the location dimension
insert_new_rows(engine, "location_dim", new_school_location_df[['School', 'Common Name', 'Nickname', 'City', 'State', 'Type', 'Subdivision', 'Primary Conference']].drop_duplicates(), 'location_key')

# Insert new rows into the school dimension
insert_new_rows(engine, "school_dim", new_sap_red_df[['SCHOOL_NAME']].drop_duplicates(), 'school_key')

# Insert new rows into the sport dimension
insert_new_rows(engine, "sport_dim", new_sap_red_df[['SPORT_NAME']].drop_duplicates(), 'sport_key')

# Insert new rows into the fact table
insert_new_rows(engine, "academic_score_snapshot_fact", new_sap_red_df, 'school_key')
