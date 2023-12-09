# loader.py
import random
import sqlalchemy
import ETL_pipeline
import pandas as pd

# Initialize counter and generate surrogate keys
i = 0
surrogate_key_list = random.sample(range(10000, 99999), 1000)

# Load dataframes using ETL_pipeline
sap_red_df, school_location_df, contact_sports_df = ETL_pipeline.loader_data()

# Create date dimension
date_dim = pd.DataFrame(sap_red_df['year'].drop_duplicates().reset_index(drop=True))
date_dim['date_key'] = 0

# Assign surrogate keys to date dimension
date_dim['date_key'] = surrogate_key_list[i:i+len(date_dim)]
i += len(date_dim)

# Create location dimension
location_dim = pd.DataFrame(school_location_df[['school_name', 'school_conference', 'school_type']].drop_duplicates().reset_index(drop=True))
location_dim['location_key'] = 0

# Assign surrogate keys to location dimension
location_dim['location_key'] = surrogate_key_list[i:i+len(location_dim)]
i += len(location_dim)

# Create school dimension
school_dim = pd.DataFrame(sap_red_df['school_name'].drop_duplicates().reset_index(drop=True))
school_dim['school_key'] = 0

# Assign surrogate keys to school dimension
school_dim['school_key'] = surrogate_key_list[i:i+len(school_dim)]
i += len(school_dim)

# Create sport dimension
sport_dim = pd.DataFrame(sap_red_df[['gender', 'sport']].drop_duplicates().reset_index(drop=True))
sport_dim['sport_key'] = 0

# Assign surrogate keys to sport dimension
sport_dim['sport_key'] = surrogate_key_list[i:i+len(sport_dim)]
i += len(sport_dim)

# Query to join school_dim and school_location_df using pandas merge
school_dim = pd.merge(school_dim, school_location_df[['school_name', 'school_conference', 'school_type']], on='school_name', how='inner')

# Query to join sport_dim and contact_sports_df
sport_dim_q = '''
    SELECT sp.sport_key,
            sp.gender,
            sp.sport,
            c.contact_sport
    FROM sport_dim sp
    INNER JOIN contact_sports_df c
    ON sp.sport = c.sport;
'''
sport_dim = ETL_pipeline.sqldf(sport_dim_q, locals())

# Create fact table from dimension tables and original raw table
fact_tbl_query = '''
    SELECT d.date_key,
           l.location_key,
           sc.school_key,
           sp.sport_key,
           st.num_athletes,
           st.academic_score
    FROM sap_red_df st
    INNER JOIN date_dim d ON st.year = d.year
    INNER JOIN location_dim l ON st.school_name = l.school_name
    INNER JOIN school_dim sc ON st.school_name = sc.school_name
    INNER JOIN sport_dim sp ON st.gender = sp.gender AND st.sport = sp.sport;
'''

academic_score_snapshot_fact = ETL_pipeline.sqldf(fact_tbl_query, locals())

# Drop 'school_name' column from location_dim
location_dim = location_dim.drop('school_name', axis=1)

# Loading dimension/fact tables into PostgreSQL database
# USERNAME and PASSWORD are specific to your postgreSQL account
engine = sqlalchemy.create_engine("postgresql://postgres:postgres@localhost/student_sport_academics_DW")

tables = [date_dim, location_dim, school_dim, sport_dim, academic_score_snapshot_fact]
table_names = ["date_dim", "location_dim", "school_dim", "sport_dim", "academic_score_snapshot_fact"]

# Insert data into PostgreSQL tables
for i in range(len(tables)):
    tables[i].to_sql(name=table_names[i], con=engine, schema="student_ath", if_exists="append", index=False)
