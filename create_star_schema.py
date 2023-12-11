import psycopg2

# SQL queries
init_q = '''DROP TABLE IF EXISTS student_ath.academic_score_snapshot_fact,
                                student_ath.location_dim,
                                student_ath.school_dim,
                                student_ath.sport_dim,
                                student_ath.date_dim;
            DROP SCHEMA IF EXISTS student_ath;
            CREATE SCHEMA IF NOT EXISTS student_ath
            AUTHORIZATION postgres;'''

create_date_dim_q = '''
    CREATE TABLE student_ath.date_dim ( 
        date_key integer PRIMARY KEY,
        year integer
);
'''

create_location_dim_q = '''
    CREATE TABLE student_ath.location_dim ( 
        location_key integer PRIMARY KEY,
        school_name varchar(100),
        school_conference varchar(100),
        school_type varchar 
    );
'''

create_school_dim_q = '''
    CREATE TABLE student_ath.school_dim ( 
       school_key integer PRIMARY KEY,
       school_name varchar(100),
       school_conference varchar(100),
       school_type varchar
);
'''

create_sport_dim_q = '''
    CREATE TABLE student_ath.sport_dim ( 
       sport_key integer PRIMARY KEY,
       sport varchar(100),
       gender varchar(5),
       contact_sport char(1)
);
'''

create_fact_tbl_q = '''
    CREATE TABLE student_ath.academic_score_snapshot_fact ( 
       date_key integer,
       location_key integer,
       sport_key integer,
       school_key integer,
       academic_score integer,
       num_athletes integer,
       PRIMARY KEY (date_key, location_key, sport_key, school_key),
       FOREIGN KEY (date_key) REFERENCES student_ath.date_dim(date_key),
       FOREIGN KEY (location_key) REFERENCES student_ath.location_dim(location_key),
       FOREIGN KEY (school_key) REFERENCES student_ath.school_dim(school_key),
       FOREIGN KEY (sport_key) REFERENCES student_ath.sport_dim(sport_key)
);
'''

queries = [init_q, create_date_dim_q, create_location_dim_q, create_school_dim_q, create_sport_dim_q, create_fact_tbl_q]

# Database connection details
db_name = "student_sport_academics_DW"
username = "postgres"
password = "lina2015"

try:
    # Establish database connection
    conn = psycopg2.connect(host="localhost", dbname=db_name, user=username, password=password)
    cur = conn.cursor()

    # Loop through queries and execute each one
    for query in queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(f"Error executing query: {query}")
            print(f"Error message: {e}")
            conn.rollback()

except psycopg2.Error as e:
    print(f"Unable to connect to the database. Error: {e}")
finally:
    # Close the database connection
    if conn:
        conn.close()
        print("\nCreated all the star schemas.")
        print("Database connection closed.")