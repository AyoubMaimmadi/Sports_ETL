import pandas as pd
import requests

# Extraction Process

# Scrape NCAA Division I school data from the Wikipedia table

'''
extraction process:
    - scrape NCAA Division 1 school data from wiki table and put it in a dataframe
    - import csv files (NCAA Division 1 school academic performance/sport contact data) and put in dataframes
'''

url = "https://en.wikipedia.org/wiki/List_of_NCAA_Division_I_institutions"
temp_df_list = pd.read_html(url)
school_location_df = temp_df_list[0].rename(columns={'City': 'city', 'State': 'state', 'School': 'school_name', 'Primary conference': 'school_conference', 'Type': 'school_type'})

# Print school_location_df contents before export (for debugging)
print("school_location_df contents before export:")
print(school_location_df)

# Import CSV files (NCAA Division I school academic performance/sport contact data)
school_academic_perf_df = pd.read_csv('NCAA_school_academic_performance.csv')
contact_sports_df = pd.read_csv('contact_sports.csv').rename(columns={'Sport': 'sport', 'Contact': 'contact_sport'})

# Transformation Process
'''
transformation process:
    school_academic_performance_df
        - sport is listed in format "mens/womens sportname", separate into 2 columns (gender, sport)
        - change gender column from 'men's/women's' to 'M/F'
        - different columns for each year's data, convert to one year column with additional rows for each year
        - rename columns
        - drop unnecessary data (school id, sport code, NCAA division (since all division 1))
'''


# Transform school_academic_performance_df
school_academic_perf_df[['gender', 'sport']] = school_academic_perf_df['SPORT_NAME'].str.split(' ', n=1, expand=True)
school_academic_perf_df.loc[school_academic_perf_df['sport'].isnull(), 'sport'] = school_academic_perf_df['gender']
school_academic_perf_df.loc[school_academic_perf_df['sport'] == school_academic_perf_df['gender'], 'gender'] = 'M'
school_academic_perf_df.loc[school_academic_perf_df['gender'] == "Men's", 'gender'] = 'M'
school_academic_perf_df.loc[school_academic_perf_df['gender'] == "Women's", 'gender'] = 'F'

# Print the first five rows of the transformed DataFrame
print(school_academic_perf_df.head())

# List of years for further processing
year_list = [2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014]

# Convert the table format from one column per year to one column with all years
def year_column_reformat(year_list, school_academic_perf_df):
    result_df = pd.DataFrame()
    for year in year_list:
        temp_df = school_academic_perf_df[['SCHOOL_NAME', 'sport', 'gender', f'{year}_ATHLETES', f'{year}_SCORE']]
        temp_df = temp_df.rename(columns={'SCHOOL_NAME': 'school_name', f'{year}_ATHLETES': 'num_athletes', f'{year}_SCORE': 'academic_score'})
        temp_df['year'] = year
        result_df = pd.concat([result_df, temp_df], ignore_index=True)
    
    return result_df

# Call the function for further formatting
sap_red_df = year_column_reformat(year_list, school_academic_perf_df)

def loader_data():
    return sap_red_df, school_location_df, contact_sports_df

# Uncomment the line below if you want to export the school_location_df DataFrame to an Excel file
contact_sports_df.to_excel('wikitable.xlsx')
