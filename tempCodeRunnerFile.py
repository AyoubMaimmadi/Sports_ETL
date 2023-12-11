school_dim['school_name'] = school_dim['school_name'].str.strip().str.lower()
school_location_df['school_name'] = school_location_df['school_name'].str.strip().str.lower()

# Print unique values in school_dim and school_location_df
print("Unique schools in school_dim:", school_dim['school_name'].unique())
print("Unique schools in school_location_df:", school_location_df['school_name'].unique())

# Query to join school_dim and school_location_df using pandas merge
school_dim = pd.merge(school_dim, school_location_df[['school_name', 'school_conference', 'school_type']], on='school_name', how='inner')

# Print shape and unique values in school_dim
print("Shape of school_dim after merge:", school_dim.shape)
print("Unique schools in school_dim after merge:", school_dim['school_name'].unique())