'''
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