# Date Warehouse project with AWS Redshift

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project builds an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

## Project Description

In this project, we build an ETL pipeline for a database hosted on Redshift. 
This project loads data from S3 to staging tables on Redshift, and then execute SQL statements to create the analytics tables from these staging tables.

## Database Schema and ETL

### Schema

#### Fact table

    * songplays - records in log data associated with song plays i.e. records with page NextSong
    
        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
        
#### Dimension tables

    * users - users in the app
    
        user_id, first_name, last_name, gender, level
        
    * songs - songs in music database
    
        song_id, title, artist_id, year, duration
        
    * artists - artists in music database
    
        artist_id, name, location, latitude, longitude
        
    * time - timestamps of records in songplays broken down into specific units
    
        start_time, hour, day, week, month, year, weekday


### ETL

    * Create tables
    
        ```bash
        python3 create_tables.py
        ```
        
    * Load data from s3 into staging tables and extract anaylitic tables
    
        ```bash
        python3 etl.ipynb
        ```
        
### Example Queries
        
* queries.ipynb
        
  