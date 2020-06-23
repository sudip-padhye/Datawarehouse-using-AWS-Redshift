# Redshift Data Warehouse
This project is concerned with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables (ie. Star schema using distribution strategies for optimal performance)

## Project Datasets
There are 2 sources of this project which reside on S3. Here are the S3 links for each:
1. Song data: s3://udacity-dend/song_data
2. Log data: s3://udacity-dend/log_data

### Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. 
For example, here are filepaths to two files in this dataset.

> song_data/A/B/C/TRABCEI128F424C983.json

> song_data/A/A/B/TRAABJL12903CDCF1A.json

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

> {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

### Log Dataset
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

>log_data/2018/11/2018-11-12-events.json

>log_data/2018/11/2018-11-13-events.json

And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.

<img src="log-data.png" alt="drawing" width="800" height="600"/>

## Schema for Song Play Analysis
Using the song and event datasets, star schema is created, which is optimized for queries on song play analysis. This includes the following tables.

### Fact Table
1. songplays - records in event data associated with song plays i.e. records with page NextSong
> songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
1. users - users in the app
>user_id, first_name, last_name, gender, level

2. songs - songs in music database
> song_id, title, artist_id, year, duration

3. artists - artists in music database
> artist_id, name, location, lattitude, longitude

4. time - timestamps of records in songplays broken down into specific units
> start_time, hour, day, week, month, year, weekday

<img src="Database Schema.PNG" alt="drawing" width="800" height="600"/>

## Requirements

You'll need an [AWS](https://aws.amazon.com/) account with the resources to provision a Redshift cluster (recommended: 4 x dc2.large EC2 instances)

You'll also need this software installed on your system 
* [PostgreSQL](https://www.postgresql.org/download/)
* [Python](https://www.python.org/downloads/)

In addition you'll need the PostgreSQL python driver which can be obtained via `pip`
```
pip install psycopg2 
```

## Project Template
The project includes four files:
1. create_table.py - is where fact and dimension tables for the star schema in Redshift are created.
2. etl.py - is where data is loaded from S3 into staging tables on Redshift and then processed that data into analytics tables (Star schema) on Redshift.
3. sql_queries.py - is where SQL statements are defined, which will be imported into the two other files above.
4. dwh.cfg - this is a configuration file which includes AWS credentials, cluster details, IAM details, S3 details and Datawarehouse details.

## Quick Start
1. Provision a Redshift cluster within AWS utilizing either the Quick Launch wizard, [AWS CLI](https://docs.aws.amazon.com/cli/index.html), or the various AWS SDKs (e.g. [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) for python).

2. Update the credentials and configuration details in 'dwh.cfg' for [AWS] & [DWH] groups.

3. Execute the jupyter file for creating a Redshift cluster.

4. Once, cluster is created, record the cluster details into the 'dwh.cfg' for the remaining sections [CLUSTER] & [IAM_ROLE]. (Don not change the S3 credentials as this are they contain the source details). 

5. Create the database tables by running the 'create_tables.py' script.
> python create_tables.py

6. Execute 'etl.py' to perform the data loading.
> python etl.py

7. You can use Query Editor in the AWS Redshift console for checking the table schemas in your redshift database.

8. Optionally a PostgreSQL client (or `psycopg2`) can be used to connect to the Sparkify db to perform analytical queries afterwards. Alternatively, you can use the Redshift Query editor to fire the analytical queries.

Eg.- 
i. Top 5 users ranked according to their app usage
> SELECT u.first_name, u.last_name, count(*) usage_frequency
FROM songplays sp
JOIN users u ON u.user_id = sp.user_id
GROUP BY u.first_name, u.last_name
ORDER BY usage_frequency DESC
LIMIT 5;

ii. Top Artist whose songs users listen more frequently
> SELECT a.name artist_name, count(*) hits
FROM songplays sp
JOIN artists a ON a.artist_id = sp.artist_id
GROUP BY artist_name
ORDER BY hits DESC
LIMIT 5;

9. Delete your redshift cluster when finished.