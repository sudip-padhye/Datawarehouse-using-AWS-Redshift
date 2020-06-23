import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ARN = config.get("IAM_ROLE","ARN")
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop =  "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop =       "DROP TABLE IF EXISTS songplays"
user_table_drop =           "DROP TABLE IF EXISTS users"
song_table_drop =           "DROP TABLE IF EXISTS songs"
artist_table_drop =         "DROP TABLE IF EXISTS artists"
time_table_drop =           "DROP TABLE IF EXISTS time"


# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE staging_events(
    artist_name         VARCHAR,
    auth                VARCHAR,
    first_name          VARCHAR,
    gender              VARCHAR,
    itemInSession       INT,
    last_name           VARCHAR,
    length              FLOAT,
    level               VARCHAR,
    location            VARCHAR,
    method              VARCHAR,
    page                VARCHAR,
    registration        FLOAT,
    sessionId           INT,
    song                VARCHAR,
    status              INT,
    ts                  double precision,
    userAgent           VARCHAR,
    userID              INT
    )
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    song_id             VARCHAR,
    num_songs           INT,
    title               VARCHAR,
    artist_name         VARCHAR,
    artist_latitude     FLOAT,
    year                INT,
    duration            FLOAT,
    artist_id           VARCHAR,
    artist_longitude    FLOAT,
    artist_location     VARCHAR
    )
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id INT IDENTITY(0,1) PRIMARY KEY DISTKEY, 
    start_time  TIMESTAMP not null SORTKEY REFERENCES TIME(START_TIME), 
    user_id     VARCHAR not null REFERENCES USERS(USER_ID), 
    level       VARCHAR, 
    song_id     VARCHAR not null REFERENCES SONGS(SONG_ID), 
    artist_id   VARCHAR not null REFERENCES ARTISTS(ARTIST_ID), 
    session_id  VARCHAR not null, 
    location    VARCHAR, 
    user_agent  VARCHAR) 
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id    VARCHAR PRIMARY KEY SORTKEY DISTKEY, 
    first_name VARCHAR, 
    last_name  VARCHAR, 
    gender     VARCHAR, 
    level      VARCHAR)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id   VARCHAR PRIMARY KEY SORTKEY DISTKEY, 
    title     VARCHAR, 
    artist_id VARCHAR not null REFERENCES ARTISTS(ARTIST_ID), 
    year      INT, 
    duration  INT)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id  VARCHAR PRIMARY KEY SORTKEY DISTKEY, 
    name       VARCHAR, 
    location   VARCHAR, 
    lattitude  FLOAT, 
    longitude  FLOAT)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP PRIMARY KEY SORTKEY DISTKEY, 
    hour       INT, 
    day        INT, 
    week       INT, 
    month      INT, 
    year       INT, 
    weekday    INT)
""")


# STAGING TABLES
staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    json {}
    region 'us-west-2'
""").format(LOG_DATA, IAM_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2'
""").format(SONG_DATA, IAM_ARN)


# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time, 
        user_id, 
        level, 
        song_id, 
        artist_id, 
        session_id, 
        location, 
        user_agent
    )
    SELECT DISTINCT timestamp 'epoch' + se.ts/1000 * interval '1 second' AS ts, 
		se.userid, 
        se.level, 
        s.song_id, 
        a.artist_id, 
        se.sessionid, 
        se.location, 
        se.useragent
    FROM staging_events se
    LEFT JOIN songs s ON s.title = se.song
    LEFT JOIN artists a ON a.name = se.artist_name and s.artist_id = a.artist_id 
    WHERE se.page='NextSong'
""")

user_table_insert = ("""
    INSERT INTO users 
    SELECT DISTINCT userid, first_name, last_name, gender, level
    FROM staging_events
    WHERE userid IS NOT NULL AND page='NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday
    )
    SELECT DISTINCT ts_convert.ts, 
    	            date_part(hour, ts_convert.ts)  as hour,
                    date_part(day, ts_convert.ts)   as day,
                    date_part(week, ts_convert.ts)  as week_of_year,
                    date_part(month, ts_convert.ts) as month,
                    date_part(year, ts_convert.ts)  as year,
    	            date_part(dow, ts_convert.ts)   as dow
    FROM
        (select timestamp 'epoch' + ts/1000 * interval '1 second' AS ts
        FROM staging_events
        ) ts_convert
""")


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
