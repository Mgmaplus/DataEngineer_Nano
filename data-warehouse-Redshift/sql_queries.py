import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events ( \
    artist               VARCHAR              sortkey, 
    auth                 VARCHAR(15)         NOT NULL, 
    first_name           VARCHAR(15)                 , 
    gender               VARCHAR(1)                  ,
    iteminsession        INTEGER             NOT NULL, 
    last_name            VARCHAR(15)                 , 
    length               NUMERIC                     , 
    level                VARCHAR(8)          NOT NULL, 
    location             VARCHAR                     ,
    method               VARCHAR(5)          NOT NULL,
    page                 VARCHAR(25)         NOT NULL,
    registration         NUMERIC                     ,
    session_id           INTEGER             NOT NULL,
    song                 VARCHAR              distkey,
    status               INTEGER             NOT NULL,
    ts                   TIMESTAMP           NOT NULL,
    user_agent           VARCHAR                     ,
    user_id              INTEGER             
);
""")

staging_songs_table_create= ("""CREATE TABLE IF NOT EXISTS staging_songs ( \
    num_songs            INTEGER            NOT NULL, 
    artist_id            VARCHAR(25)        NOT NULL sortkey, 
    artist_latitude      NUMERIC                    , 
    artist_longitude     NUMERIC                    ,
    artist_location      VARCHAR                    ,
    artist_name          VARCHAR            NOT NULL, 
    song_id              VARCHAR(25)        NOT NULL, 
    title                VARCHAR            NOT NULL distkey, 
    duration             NUMERIC            NOT NULL,
    year                 INTEGER                  
);
""")


songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays ( \
    songplay_id         INTEGER PRIMARY KEY                    IDENTITY(0,1) sortkey, 
    start_time          TIMESTAMP REFERENCES time(start_time)  NOT NULL, 
    user_id             INT REFERENCES users(user_id)          NOT NULL, 
    level               VARCHAR(8)                             NOT NULL, 
    song_id             VARCHAR(20) REFERENCES songs(song_id)  NOT NULL distkey, 
    artist_id           VARCHAR REFERENCES artists(artist_id)  NOT NULL, 
    session_id          INTEGER                                NOT NULL, 
    location            VARCHAR                                        , 
    user_agent          VARCHAR                                NOT NULL
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users ( \
    user_id             INTEGER PRIMARY KEY                    NOT NULL sortkey, 
    first_name          VARCHAR(15)                            NOT NULL, 
    last_name           VARCHAR(15)                            NOT NULL, 
    gender              VARCHAR(1)                             NOT NULL, 
    level               VARCHAR(8)                             NOT NULL 
);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs ( \
    song_id             VARCHAR(20) PRIMARY KEY                NOT NULL sortkey distkey, 
    title               VARCHAR                                NOT NULL, 
    artist_id           VARCHAR(20)                            NOT NULL, 
    year                INTEGER                                        , 
    duration            NUMERIC                                NOT NULL 
);

""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists ( \
    artist_id           VARCHAR(20) PRIMARY KEY               NOT NULL sortkey, 
    name                VARCHAR                               NOT NULL, 
    location            VARCHAR                                       , 
    latitude            NUMERIC                                       , 
    longitude           NUMERIC
);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time ( \
    start_time          TIMESTAMP PRIMARY KEY                 NOT NULL sortkey, 
    hour                INTEGER                               NOT NULL, 
    day                 INTEGER                               NOT NULL, 
    week                INTEGER                               NOT NULL,  
    month               INTEGER                               NOT NULL, 
    year                INTEGER                               NOT NULL, 
    weekday             INTEGER                               NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = (""" COPY staging_events from {} 
credentials 'aws_iam_role={}'
json {} timeformat 'epochmillisecs' region 'us-west-2';
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""COPY staging_songs from {} 
credentials 'aws_iam_role={}'
json 'auto' region 'us-west-2';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
       SELECT e.ts                    AS start_time,
       e.user_id                      AS user_id,
       e.level                        AS level,
       s.song_id                      AS song_id,
       s.artist_id                    AS artist_id,
       e.session_id                   AS session_id,
       e.location                     AS location,
       e.user_agent                   AS user_agent
FROM staging_events as e
JOIN staging_songs as s ON(e.artist = s.artist_name)
WHERE e.page IN ('NextSong');
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
       SELECT DISTINCT user_id        AS user_id,
       first_name                     AS first_name,
       last_name                      AS last_name,
       gender                         AS gender,
       level                          AS level
FROM staging_events
WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
       SELECT DISTINCT song_id        AS song_id,
       title                          AS title,
       artist_id                      AS artist_id,
       year                           AS year,
       duration                       AS duration
FROM staging_songs;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
       SELECT DISTINCT artist_id      AS artist_id,
       artist_name                    AS name,
       artist_location                AS location,
       artist_latitude                AS latitude,
       artist_longitude               AS longitude
FROM staging_songs;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
       SELECT ts                      AS start_time,
       EXTRACT(hour from ts)          AS hour,
       EXTRACT(day from ts)           AS day,
       EXTRACT(week from ts)          AS week,
       EXTRACT(month from ts)         AS month,
       EXTRACT(year from ts)          AS year,
       EXTRACT(weekday from ts)       AS weekday
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [time_table_insert,songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert]
