import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

S3_LOGDATA= config.get("S3","LOG_DATA")
S3_LOG_JSONPATH= config.get("S3","LOG_JSONPATH")
S3_SONG_DATA= config.get("S3","SONG_DATA")
ROLE_ARN = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplayS"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
                            CREATE TABLE "staging_events" (   
    "artist" character varying(500),
    "auth" character varying(15) , 
    "first_name" character varying(50) , 
    "gender" character varying(1) ,
    "item_in_session" double precision ,
    "last_name" character varying(50) ,
    "length" character varying(50) ,
    "level" character varying(50) ,
    "location" character varying(500) ,
    "method" character varying(500) ,
    "page" character varying(500) ,
    "registration" double precision ,
    "session_id" double precision ,
    "song" character varying(500) , 
    "status" double precision , 
    "ts" double precision , 
    "user_agent" character varying(500) , 
    user_id double precision
);
""")

staging_songs_table_create = ("""
                                create table staging_songs(
                                num_songs double precision,
                                artist_id character varying(500),
                                artist_latitude character varying(500),
                                artist_longitude character varying(500),
                                artist_location character varying(500),
                                artist_name character varying(500),
                                song_id character varying(500),
                                title character varying(500),
                                duration double precision,
                                year double precision
                                );
""")

songplay_table_create = ("""
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
""")

# STAGING TABLES

staging_events_copy = ("""
                        copy staging_events from {}
                        credentials 'aws_iam_role={}'
                        json {} 
                        region 'us-west-2';
""").format(S3_LOGDATA,ROLE_ARN,S3_LOG_JSONPATH)

staging_songs_copy = ("""
                        copy staging_songs from {}
                        credentials 'aws_iam_role={}'
                        format as json 'auto'
                        region 'us-west-2';
""").format(S3_SONG_DATA, ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

# create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
# copy_table_queries = [staging_events_copy, staging_songs_copy]
# insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
create_table_queries = [staging_events_table_create,staging_songs_table_create]
drop_table_queries = [staging_events_table_drop,staging_songs_table_drop]
copy_table_queries = [staging_events_copy,staging_songs_copy]
