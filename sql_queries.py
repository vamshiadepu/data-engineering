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
                            CREATE TABLE staging_events (
                                staging_events_id  bigint identity(0,1) not null,
                                artist character varying(500),
                                auth character varying(15) , 
                                first_name character varying(50) , 
                                gender character varying(1) ,
                                item_in_session double precision ,
                                last_name character varying(50) ,
                                length character varying(50) ,
                                level character varying(50) ,
                                location character varying(500) ,
                                method character varying(500) ,
                                page character varying(500) ,
                                registration double precision ,
                                session_id double precision ,
                                song character varying(500) , 
                                status double precision , 
                                ts double precision , 
                                user_agent character varying(500) , 
                                user_id double precision
                            );
""")

staging_songs_table_create = ("""
                            create table staging_songs(
                              staging_songs_id  bigint identity(0,1) not null,
                                num_songs double precision,
                                artist_id character varying(50),
                                artist_latitude character varying(500),
                                artist_longitude character varying(500),
                                artist_location character varying(500),
                                artist_name character varying(500),
                                song_id character varying(50),
                                title character varying(500),
                                duration double precision,
                                year int
                            );
""")

songplay_table_create = ("""
                        create table songplays(
                            songplay_id bigint identity(0,1) primay key,
                            start_time timestamp not null sortkey,
                            user_id bigint not null distkey, 
                            level character varying(50), 
                            song_id character varying(50), 
                            artist_id character varying(50),  
                            session_id bigint, 
                            location character varying(500), 
                            user_agent character varying(500)
                         ) diststyle key;
""")

user_table_create = ("""
                    create table users(
                        user_id bigint primary key sortkey, 
                        first_name character varying(50) , 
                        last_name character varying(50) ,  
                        gender char(1), 
                        level character varying(50) 
                     ) diststyle all;
""")

song_table_create = ("""
                    create table songs(
                        song_id character varying(50) primay key sortkey, 
                        title character varying(50), 
                        artist_id character varying(50), 
                        year int, 
                        duration double precision
                     ) diststyle key;
""")

artist_table_create = ("""
                        create table artists(
                            artist_id character varying(50) primary key sortkey, 
                            name character varying(50) , 
                            location character varying(500), 
                            latitude character varying(500), 
                            longitude character varying(500)
                       )diststyle all;
""")

time_table_create = ("""
                    create table time(
                        start_time timestamp primary key sortkey, 
                        hour int, 
                        day char(9), 
                        week smallint, 
                        month smallint, 
                        year smallint, 
                        weekday smallint
                     ) diststyle key;
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
                            insert into songplays as
                            (
                            select 
                                timestamp 'epoch' + (se.ts/1000) * interval '1 second' start_time 
                                ,se.user_id    
                                ,se.level      
                                ,ss.song_id    
                                ,ss.artist_id  
                                ,se.session_id 
                                ,se.location   
                                ,se.user_agent 
                                ,se.artist
                                ,ss.artist_name
                                ,se.song
                                ,se.page
                                ,ss.duration
                                ,se.length
                            from 
                                staging_songs ss
                                , staging_events se
                            where 
                                ss.title = se.song 
                                and ss.artist_name = se.artist  
                            );
""")

user_table_insert = ("""
                    insert into users 
                        select distinct 
                            se.user_id
                            ,se.first_name
                            ,se.last_name
                            ,se.gender
                            ,se.level     
                        from 
                            staging_events se
                        where 
                            se.user_id is not null 
                        group by 
                            se.user_id
                            ,se.first_name
                            ,se.last_name
                            ,se.gender   
                            ,se.level ;
""")

song_table_insert = ("""
                    insert into songs
                        select 
                            ss.song_id
                            ,ss.title
                            ,ss.artist_id
                            ,ss.year
                            ,ss.duration   
                        from 
                            staging_songs ss;
""")

artist_table_insert = ("""
                        insert into artists
                            select 
                                ss.artist_id
                                ,ss.artist_name
                                ,ss.artist_location
                                ,ss.artist_latitude
                                ,ss.artist_longitude    
                            from 
                                staging_songs ss
                            group by   
                                ss.artist_id
                                ,ss.artist_name
                                ,ss.artist_location
                                ,ss.artist_latitude
                                ,ss.artist_longitude;
""")

time_table_insert = ("""
                    insert into time
                        with ts_converted AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM stage_event)
                        select distinct
                            ts,
                            extract(hour from ts),
                            extract(day from ts),
                            extract(week from ts),
                            extract(month from ts),
                            extract(year from ts),
                            extract(weekday from ts)
                        from
                            ts_converted;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
# insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
# create_table_queries = [staging_events_table_create,staging_songs_table_create]
# drop_table_queries = [staging_events_table_drop,staging_songs_table_drop]
# copy_table_queries = [staging_events_copy,staging_songs_copy]
