import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS song_plays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events(
                                 artist character varying(200), 
                                 auth character varying(25),
                                 firstName character varying(50),
                                 gender character varying(1), 
                                 iteminSession integer, 
                                 lastName character varying(50), 
                                 length double precision, 
                                 level character varying(10), 
                                 location character varying(200),
                                 method character varying(10), 
                                 page character varying(25), 
                                 registration double precision, 
                                 sessionId integer,
                                 song character varying(200), 
                                 status integer NOT NULL, 
                                 ts bigint NOT NULL, 
                                 userAgent character varying(200),
                                 userId integer                                              
                                 )
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs(
                                 song_id character varying(100) NOT NULL,
                                 num_songs integer NOT NULL, 
                                 title character varying(400) NOT NULL,
                                 artist_name character varying(400) NOT NULL, 
                                 artist_latitude real, 
                                 year integer NOT NULL, 
                                 duration double precision NOT NULL, 
                                 artist_id character varying(25) NOT NULL,                  
                                 artist_longitude real, 
                                 artist_location character varying(300)                 
                                 )
""")

songplay_table_create = ("""CREATE TABLE song_plays(
                            song_play_id bigint identity(0,1), 
                            start_time timestamp NOT NULL,
                            user_id integer NOT NULL, 
                            level character varying(10), 
                            song_id character varying(100), 
                            artist_id character varying(25), 
                            session_id integer, 
                            location character varying(100), 
                            user_agent character varying(200)
                            )
""")




user_table_create = ("""CREATE TABLE users(
                        userId integer NOT NULL,
                        firstName character varying(50), 
                        lastName character varying(50), 
                        gender character varying(1), 
                        level character varying(10)                         
                        )
""")



song_table_create = ("""CREATE TABLE songs(
                        song_id character varying(100) NOT NULL, 
                        title character varying(400) NOT NULL,
                        artist_id character varying(25)  NOT NULL, 
                        year integer NOT NULL, 
                        duration double precision NOT NULL
                        )
""")


artist_table_create = ("""CREATE TABLE artists(
                          artist_id character varying(25) NOT NULL, 
                          name character varying(400) NOT NULL, 
                          location character varying(300), 
                          latitude real, 
                          longitude real
                          )
""")



time_table_create = ("""CREATE TABLE time(
                        start_time timestamp, 
                        hour integer,
                        day integer,
                        week integer, 
                        month integer,
                        year integer, 
                        weekday integer 
                        )
""")



# STAGING TABLES


staging_events_copy = ("""COPY staging_events 
                          FROM {}                          
                          credentials 
                          'aws_iam_role=arn:aws:iam::<put_credentials>:role/<put_credentials>'
                          region 'us-west-2'  
                          JSON {}
                      """).format(config.get('S3', 'LOG_DATA'), 
                                  config.get('S3', 'LOG_JSONPATH'))



# The following was a good resource 
# https://knowledge.udacity.com/questions/39352

staging_songs_copy = ("""COPY staging_songs 
                         FROM {}
                         format as json 'auto'
                         credentials  
                         'aws_iam_role=arn:aws:iam::637150515554:role/myRedshiftRole'                                    region 'us-west-2'                                 
                     """).format(config.get("S3", "SONG_DATA"))



# FINAL TABLES

songplay_table_insert = ("""INSERT INTO song_plays
                                       ( start_time, 
                                         user_id,
                                         level, 
                                         song_id, 
                                         artist_id, 
                                         session_id,
                                         location, 
                                         user_agent                           
                                      )
                      
                            SELECT TIMESTAMP 'epoch' + 
                                              e.ts/1000 * interval '1 second', 
                                   e.userId, 
                                   e.level, 
                                   s.song_id, 
                                   s.artist_id, 
                                   e.sessionId, 
                                   e.location, 
                                   e.userAgent
                            FROM staging_events AS e 
                            JOIN staging_songs AS s 
                            ON ( e.song = s.title AND 
                                 e.artist = s.artist_name
                                 )
                            WHERE page='NextSong'
                            
""")


# The following was a good resource
# https://knowledge.udacity.com/questions/58152


user_table_insert = ("""INSERT INTO users 
                        SELECT userId, firstName, lastName, 
                               gender, level 
                        FROM staging_events
                        WHERE page='NextSong'
""")


song_table_insert = ("""INSERT INTO songs
                        SELECT song_id, title, artist_id, 
                               year, duration 
                        FROM staging_songs
""")

artist_table_insert = ("""INSERT INTO artists 
                          SELECT artist_id, artist_name, artist_location, 
                                 artist_latitude, artist_longitude                          
                          FROM staging_songs 
""")


## The following was a good reference 
## https://knowledge.udacity.com/questions/47920

time_table_insert = ("""INSERT INTO time 
                        SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, 
                        EXTRACT(hour from start_time), 
                        EXTRACT(day from start_time), 
                        EXTRACT(week from start_time), 
                        EXTRACT(month from start_time),        
                        EXTRACT(year from start_time),               
                        EXTRACT(weekday from start_time)
                        FROM staging_events
                        WHERE page='NextSong'                        
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]



drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]


copy_table_queries = [staging_events_copy, staging_songs_copy]


insert_table_queries = [songplay_table_insert,  user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
