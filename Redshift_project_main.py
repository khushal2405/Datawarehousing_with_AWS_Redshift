#!/usr/bin/env python
# coding: utf-8

# In[62]:


import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('kdb_redshift_dwh.cfg')

# dropping tables from cluster

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES(redshift staging tables + star schema tables)

staging_events_table_create= (
   """
   CREATE TABLE staging_events_table (
      stagingEventId bigint IDENTITY(0,1) PRIMARY KEY,
      artist VARCHAR(500),
      auth VARCHAR(20),
      firstName VARCHAR(500),
      gender CHAR(1),
      itemInSession SMALLINT,
      lastName VARCHAR(500),
      length NUMERIC,
      level VARCHAR(10),
      location VARCHAR(500),
      method VARCHAR(20),
      page VARCHAR(500),
      registration NUMERIC,
      sessionId SMALLINT,
      song VARCHAR,
      status SMALLINT,
      ts BIGINT,
      userAgent VARCHAR(500),
      userId SMALLINT
    )
   """
)

staging_songs_table_create = (
   """
   CREATE TABLE staging_songs_table (
      staging_song_id bigint IDENTITY(0,1) PRIMARY KEY,
      num_songs INTEGER NOT NULL,
      artist_id VARCHAR(20) NOT NULL,
      artist_latitude NUMERIC,
      artist_longitude NUMERIC,
      artist_location VARCHAR(500),
      artist_name VARCHAR(500) NOT NULL,
      song_id VARCHAR(20) NOT NULL,
      title VARCHAR(500) NOT NULL,
      duration NUMERIC NOT NULL,
      year SMALLINT NOT NULL
   );
   """
)

songplay_table_create = (
   """
   CREATE TABLE songplays (
      songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY, 
      start_time BIGINT REFERENCES time(start_time) distkey, 
      user_id SMALLINT REFERENCES users(user_id), 
      level VARCHAR(10), 
      song_id VARCHAR(20) REFERENCES songs(song_id), 
      artist_id VARCHAR(20) REFERENCES artists(artist_id), 
      session_id SMALLINT, 
      location VARCHAR(500), 
      user_agent VARCHAR(500)
   )
   sortkey(level, start_time);
   """
)

user_table_create = (
   """
   CREATE TABLE users (
      user_id INT PRIMARY KEY, 
      first_name VARCHAR(500),
      last_name VARCHAR(500),
      gender CHAR(1),
      level VARCHAR(10) NOT NULL
   )
   diststyle all
   sortkey(level, gender, first_name, last_name);
   """
)

song_table_create = (
   """
   CREATE TABLE songs (
      song_id VARCHAR(20) PRIMARY KEY, 
      title VARCHAR(500) NOT NULL,
      artist_id VARCHAR(20) NOT NULL,
      year SMALLINT NOT NULL,
      duration NUMERIC NOT NULL
   )
   diststyle all
   sortkey(year, title, duration);
   """
)

artist_table_create = (
   """
   CREATE TABLE artists (
      artist_id VARCHAR(20) PRIMARY KEY, 
      name VARCHAR(500) NOT NULL,
      location VARCHAR(500),
      latitude NUMERIC,
      longitude NUMERIC
   )
   diststyle all
   sortkey(name, location);
   """
)

time_table_create = (
   """
   CREATE TABLE time (
      start_time timestamp PRIMARY KEY distkey, 
      hour SMALLINT NOT NULL,
      day SMALLINT NOT NULL,
      week SMALLINT NOT NULL,
      month SMALLINT NOT NULL,
      year SMALLINT NOT NULL,
      weekday SMALLINT NOT NULL
   )
   sortkey(year, month, day);
   """
)


# In[98]:


# inserting json data from S3 to Redshift statging tables

staging_events_copy = (
   """
   copy staging_events_table (
      artist, auth, firstName, gender,itemInSession, lastName, 
      length, level, location, method, page, registration, 
      sessionId, song, status, ts, userAgent, userId
   )
   from {}
   credentials 'aws_iam_role={}'
   region 'ap-south-1' format as JSON 'auto'
   timeformat as 'epochmillisecs';
   """
).format(config['S3']['log_data'], config['IAM_ROLE']['ARN'])

staging_songs_copy = (
   """
   copy staging_songs_table from {}
   credentials 'aws_iam_role={}'
   region 'ap-south-1' format as JSON 'auto';
   """
).format(config['S3']['song_data'], config['IAM_ROLE']['ARN'])


# In[99]:


# Now, takin data from staging tables and inserting it into Facts and Dimentions tables(STAR SCHEMA).

songplay_table_insert = (
   """
   INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, 
                          session_id, location, user_agent)
   SELECT se.ts, se.userId, se.level, sa.song_id, sa.artist_id, se.sessionId, 
          se.location, se.userAgent 
   FROM staging_events_table se
   JOIN (
         SELECT s.song_id AS song_id, a.artist_id AS artist_id, s.title AS song, 
         a.name AS artist, s.duration AS length 
         FROM songs s
         JOIN artists a ON s.artist_id=a.artist_id
   ) sa 
   ON se.song=sa.song AND se.artist=sa.artist AND se.length=sa.length; 
   """
)

user_table_insert = (
   """
   INSERT INTO users (user_id, first_name, last_name, gender, level)
   SELECT userId, firstName, lastName, gender, level
   FROM (
         SELECT userId, firstName, lastName, gender, level,
         ROW_NUMBER() OVER (PARTITION BY userId
                            ORDER BY firstName, lastName,
                            gender, level) AS user_id_ranked
         FROM staging_events_table
         WHERE userId IS NOT NULL
   ) AS ranked
   WHERE ranked.user_id_ranked = 1;
   """
)

song_table_insert = (
   """
   INSERT INTO songs (song_id, title, artist_id, year, duration)
   SELECT song_id, title, artist_id, year, duration
   FROM (
         SELECT song_id, title, artist_id, year, duration,
         ROW_NUMBER() OVER (PARTITION BY song_id
                            ORDER BY title, artist_id,
                            year, duration) AS song_id_ranked
         FROM staging_songs_table
         WHERE song_id IS NOT NULL
   ) AS ranked
   WHERE ranked.song_id_ranked = 1;
   """
)

artist_table_insert = (
   """
   INSERT INTO artists (artist_id, name, location, latitude, longitude)
   SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
   FROM (
         SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude,
         ROW_NUMBER() OVER (PARTITION BY artist_id
                            ORDER BY artist_name, artist_location,
                            artist_latitude, artist_longitude) AS artist_id_ranked
         FROM staging_songs_table
         WHERE artist_id IS NOT NULL
   ) AS ranked
   WHERE ranked.artist_id_ranked = 1;
   """
)


time_table_insert = (
   """
   INSERT INTO time (start_time, hour, day, week, month, year, weekday)
   SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
         EXTRACT(HOUR FROM start_time) AS hour,
         EXTRACT(DAY FROM start_time) AS day,
         EXTRACT(WEEK FROM start_time) AS week,
         EXTRACT(MONTH FROM start_time) AS month,
         EXTRACT(YEAR FROM start_time) AS year,
         EXTRACT(DOW FROM start_time) AS weekday
   FROM staging_events_table
   WHERE ts IS NOT NULL;
   """
)


# In[100]:


create_table_queries = [staging_events_table_create, staging_songs_table_create, 
                        user_table_create, song_table_create, artist_table_create,
                        time_table_create,songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                     songplay_table_drop, user_table_drop, song_table_drop,
                     artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert, songplay_table_insert]


# In[101]:


import configparser
import psycopg2
#from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Description: Drops each table using the queries in `drop_table_queries` list in sql_queries.

    Arguments:
        cur: the cursor object. 
        conn: connection object to redshift. 

    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description: Creates each table using the queries in `create_table_queries` list in sql_queries.

    Arguments:
        cur: the cursor object. 
        conn: connection object to redshift. 

    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
     Description:
        - Establishes connection with the sparkify database and gets
        cursor to it (on AWS redshift cluster created earlier).  
        
        - Drops all the tables.  
        
        - Creates all tables needed. 
        
        - Finally, closes the connection.

    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('kdb_redshift_dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
    


# In[102]:


import configparser
import psycopg2
#from sql_queries import copy_table_queries, insert_table_queries, copy_staging_order
                        #count_staging_queries, insert_table_order, count_fact_dim_queries


# In[103]:


def load_staging_tables(cur, conn):
    """
    Description: Copies data in json format in S3 to staging tables in redshift.

    Arguments:
        cur: the cursor object. 
        conn: connection object to redshift. 

    Returns:
        None
    """
    p = 0
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        
        print('No. of rows copied into {}'.format(p))
        p=p+1


# In[105]:


def insert_tables(cur, conn):
    """
    Description: ETL from staging tables to songplays fact and its dimension 
                 tables in redshift.

    Arguments:
        cur: the cursor object. 
        conn: connection object to redshift. 

    Returns:
        None
    """
    p=0
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print('No. of rows inserted {}'.format(p))
        p=p+1


def main():
    """
    Description:
        - Establishes connection with the sparkify database and gets
        cursor to it (on AWS redshift cluster created earlier).
        
        - Loads staging tables from raw log and song files to redshift database
        
        - From staging tables, perform ETL to songplays fact and its dimension 
          tables in redshift using SQL
    
    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('kdb_redshift_dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()


# In[ ]:





# In[ ]:




