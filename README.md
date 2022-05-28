# Datawarehousing_with_AWS_Redshift
Data Warehousing using AWS Redshift

In this project we use ETL pipeline to populate the sparkifydb database in AWS Redshift.

The purpose of this database is to enable Sparkify to answer business questions it may have of its users, the types of songs they listen to and the artists of those songs using the data that it has in logs and files. The database provides a consistent and reliable source to store this data.

This source of data will be useful in helping Sparkify reach some of its analytical goals, for example, finding out songs that have highest popularity or times of the day which is high in traffic.

For the schema design, the STAR schema is used as it simplifies queries and provides fast aggregations of data.


######## ETL design ##############
For the ETL pipeline, Python is used as it contains libraries such as pandas, that simplifies data manipulation. It also allows connection to Postgres Database.

There are 2 types of data involved, song and log data. For song data, it contains information about songs and artists, which we extract from and load into users and artists dimension table

First, we load song and log data from JSON format in S3 into our staging tables (staging_songs_table and staging_events_table)

Next, we perform ETL using SQL, from the staging tables to our fact and dimension tables.

