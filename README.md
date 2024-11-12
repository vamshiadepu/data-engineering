# Project Title

Sparkify ETL Pipeline

## Description

This ETL pipeline extracts Sparkify data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for the analytics team to continue finding insights into what songs Sparkify users are listening to.

As part of this project, following tables are created

Staging tables for loading data from S3:
* staging_songs
* staging_events

Star Schema tables:
* songplays
* songs 
* artists 
* time
* users


### Executing program

* git clone https://github.com/vamshiadepu/data-engineering
* Export key and sercrets
   * export KEY=<your_aws_key>
   * export SECRET=<your_aws_secret>
* Fill in dwh.cfg CLUSTER section. 
* Set up database and amazon redshift
   * Run aws_setup.py for creating redshift cluster  
* Run create_tables.py for creating required staging and star schema tables
* Run etl.py for loading staging tables and star schema tables created
* Run aws_setup.py with parameter --clean_up as True for cleaning up the AWS resources

## Authors
Vamshi Adepu

## References
* Udacity Data Engineering with AWS Course

