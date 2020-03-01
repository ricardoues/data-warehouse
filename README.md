# Project: Data Warehouse from Data Engineer Nanodegree

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


## Discussion 

Data warehouses are core components in data science, they are optimized for queries and are used for reporting and data analysis. In this sense, Sparkify need to put attention to the consumer behaviour in order to do the following analysis: 

* Statistics about songs, users, and artists
* Predict customer churn 
* Build recommendation systems 

The schema includes the following tables: 

* Fact Table
    * songplays - records in event data associated with song plays.



