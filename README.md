# Sparkify Data Pipelines with Airflow

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines with Apache Airflow - an open-source workflow management platform. Their data resides in Amazon S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app and needs to be processed in Sparkify's data warehouse in Amazon Redshift.

We will take the role to build data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. To achieve our project's goals, we will need to create custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.
