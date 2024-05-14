# multinational-retail-data-centralisation728

This project is the first of two industry-grade projects completed with AiCore. The scenario which the project aims to simulate is the following:

A multinational company finds its data spread across many different data sources, making it not easily accesible or analysable by its team. They would like this data to be easily accesible from one location, to act as a single source of truth for all of its sales data, and for this data to be queried to provide some metrics.

The setup for the project is spread across three files: database_utils, data_extraction, and data_cleaning.

The data gathering part of this project involved extracting and cleaning data from:
- An AWS RDS database
- A pdf document stored in an S3 container, for which I used the Python package Tabula
- An API, for which I used the package FastAPI
- A CSV file stored in an S3 bucket, for which I used the package boto3 to download the file

After retrieving the data from these sources, which were either in a CSV, a JSON, or stored as text in a pdf, I cleaned them using pandas and uploaded them to a SQL database on my machine. I updated the columns in this database by casting types and adding categorical entries (e.g. categories for products by weight). I defined primary keys and foreign keys which would connect all of the tables around the orders_table into a star-based schema.

The last step was to query the data on pgadmin, which can be found in the file sql_queries.sql (although this is not a script but a record of the queries i submitted).
