# Overview:

Welcome to the ingester repository, built by Wyatt and Henry.
This repository houses multiple ingester files that all (with one exception) 
take data files and write to a database.
For production, this is a MySQL database, for testing it is an H2 database

For general usage, see the top-level script `ingest.py`. This script functions 
as a demo and will pull CSVs from the dummy_data folder and write them to a mysql
database. Simply specify which ingesters as arguments (eg `python ingest.py user card`)


# Testing:

If you have a database filled from the producer repository the tests 
will work straight up.
If not, please use that or inject an h2 dump from that repo
All tests work from the root directory of this repo

## YOU MUST set the H2 environment variable to the h2 jar

