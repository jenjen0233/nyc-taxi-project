# NYC Taxi Analytics Pipeline

An end-to-end data pipeline for ingesting, transforming, and analyzing NYC taxi trip data using modern data engineering tools and Google Cloud Platform.

## Project Overview

This project demonstrates a production-ready analytics engineering pipeline that:
* Ingests millions of NYC taxi trip records from public datasets
* Stores raw data efficiently in Google Cloud Storage
* Transforms data using dbt (data build tool) with dimensional modeling
* Loads clean, modeled data into BigQuery for analysis
* Provides automated data quality testing and validation
