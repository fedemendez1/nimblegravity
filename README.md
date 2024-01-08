# nimblegravity
Data challenge for the Data Scientist position at Nimble Gravity

##########

PostgreSQL Database has already been created.

Credentials to the database can be found in 'data_ingestion.py'.

##########
FIRST SCRIPT TO RUN: 'data_ingestion.py'

The script has three .csv as dependencies which have been downloaded from:
https://www.bls.gov/data/#employment

'women_in_government.csv'
Contains monthly time series from 1964 through 2023 of thousands of women employees in the government. The data is seasonalized. The serie id is CES9000000010.

'all_employees_private.csv'
Contains monthly time series from 1939 through 2023 of thousands of employees in the private sector. The data is seasonalized. The serie id is CES0500000001.

'all_employees_private.csv'
Contains time series from 1964 through 2023 of thousands of production employees in the private sector. The data is seasonalized. The serie id is CES0500000006.

data_ingestion.py will:
1) create temporary tables in the database to store the raw contents of the .csvs (.csv's are read through chunks to save memory).

2) create a store procedure that transforms the raw data and stores transformed data into permanent tables.

3) the transformed data lives in two tables:

ces.women_in_government
Contains date as yyyy-mm-dd with dd as start of the month;
women in government (thousands of employees)

ces.production_supervisory_ratio
Contains date as yyyy-mm-dd with dd as start of the month;
ratio 'production employees / supervisory employees'

##########
SECOND AND THIRD SCRIPTS TO RUN: 'api_women_in_government.py' & 'api_production_supervisory_ratio.py'

The contents of each of the tables can be exposed in JSON format through REST API by running these two scripts.

API scripts has one file dependency 'nimblegravity.conf' that is required to run the PostgREST server.