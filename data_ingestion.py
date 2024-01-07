#!/usr/bin/env python
# coding: utf-8

# In[2]:


# Import necessary libraries
import psycopg2
import pandas as pd
import requests
import time
import subprocess
import sys
print("Imported libraries.")


# In[5]:


# This code ingest data of Women in Government into PostgreSQL
# files pre-requisites: women_in_government.csv

# Connect to PostgreSQL database
try:
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="findi",
        host="localhost",
        port="5444"
    )
    print("Connection to PostegreSQL successful.")
except psycopg2.OperationalError as e:
    print(f"Unable to connect to PostgreSQL: {e}")


# Create a cursor
cur = conn.cursor()

# Create a temporary table 'women_in_government_temp' where raw data of Employment of Women in Government will be stored
cur.execute('''
    CREATE TEMP TABLE IF NOT EXISTS women_in_government_temp (
        year TEXT,
        month TEXT,
        value_in_thousands INTEGER
    );
''')

# Create a stored procedure that will transform data from the temporary table 'women_in_government_temp' and insert transformed data into a final 'women_in_government' table
# The stored procedure will also delete the table completely before inserting data (this avoids data duplication)
cur.execute('''
    CREATE OR REPLACE PROCEDURE transform_data()
    LANGUAGE plpgsql
    AS $$
    BEGIN
        DELETE FROM ces.women_in_government;

        CREATE TABLE IF NOT EXISTS ces.women_in_government (
            date DATE,
            value_in_thousands INTEGER
        );

        INSERT INTO ces.women_in_government (date, value_in_thousands)
        SELECT TO_DATE(year || '-' || RIGHT(month,2) || '-01', 'YYYY-MM-DD'),
               value_in_thousands
        FROM women_in_government_temp;
    END;
    $$;
''')

# Read the CSV file in chunks of 50 rows and insert data into the temporary table
chunk_size = 50
for chunk in pd.read_csv('women_in_government.csv', chunksize=chunk_size):
    # Extract values
    values = [tuple(row) for row in chunk[['year', 'month', 'value_in_thousands']].values]

    # Insert values into the temporary table
    cur.executemany('''
        INSERT INTO women_in_government_temp (year, month, value_in_thousands)
        VALUES (%s, %s, %s);
    ''', values)
    print(f"Inserted {len(values)} rows into 'women_in_government_temp' table.")

# Use the stored procedure 'transform_data' to transform and insert data into final 'women_in_government' table
cur.execute('CALL transform_data();')

# Validates whether data was inserted in the table
cur.execute('SELECT COUNT(*) FROM ces.women_in_government;')
row_count = cur.fetchone()[0]

if row_count > 0:
    print("Data inserted successfully into 'women_in_government' table.")
else:
    print("No data inserted.")

# Commit changes and close connection
conn.commit()
print("Transaction committed.")
cur.close()
print("Cursor closed.")
conn.close()
print("Connection closed.")


# In[6]:


# This code ingest data of Production/Supervisory Employees Ratio into PostgreSQL
# files pre-requisites: all_employees_private.csv and production_employees_private.csv

# Connect to PostgreSQL database
try:
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="findi",
        host="localhost",
        port="5444"
    )
    print("Connection to PostegreSQL successful.")
except psycopg2.OperationalError as e:
    print(f"Unable to connect to PostgreSQL: {e}")

# Create a cursor
cur = conn.cursor()

# Create two temporary tables 'all_employees_private_temp' and 'production_employees_private_temp'
# These tables will store the raw data coming from CES: All employees and Production Employees of the private sector
cur.execute('''
    CREATE TEMP TABLE IF NOT EXISTS all_employees_private_temp (
        year TEXT,
        month TEXT,
        value_in_thousands INTEGER
    );

    CREATE TEMP TABLE IF NOT EXISTS production_employees_private_temp (
        year TEXT,
        month TEXT,
        value_in_thousands INTEGER
    );
''')

# Create a stored procedure that will transform data from the temporary tables to create the ratio
# In order to create the ratio, we assume All employees = Production Employees + Supervisory Employees ;
# Supervisory Employees = All Employees - Production Employees ;
# Ratio = Production Employees / Supervisory Employees ;
# The stored procedure will also delete the table completely before inserting data (this avoids data duplication)
cur.execute('''
    CREATE OR REPLACE PROCEDURE transform_data_ratio()
    LANGUAGE plpgsql
    AS $$
    BEGIN
        DELETE FROM ces.production_supervisory_ratio;

        CREATE TABLE IF NOT EXISTS ces.production_supervisory_ratio (
            date DATE,
            ratio NUMERIC
        );

        INSERT INTO ces.production_supervisory_ratio (date, ratio)
        SELECT TO_DATE(CONCAT(a.year, '-', RIGHT(a.month,2), '-01'), 'YYYY-MM-DD') AS date,
               CASE
                   WHEN CAST(a.value_in_thousands AS NUMERIC) - CAST(p.value_in_thousands AS NUMERIC) = 0 THEN NULL
                   ELSE ROUND(CAST(p.value_in_thousands AS NUMERIC) / (CAST(a.value_in_thousands AS NUMERIC) - CAST(p.value_in_thousands AS NUMERIC)), 2)
               END AS ratio
        FROM all_employees_private_temp a
        INNER JOIN production_employees_private_temp p 
        ON a.year = p.year
        AND a.month = p.month;
    END;
    $$;
''')

# Read the CSV file in chunks of 50 rows and insert data into the temporary tables
chunk_size = 50
for chunk in pd.read_csv('all_employees_private.csv', chunksize=chunk_size):
    # Extract values
    values = [tuple(row) for row in chunk[['year', 'month', 'value_in_thousands']].values]

    # Insert values into the temporary table
    cur.executemany('''
        INSERT INTO all_employees_private_temp (year, month, value_in_thousands)
        VALUES (%s, %s, %s);
    ''', values)
    print(f"Inserted {len(values)} rows into 'all_employees_private_temp' table.")

for chunk in pd.read_csv('production_employees_private.csv', chunksize=chunk_size):
    # Extract values
    values = [tuple(row) for row in chunk[['year', 'month', 'value_in_thousands']].values]

    # Insert values into the temporary table
    cur.executemany('''
        INSERT INTO production_employees_private_temp (year, month, value_in_thousands)
        VALUES (%s, %s, %s);
    ''', values)
    print(f"Inserted {len(values)} rows into 'production_employees_private_temp' table.")

# Use the stored procedure 'transform_data_ratio' to transform and insert data into final 'production_supervisory_ratio' table
cur.execute('CALL transform_data_ratio();')    
    
# Validates whether data was inserted in the table
cur.execute('SELECT COUNT(*) FROM ces.production_supervisory_ratio;')
row_count = cur.fetchone()[0]
    
if row_count > 0:
    print("Data inserted successfully into 'production_supervisory_ratio' table.")
else:
    print("No data inserted.")
    
# Commit changes and close connection
conn.commit()
print("Transaction committed.")
cur.close()
print("Cursor closed.")
conn.close()
print("Connection closed.")

