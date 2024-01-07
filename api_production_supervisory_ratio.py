#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Import necessary libraries
import psycopg2
import pandas as pd
import requests
import time
import subprocess
import sys
print("Imported libraries")


# In[3]:


# This code consults data in the table 'production_supervisory_ratio' within the postgreSQL database through the REST API
# pre-requisite file: 'nimblegravity.conf'


# Function to check if PostgREST server is running
def is_server_running():
    try:
        response = requests.get('http://localhost:3000')
        return response.status_code == 200
    except requests.ConnectionError:
        return False

# Check if the server is running
if is_server_running():
    print("PostgREST server is already running.")
else:
    print("Starting PostgREST server...")
    # Start PostgREST using configuration file
    subprocess.Popen(['postgrest', 'nimblegravity.conf'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Wait for the server to start (adjust the delay based on your server's startup time)
    time.sleep(5)

    # Check if the server is running after starting
    if is_server_running():
        print("PostgREST server started.")
    else:
        print("Failed to start PostgREST server.")
        sys.exit(1)  # Exit the script if the server failed to start

# URL for the API endpoint
url = 'http://localhost:3000/production_supervisory_ratio'

# GET request to retrieve data from the API endpoint
response = requests.get(url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Display the response content in a JSON
    print(response.json())
else:
    # Print an error message if the request was not successful
    print('Failed to retrieve data. Status code:', response.status_code)
    
subprocess.run(['pkill', '-f', 'postgrest'])
print("The PostgREST server has been disconnected.")

