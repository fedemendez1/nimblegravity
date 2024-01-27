#!/usr/bin/env python
# coding: utf-8

# # Python Challenge - Federico Mendez

# In[87]:


# Import libraries
import boto3
import concurrent.futures
import dask.dataframe as dd
import dask.array as da
from botocore.exceptions import ClientError
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import fastparquet
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs
from prefixspan import PrefixSpan
from sklearn.preprocessing import LabelEncoder
import pyarrow.parquet as pq
import snowflake.connector
from scipy.stats import kruskal


# In[46]:


# AWS credentials
aws_access_key = 'AKIASQRQO2RJDCVD7SAA'
aws_secret_key = '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'
aws_region = 'us-east-2'
s3_path = 's3://ng-data-science-interviews/clickstream2'

# Local Path
local_dir = '/Users/fmendez/Desktop/nimblegravity/'
local_path = '/Users/fmendez/Desktop/nimblegravity/'


# # Download .parquet files

# In[18]:


# Initialize the S3 client
s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)
bucket, key = s3_path.split('//')[1].split('/', 1)

def download_file(s3_path, local_path):
    bucket, key = s3_path.split('//')[1].split('/', 1)
    s3.download_file(bucket, key, local_path)
    print(f'Successfully downloaded {s3_path} to {local_path}')

# List objects in the specified S3 location
response = s3.list_objects(Bucket=bucket, Prefix=key)

# Create a ThreadPoolExecutor for parallel downloading
with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = []

    for obj in response.get('Contents', []):
        s3_path = f's3://{bucket}/{obj["Key"]}'
        local_path = f'{local_dir}{obj["Key"].split("/")[-1]}'
        futures.append(executor.submit(download_file, s3_path, local_path))

    # Wait for all downloads to complete and print progress
    for future in concurrent.futures.as_completed(futures):
        try:
            future.result()
        except Exception as e:
            print(f'Download failed: {e}')


# # Import dataset
# 
# All the .parquet files were saved in a table in Snowflake. The ETL was done with SQL and another table was created in Snowflake with the new structure.
# 
# In the next chunk I am just importing to pandas the data necessary to do the analysis, in particular with sessions that contain at least a click in TripAdvisor.

# In[47]:


# Snowflake connection parameters
snowflake_params = {
    'user': 'famendez7',
    'password': 'ng2024FEDE',
    'account': 'rg81407.sa-east-1.aws',
#     'warehouse': 'fede',
    'database': 'NG',
#     'schema': 'python_challenge',
#     'role': 'accountadmin',
}

# Create a connection object
conn = snowflake.connector.connect(**snowflake_params)

# Create a cursor object
cur = conn.cursor()

# Execute a SQL query to fetch data from the table
sql_query = 'alter warehouse FEDE resume;'
cur.execute(sql_query)
sql_query = 'SELECT count(*) as count_rows FROM python_challenge.data;'
cur.execute(sql_query)

# Fetch the results into a pandas DataFrame
df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
rows_raw_data = df.iloc[0, 0]
print(f"Number of rows complete raw data (48 .parquet files): {rows_raw_data}")

sql_query = """
SELECT *
FROM python_challenge.df_restructured
WHERE session_id IN (
    SELECT DISTINCT session_id
    FROM python_challenge.df_restructured
    WHERE LOWER(url) LIKE '%tripadvisor.com%'
);
"""

cur.execute(sql_query)
df_restructured = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
df_restructured = df_restructured.rename(columns=lambda x: x.lower())
print(df_restructured.head(20))
# Close the cursor and connection
cur.close()
conn.close()


# # Data Wrangling

# ### Creating functions: remove_outliers, extract_search_terms and has_utm

# In[56]:


# Creates an outlier cleaner function
def remove_outliers(df, column_name):
    Q1 = df[column_name].quantile(0.25)
    Q3 = df[column_name].quantile(0.75)
    IQR = Q3 - Q1

    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    df_no_outliers = df.loc[(df[column_name] >= lower_bound) & (df[column_name] <= upper_bound)]

    
    return df_no_outliers

# Creates a function to extract search terms from Google and Amazon URLs
def extract_search_terms(url):
    parsed_url = urlparse(url)
    
    # Extract search terms based on the website
    if 'google.com' in parsed_url.netloc:
        query_params = parse_qs(parsed_url.query)
        search_terms = query_params.get('q', [])
    elif 'amazon.com' in parsed_url.netloc:
        query_params = parse_qs(parsed_url.query)
        search_terms = query_params.get('k', []) or query_params.get('field-keywords', [])
    else:
        # Add conditions for other websites if needed
        search_terms = []
    
    return ' '.join(search_terms)

# Creates a function to detect UTM in URLs
def has_utm(url):
    # Parse the URL
    parsed_url = urlparse(url)

    # Extract the query parameters
    query_params = parse_qs(parsed_url.query)

    # Check if UTM parameters are present
    utm_keys = ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content']
    has_utm_params = any(key in query_params for key in utm_keys)

    return has_utm_params


# ### Additional Data Wrangling 

# In[112]:


# Converted only sessions_id
sessions_with_tripadvisor = (
    df_restructured
    .loc[df_restructured['url']
    .str.contains('tripadvisor.com', case=False), 'session_id']
    .unique()
)

# Filter sessions that led to Tripadvisor
filtered_data = (
    df_restructured
    .assign(domain=lambda x: x['url'].apply(lambda y: urlparse(y).hostname))
    .loc[lambda x: 
        (x['session_id'].isin(sessions_with_tripadvisor))
    ]
    .sort_values(by=['userid','eventtimestamp','transition_id','url_type'])
    .assign(
        seconds=lambda x: (x.groupby('session_id')['eventtimestamp'].shift(-1) - x['eventtimestamp']).dt.total_seconds().fillna(0),
        cumulative_seconds=lambda x: x.groupby('session_id')['seconds'].transform(lambda x: x.cumsum())
    )
)

# Agregates consecutive repeated domains
simplified_filtered_data = (
    filtered_data
    .loc[lambda x: 
        (x['cum_clicks'] == 1)
    ]
)

#Categorize domains
# categories = {
#     'TripAdvisor': filtered_data['domain'].str.contains('tripadvisor.com',na=False).values,
#     'SEM': filtered_data['domain'].str.contains('google.com|search.yahoo.com|bing.com|duckduckgo.com',na=False).values,
#     'Travel': filtered_data['domain'].str.contains('booking|expedia|kayak|viator|airbnb|google.com/travel/',na=False).values,
#     'Mail': filtered_data['domain'].str.contains('mail',na=False).values,
#     'Social': filtered_data['domain'].str.contains('facebook|instagram|tiktok|linkedin|snapchat|pinterest|twitter',na=False).values,
#     'Streaming & Video': filtered_data['domain'].str.contains('netflix|youtube',na=False).values,
#     'E-commerce': filtered_data['domain'].str.contains('amazon|ebay',na=False).values,
#     'Yelp': filtered_data['domain'].str.contains('yelp',na=False).values,
# }

# category_labels = np.array(list(categories.keys()))
# category_conditions = np.array(list(categories.values()))

df = pd.read_csv('/Users/fmendez/Desktop/nimblegravity/categories.csv')

categories = {}
for _, row in df.iterrows():
    domain = row['domain']
    category = row['category']

    if category not in categories:
        categories[category] = [domain]
    else:
        categories[category].append(domain)
        
category_conditions = [filtered_data['domain'].str.contains('|'.join(domains), case=False, na=False) for domains in categories.values()]
category_labels = list(categories.keys())

# Add extra columns to DF
filtered_data = filtered_data.copy()
filtered_data.loc[:, 'domain_category'] = np.select(category_conditions, category_labels, default='Other')
filtered_data.loc[:, 'search'] = filtered_data['url'].apply(extract_search_terms)
filtered_data.loc[:, 'has_utm'] = filtered_data['url'].apply(has_utm)


# # Data Analysis 

# ## Clicks on TripAdvisor by session
# 
# In more than 50% of the sessions, users perform only one click in TripAdvisor.
# This behaviour suggests low stickiness and engagement, although is expected as it's known as website with high bounce rate.

# In[74]:


# Filter rows where domain is 'www.tripadvisor.com'
clicks_by_session = filtered_data[filtered_data['domain'] == 'www.tripadvisor.com']

# Clicks by session
clicks_by_session = (
    clicks_by_session
    .groupby('session_id') 
    .size()
    .reset_index(name='clicks')
    .sort_values(by='clicks', ascending=False)
)
    
#Removing outliers
clicks_by_session_no_outliers = remove_outliers(clicks_by_session,'clicks')

# Creating a histogram
plt.figure(figsize=(10, 6))
plt.hist(clicks_by_session_no_outliers['clicks'], bins=5, color='darkblue', edgecolor='black')
plt.title("Clicks on Tripadvisor by Session")
plt.xlabel("Clicks")
plt.ylabel("Sessions")
plt.show()


# ## Last Click to TripAdvisor 
# 
# Search websites are the pages sending more traffic to TripAdvisor.
# 
# Future analysis: expand the domain theme categorization to have more insigths on what kind of pages are included in "Other".

# In[75]:


# Las Click Domain Themes
df_pairs = (
    filtered_data.assign(
        from_domain=lambda x: x['domain_category'],
        to_domain=lambda x: x['domain_category'].shift(-1)
    )
    .groupby(['from_domain', 'to_domain'])
    .size()
    .reset_index(name='n_count_pairs')
    .loc[lambda x: 
        (x['to_domain'] == 'TripAdvisor') &
        (x['from_domain'] != 'TripAdvisor')
    ]
    .sort_values(by='n_count_pairs', ascending=False)
)

#Create a bar chart
plt.figure(figsize=(15, 8))
sns.barplot(x='from_domain', y='n_count_pairs', data=df_pairs, palette='viridis',order=df_pairs['from_domain'])
plt.title("Referrer links to TripAdvisor")
plt.xlabel("Domain Category")
plt.ylabel("Clicks")
plt.xticks(rotation=45, ha='right')
plt.show()


# ## Distribution of monthly last clicks by domain theme
# 
# I wanted to see if some domains drive more clicks to TripAdvisor during some particular months. For example, Travel websites my drive more clicks when holiday season is close.
# 
# At least with this categorization this is not proven, however would be interested to improve the categorization to test the hypothesis with more certainty.

# In[76]:


month = (
    filtered_data.assign(
        from_domain=lambda x: x['domain_category'],
        to_domain=lambda x: x['domain_category'].shift(-1),
        month=lambda x: x['eventtimestamp'].dt.month)
    .loc[lambda x: 
        (x['to_domain'] == 'TripAdvisor') &
        (x['from_domain'] != 'TripAdvisor')
    ]
    .groupby(['month', 'from_domain'])
    .size()
    .reset_index(name='monthly_count')
    .sort_values(by='month', ascending=False)
)

# Pivot the DataFrame to have categories as columns
pivot_df = month.pivot(index='month', columns='from_domain', values='monthly_count').fillna(0)

# Normalizing data to account for daily seasonality (hours of sleep, work, etc)
normalized_df = pivot_df.div(pivot_df.sum(axis=1), axis=0)

# Plot the stacked bar chart
normalized_df.plot(kind='bar', stacked=True, figsize=(15, 8))
plt.title('Clicks by Month and Category')
plt.xlabel('Month')
plt.ylabel('Clicks')
plt.legend(title='Category', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.show()


# ## First Click to TripAdvisor
# 
# Search websites are the pages that mostly start the journey of users to TripAdvisor

# In[77]:


# Most common starting points for conversion sessions
result_df = (
    filtered_data
    .loc[~filtered_data['domain'].isin(['Start', 'Direct', 'End', 'www.tripadvisor.com'])]
    .groupby('session_id')
    .apply(lambda x: x.loc[x['transition_id'].idxmin()])
    .groupby(['domain', 'domain_category'])['session_id']
    .nunique()
    .reset_index(name='n_first_click')
    .sort_values(by='n_first_click', ascending=False)
    .head(20)

)
    
# Chart of number of sessions started by domain category
plt.figure(figsize=(10, 6))
plt.bar(result_df['domain_category'], result_df['n_first_click'], color = 'darkblue')
plt.xlabel('Domain Theme')
plt.ylabel('Sessions')
plt.title('First Click Conversions by Domain Theme')
plt.xticks(rotation=45, ha='right')
plt.show()


# ## Transition Probabilities
# 
# In a Markov model, which is a model used for marketing attribution, a transition probability matrix shows the probabilities associated with various state changes.
# 
# In this context, it tell us that is the probability that one person moves from Page X to Page Y.
# 
# If we focus on TripAdvisor, we can see for instance, that if a user landed in TA, the probability that they will move to a Search page is 24%, and the probability that they will stay in TA is 46%.
# 
# We see that staying in the same state (domain) is the most frequent scenario, no matter in which domain you are in, this is in line with the usual websites navigation behaviour of users. 

# In[78]:


#Create pairs DataFrame
dataframe = (
    filtered_data.assign(
        from_domain=lambda x: x['domain_category'],
        to_domain=lambda x: x['domain_category'].shift(-1)
    )
    .groupby(['from_domain', 'to_domain'])
    .agg(n_count_pairs=('session_id','count'),
         n_count_sessions=('session_id', 'nunique'))
    .reset_index()
    .merge(
        filtered_data
        .groupby('domain_category')
        .agg(n_count_from=('session_id','count'))
        .reset_index(),
        left_on='from_domain',
        right_on='domain_category',
        how='left'
    )
    .assign(transition_probability=lambda x: x['n_count_pairs'] / x['n_count_from'])
    .sort_values(by='n_count_sessions', ascending=False)
)

# Filter top pairs
top_values = 20
top_pairs = dataframe[~dataframe['from_domain'].isin(['Start', 'Direct', 'End']) & ~dataframe['to_domain'].isin(['Start', 'Direct', 'End'])].head(top_values)

# Get unique domains
vector = pd.unique(top_pairs[['from_domain', 'to_domain']].values.ravel('K'))

# Filter df based on unique domains and additional conditions
dataframe = (
    dataframe[dataframe['from_domain'].isin(['start', 'direct', 'end'] + list(vector)) &
       dataframe['to_domain'].isin(['start', 'direct', 'end'] + list(vector))]
    .assign(transition_probability=lambda x: x['n_count_pairs'] / x['n_count_from'])
    .loc[
        lambda x: ~(
            (x['from_domain'] == 'End') & (x['to_domain'] == 'Start') &
            (x['from_domain'] == 'Start') & (x['to_domain'] == 'Direct')
        )
    ]
)

# Create Transition Probabilities Matrix
transition_matrix = dataframe.pivot(index='from_domain', columns='to_domain', values='transition_probability')
transition_matrix.fillna(0, inplace=True)

# Heatmap showing Transition Probabilities
plt.figure(figsize=(15, 8))
sns.heatmap(transition_matrix, annot=True, cmap="YlGnBu")
plt.title("Transition Probabilities")
plt.xlabel("TO")
plt.xticks(rotation=90, ha='right')
plt.ylabel("FROM")
plt.yticks(rotation=0, ha='right')
plt.show()


# ## Assistances
# 
# The chart below shows which domains assist the most in converted sessions. Basically, of all sessions that land on TA at some point, what are the domains that participated the most before the user landed on TA.
# 
# We see Search again as the most important domain followed by Social and Travel.

# In[79]:


# Domains assistance to conversions

# Filter out post-conversion clicks
pre_conversion = filtered_data.loc[::-1, 'domain'].eq('www.tripadvisor.com').groupby(filtered_data['session_id']).cummax()

filtered_data = filtered_data[pre_conversion]

#Assuming df_restructured is your DataFrame

result_df = (
    filtered_data
    .loc[~filtered_data['domain'].isin(['Start', 'Direct', 'End', 'www.tripadvisor.com'])]
    .groupby(['domain', 'domain_category'])
    .agg(assistances=('session_id', 'nunique'))
    .reset_index()
    .sort_values(by='assistances', ascending=False)
    .head(50)
)

# Bar chart showing in how many sessions each of the domain categories participated
plt.figure(figsize=(15, 8))
plt.bar(result_df['domain_category'], result_df['assistances'],color = 'darkblue')
plt.xlabel('Domain Theme')
plt.ylabel('Assistances')
plt.title('Assistances by Domain Theme')
plt.xticks(rotation=45, ha='right')
plt.show()


# ## Session Duration
# 
# Shows distribution of session duration until the point of conversion (reaching TripAdvisor).

# In[80]:


# Session duration (that lead to Tripadvisor)

# Filter out post-conversion clicks
pre_conversion = filtered_data.loc[::-1, 'domain'].eq('www.tripadvisor.com').groupby(filtered_data['session_id']).cummax()
result_df = filtered_data[pre_conversion]

idx = result_df.groupby('session_id')['eventtimestamp'].idxmax()
result_df = result_df.loc[idx]
result_df['cumulative_minutes'] = result_df['cumulative_seconds'].apply(lambda x: x / 60)

result_df_no_outliers = remove_outliers(result_df, 'cumulative_minutes')

# Plot a histogram without outliers    
plt.figure(figsize=(15, 8))
plt.hist(result_df_no_outliers['cumulative_minutes'], bins=60, color='darkblue', edgecolor='black')
plt.title("Session Duration")
plt.xlabel("Time Spent (minutes)")
plt.ylabel("Sessions")
plt.legend()
plt.show()

# Plot a boxplot without outliers
plt.figure(figsize=(15, 8))
sns.boxplot(x=result_df_no_outliers['cumulative_minutes'],color = 'darkblue')
plt.title('Session Duration')
plt.xlabel('Time Spent (minutes)')
plt.show()


# ## Path Pattern finding: an approach with PrefixSpan algorithm
# 
# The algorithm helps finding common patterns between pages. Minimum % of sessions can be set to define "common", as well as the length of the paths.
# 
# Google Search -> TA is the most frequent being present in 71% of sessions.

# In[105]:


# Path Pattern finding

# Filter out post-conversion clicks
pre_conversion = simplified_filtered_data.loc[::-1, 'domain'].eq('www.tripadvisor.com').groupby(simplified_filtered_data['session_id']).cummax()
simplified_filtered_data = simplified_filtered_data[pre_conversion]

# Create a LabelEncoder instance
label_encoder = LabelEncoder()

# Fit and transform the 'domain' column to assign numerical labels
simplified_filtered_data.loc[:,'page_id'] = label_encoder.fit_transform(simplified_filtered_data['domain'])

result_df = (
    simplified_filtered_data
    .groupby('session_id')['page_id']
    .agg(path=lambda x: ','.join(map(str, x)))
    .reset_index()
)

sequences = [list(map(int, path.split(','))) for path in result_df['path']]

# Set minimum support threshold in terms of percentage of conversion sessions
percentage_of_sessions = 0.2
min_support = len(sequences) * percentage_of_sessions

# Apply PrefixSpan algorithm
patterns = PrefixSpan(sequences).frequent(min_support)

# Filter out patterns with repeated consecutive elements
# patterns = [
#     pattern for pattern in patterns
#     if not any(pattern[1][i] == pattern[1][i + 1] for i in range(len(pattern[1]) - 1))
# ]

# Restrict the length of the patterns
filtered_patterns = [pattern for pattern in patterns if 2 <= len(pattern[1]) <= 5]

# Show only top patterns
top = 30
top_patterns = sorted(filtered_patterns, key=lambda x: x[0], reverse=True)[:top]

# Inverse transform numerical labels back to page names
inverse_mapping = dict(zip(label_encoder.transform(simplified_filtered_data['domain']), simplified_filtered_data['domain']))

# Print the discovered patterns
for pattern in top_patterns:
    pages_names = [inverse_mapping[page_id] for page_id in pattern[1] if inverse_mapping.get(page_id) is not None]
    print(f"Pattern: {', '.join(pages_names)}, Support: {int((pattern[0] / len(sequences)) * 100)} % of sessions")


# ## Longest Journey: deepdive
# 
# ##### Analyzing longest session in terms of clicks:
# User started session by checking email, landed on a Walmart mail and then started playing a game called neopets (reached neopets directly, not referred). Through the game in neopets they looked for guidance in a page called jellyneo, related to the game. The user had some activity in Tumblr. Some movements of pages within neopets happen exactly every 60 seconds, so maybe there is an auto refresh in place in that domain and doesn't mean the user is engaged. There's also some college research activity. User searching for cost per semester in google. Some job reasearch at a page called onetonline.com. Judging for google search, interested in barchelors in microbiology and medicine. Neopets still refreshing in the back. Some addresses searched in google maps. Apparently they are also looking for apartments where they are planning to study (apartments.com).
# 
# ##### Main insight:
# Basically the longest session in terms of clicks is a result of auto-refreshes on a game webpage called neopets. It's good to know to we can detect this website behaviour in the future and filter the sessions that do not represent user true engagement.

# In[115]:


# Longest Journey in terms of clicks
longest_session_id = filtered_data.groupby('session_id')['transition_id'].nunique().idxmax()
longest_session_clicks = filtered_data[filtered_data['session_id'] == longest_session_id]

# Longest Journey in terms of unique pages visited
longest_session_id = filtered_data.groupby('session_id')['domain'].nunique().idxmax()
longest_session_pages = filtered_data[filtered_data['session_id'] == longest_session_id]

# Creates a search column with search key terms to help with the journey comprehension
longest_session_clicks.loc[:, 'search'] = longest_session_clicks['url'].apply(extract_search_terms)

selected_columns = ['userid','eventtimestamp', 'url', 'url_type', 'domain', 'search', 'cum_clicks', 'seconds', 'cumulative_seconds']
df_selected = longest_session_clicks.loc[:, selected_columns]
# Display
pd.set_option('display.max_rows', None)
display(df_selected)


# # Monthly Retention of TripAdvisor users
# 
# The chart shows the monthly retention of TA users. Only 13% of users who enter TA website on their first time, then come back the following month. Sounds reasonable given TA usecase is quite sporadic, an average user migth only enter when planning a trip, maybe once or twice a year.
# 
# Further exploration: one thing I did not do but would be interested is group TA pages into categories and see if there are difference in retention behavior across categories. Maybe restaurant pages have higher retention as people may dine out more frequently than booking hotels or visiting a new city.

# In[83]:


# TripAdvisor Monthly Retention

retention_data = (
    filtered_data
    .loc[lambda x: 
        (x['domain'] == 'www.tripadvisor.com')
    ]
)

# Calculate the month from the first event for each user
retention_data['days_from_start'] = (retention_data['eventtimestamp'] - retention_data.groupby('userid')['eventtimestamp'].transform('min')).dt.days

# Create a DataFrame to store monthly retention percentages
monthly_retention_df = pd.DataFrame(index=range(13), columns=['month','retention','users'])
monthly_retention_df.loc[:, 'month'] = range(13)
monthly_retention_df.loc[0, 'retention'] = 100
monthly_retention_df.loc[0, 'users'] = retention_data['userid'].nunique()


# # Calculate monthly retention percentages
for i in list(range(1, 13)):
    users_in_month_i = retention_data[(retention_data['days_from_start'] >= i * 30) & (retention_data['days_from_start'] <= (i * 30) + 30)]['userid'].nunique()
    monthly_retention_df.loc[i, 'users'] = users_in_month_i

monthly_retention_df.loc[1:, 'retention'] = (monthly_retention_df['users'].iloc[1:] / retention_data['userid'].nunique()) * 100

# Convert DataFrame columns to NumPy arrays
month = monthly_retention_df['month'].to_numpy()
retention = monthly_retention_df['retention'].to_numpy()

# Plot the line chart
plt.figure(figsize=(15, 6))
plt.plot(month, retention, marker='o', linestyle='-', color='b')
plt.title('Monthly Retention Chart')
plt.xlabel('Month')
plt.ylabel('Retention (%)')
plt.grid(True)
plt.show()


# ## Time spent in TripAdvisor by session
# 
# Chart showing the distribution of time spent in TA by session. The median is 1 minute.

# In[84]:


# Group by session_id and calculate time_spent
time_in_tripadvisor = (
    filtered_data
    .loc[lambda x: 
        (x['domain'] == 'www.tripadvisor.com')
    ]
    .groupby('session_id')
    .agg(time_spent=('seconds','sum'))
    .reset_index()
)
    
time_in_tripadvisor_no_outliers = remove_outliers(time_in_tripadvisor,'time_spent')

# Calculate the median time_spent
# median_time_spent = time_in_tripadvisor['time_spent'].median()
# mean_time_spent = time_in_tripadvisor['time_spent'].mean()

# print("Median time spent: {:.2f}".format(median_time_spent))
# print("Mean time spent: {:.2f}".format(mean_time_spent))

plt.figure(figsize=(15, 6))
plt.hist(time_in_tripadvisor_no_outliers['time_spent'], bins=50, color='darkblue', edgecolor='black')
plt.title("Time Spent in Tripadvisor per session")
plt.xlabel("Time Spent (seconds)")
plt.ylabel("Sessions")
plt.show()


# ## Correlation between time spent in TripAdvisor and the referrer domain
# 
# The test and the chart shows there is correlation between the time spent in TA and the referrer domain. The hypothesis is that some domains such as Travel websites (Booking.com or Airline websites) may be correlated with a higher level of engagement because it is likely that the user has just booked a flight or a hotel so the interest could be higher that if they come from mail or social.
# 
# Although the test rejects the null hypothesis that the time spent are the same, judging by the boostrap analysis and the boxplots we cannot conclude yet and more exploration would be needed.
# 
# Future analysis: for future analysis it would be interested to work on a bag of words model to look for correlation between the search keywords used right before entering TA and the time spent/clicks in TA. Also it would help us break down the SEM into diverse search behavior.

# In[88]:


engagement_in_tripadvisor = (
    filtered_data
    .loc[lambda x: 
        (x['cum_clicks'] == 1) &
        (x['domain'].shift(-1) == 'www.tripadvisor.com') &
        (x['domain'] != 'www.tripadvisor.com')
    ]
)

selected_columns = ['session_id', 'domain_category']
domains = engagement_in_tripadvisor.loc[:, selected_columns]

# Merge dataframes on 'session_id'
merged_df = pd.merge(domains, time_in_tripadvisor_no_outliers, on='session_id')

# Ensure 'time_spent' is of numeric type
merged_df['time_spent'] = pd.to_numeric(merged_df['time_spent'], errors='coerce')

# Kruskal-Wallis test
result = kruskal(*[group['time_spent'].dropna() for name, group in merged_df.groupby('domain_category')])
# Kruskal-Wallis test
result = kruskal(*[group['time_spent'] for name, group in merged_df.groupby('domain_category')])

# Display the result
print("Kruskal-Wallis H-test:")
print(result)


# In[92]:


# Function to perform bootstrapping
def bootstrap_sample(data):
    return np.random.choice(data, size=len(data), replace=True)

# Number of bootstrap samples
num_samples = 2000
bootstrap_results = pd.DataFrame()

merged_df = merged_df[merged_df['domain_category'] != 'TripAdvisor']
grouped_data = merged_df.groupby('domain_category')['time_spent']

# Bootstrap samples for each domain and store in the results DataFrame
for domain, data in grouped_data:
    bootstrap_samples = [bootstrap_sample(data) for _ in range(num_samples)]
    bootstrap_results[domain] = np.mean(bootstrap_samples, axis=1)

df_long = pd.melt(bootstrap_results, var_name='domain', value_name='time_spent')

# Create a boxplot
plt.figure(figsize=(15, 6))
sns.boxplot(x='domain', y='time_spent', data=df_long)
plt.title('Time Spent in TA by Referrer Domain')
plt.xlabel('Themes')
plt.ylabel('Time Spent')
plt.show()


# In[ ]:




