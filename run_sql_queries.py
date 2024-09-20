# Import necessary libraries

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
from sqlalchemy import create_engine

# function to load in csv file, create sqlite engine, and a connection (conn)
# takes file as input and returns a dataframe, engine, and conn

def df_sql(file):
    df = pd.read_csv(file)
    engine = create_engine('sqlite:///:memory:')
    df.to_sql('prior_auth', engine, index=False, if_exists='replace')
    # SQLAlchemy allows you to execute queries directly using the Connection object
    conn = engine.connect()
    return df, engine, conn


# use function to define dataframe, engine, and conn for analysis
df, engine, conn = df_sql('synthetic_pa_data.csv')

# Query Analysis

# 1 Count of requests by status
request_query = """
SELECT request_status, COUNT(*) as count
FROM prior_auth
GROUP BY request_status
"""
request_results = pd.read_sql(request_query, conn)
print("Requests by status:")
print(request_results)

# 2 Average processing time by drug
processing_query = """
SELECT drug_name, 
       strftime('%Y-%m', request_date) as month,
       AVG(JULIANDAY(resolution_date) - JULIANDAY(request_date)) as avg_processing_time
FROM prior_auth
WHERE request_status != 'pending'
GROUP BY drug_name, month
ORDER BY avg_processing_time DESC
"""
processing_result = pd.read_sql(processing_query, conn)
print("\nTop 5 drugs by average processing time (in days):")
print(processing_result)

# 3 Approval rate by provider
approval_query = """
SELECT provider_type,
       COUNT(*) as total_requests,
       SUM(CASE WHEN request_status = 'approved' THEN 1 ELSE 0 END) as approved_requests,
       CAST(SUM(CASE WHEN request_status = 'approved' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) as approval_rate
FROM prior_auth
GROUP BY provider_type
ORDER BY approval_rate DESC
LIMIT 10
"""
approval_result = pd.read_sql(approval_query, conn)
print("\nTop 10 providers by approval rate:")
print(approval_result)

# 4 Time trends in Different regions
time_trend_query = """
SELECT
    geographic_region,
    strftime('%Y-%m', request_date) as month,
    COUNT(*) as total_requests,
    SUM(CASE WHEN request_status = 'approved' THEN 1 ELSE 0 END) as approved_requests,
    CAST(SUM(CASE WHEN request_status = 'approved' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) as approval_rate
FROM prior_auth
GROUP BY geographic_region, month
ORDER BY month
"""
time_trend_results = pd.read_sql(time_trend_query, conn)
print("Time trends in Midwest requests:")
print(time_trend_results)

# 5 Geographic variations in approval rates
geographic_query = """
SELECT 
    geographic_region,
    COUNT(*) as total_requests,
    SUM(CASE WHEN request_status = 'approved' THEN 1 ELSE 0 END) as approved_requests,
    CAST(SUM(CASE WHEN request_status = 'approved' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) as approval_rate
FROM prior_auth
GROUP BY geographic_region
ORDER BY approval_rate DESC
"""
geographic_results = pd.read_sql(geographic_query, conn)
print("\nGeographic variations in approval rates:")
print(geographic_results)

# 6 Correlation between patient age group and request outcomes
age_correlation_query = """
SELECT 
    patient_age_group,
    strftime('%Y-%m', request_date) as month,
    COUNT(*) as total_requests,
    SUM(CASE WHEN request_status = 'approved' THEN 1 ELSE 0 END) as approved_requests,
    CAST(SUM(CASE WHEN request_status = 'approved' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) as approval_rate
FROM prior_auth
GROUP BY patient_age_group, month
ORDER BY patient_age_group
"""
age_correlation_results = pd.read_sql(age_correlation_query, conn)
print("\nCorrelation between patient age group and request outcomes:")
print(age_correlation_results)

# 7 Identification of drugs with unusually long processing times
long_processing_query = """
WITH processing_times AS (
    SELECT 
        drug_name,
        JULIANDAY(resolution_date) - JULIANDAY(request_date) AS processing_time
    FROM prior_auth
    WHERE request_status != 'pending'
),
ranked_times AS (
    SELECT 
        drug_name,
        processing_time,
        ROW_NUMBER() OVER (PARTITION BY drug_name ORDER BY processing_time) AS row_num,
        COUNT(*) OVER (PARTITION BY drug_name) AS total_count
    FROM processing_times
)
SELECT
    drug_name,
    COUNT(*) AS total_requests,
    AVG(processing_time) AS avg_processing_time,
    AVG(CASE
        WHEN row_num = (total_count + 1) / 2 OR row_num = (total_count + 2) / 2
        THEN processing_time
        ELSE NULL
    END) AS median_processing_time
FROM ranked_times
GROUP BY drug_name
HAVING COUNT(*) > 10
ORDER BY avg_processing_time DESC
LIMIT 10;
"""

long_processing_results = pd.read_sql(long_processing_query, conn)
print("\nDrugs with unusually long processing times:")
print(long_processing_results)

# Concatenate the DataFrames (stack them vertically)
combined_df = pd.concat([request_results, processing_result, approval_result, time_trend_results,
                        geographic_results, age_correlation_results, long_processing_results], axis=0)  # Axis=0 means stacking rows

# Export the combined DataFrame to a CSV file
combined_df.to_csv('combined_output.csv', index=False)

# close connection to sqlite
conn.close()
