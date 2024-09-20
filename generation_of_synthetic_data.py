# generation of synthetic data

# Import necessary libraries

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
from sqlalchemy import create_engine

# Load data from various sources
drugs = pd.read_csv('drug.csv')  # From FDA National Drug Code Directory
# From https://www.cms.gov/medicare/payment/prospective-payment-systems/provider-specific-data-public-use-text-format
providers = pd.read_csv('provider_data.csv')
insurance_plans = pd.read_csv('insurance_plans.csv')  # From Healthcare.gov


# inspect dataframes
print(drugs.head())
print(providers.head())
print(insurance_plans.head())

# fix the column index for insurance plans
insurance_plans.columns = insurance_plans.iloc[0]
insurance_plans_clean = insurance_plans[1:].reset_index(drop=True)
insurance_plans_clean.head()

# Generate synthetic data
num_records = 10000
start_date = datetime(2023, 1, 1)
end_date = datetime(2024, 1, 1)

pa_data = pd.DataFrame({
    'request_id': range(1, num_records + 1),
    'drug_name': np.random.choice(drugs['PRODUCTTYPENAME'], num_records),
    'provider_type': np.random.choice(providers['providerType'], num_records),
    'pharmacy_name': np.random.choice(['CVS', 'Walgreens', 'Rite Aid', 'Walmart Pharmacy'], num_records),
    'insurance_plan': np.random.choice(insurance_plans_clean['Plan Marketing Name'], num_records),
    'request_status': np.random.choice(['approved', 'denied', 'pending'], num_records, p=[0.7, 0.2, 0.1]),
    'request_date': [start_date + timedelta(days=np.random.randint(0, 365)) for _ in range(num_records)],
    'patient_age_group': np.random.choice(['0-17', '18-34', '35-50', '51-65', '65+'], num_records),
    'geographic_region': np.random.choice(['Northeast', 'Midwest', 'South', 'West'], num_records)
})

# combine provider_type for fewer categories
pa_data['provider_type'].value_counts()
pa_data['provider_type'].replace(
    "  ", "00", inplace=True)  # replacing blanks with 00
# both are medicare dependent hospitals and for ease, combining
pa_data['provider_type'].replace("15", "14", inplace=True)
# both are Essetnial Access Community Hospitals and for ease, combining
pa_data['provider_type'].replace("21", "22", inplace=True)

# replace values with dictionary of names for ease of visualization later

provider_type_names = {'00': 'Short term facility',
                       '01': 'Individual Provider',
                       '08': 'Indian Health Service',
                       '16': 'Re-based Sole Community Hospital',
                       '07': 'Rural Referral Center',
                       '14': 'Medicare Depending Hospital',
                       '17': 'Medical Assistance Facility',
                       '22': 'Essetnial Access Community Hospital',
                       '13': 'Cancer Facility',
                       '18': 'Medical Assistance Facility'}

pa_data['provider_type'] = pa_data['provider_type'].replace(
    provider_type_names)

# Calculate resolution date (add 1-10 days to request date)
pa_data['resolution_date'] = pa_data['request_date'] + \
    pd.to_timedelta(np.random.randint(1, 11, num_records), unit='D')

# Ensure pending requests don't have a resolution date
pa_data.loc[pa_data['request_status'] == 'pending', 'resolution_date'] = None

# Save the synthetic dataset
pa_data.to_csv('synthetic_pa_data.csv', index=False)

print("Synthetic Prior Authorization dataset created and saved as 'synthetic_pa_data.csv'")



