## Project Overview:
This project aims to create a comprehensive analytics dashboard focused on prior authorization (PA) requests in the healthcare industry. It demonstrates the ability to work with real-world healthcare data, perform complex SQL queries, create interactive visualizations, and derive meaningful insights from large datasets.

# Data Sources
This project utilizes the following publicly available datasets:

- CMS Provider Specific Data (Public Use)
  -  Used for: Provider-specific information and service utilization data

- FDA National Drug Code Directory
  - Used for: Comprehensive drug information

- Healthcare.gov Health Plan Information (2024)
  - Used for: Insurance plan data



# Features

- Data processing and integration from multiple healthcare datasets
  - found in generation_of_synthetic_data.py
  - synthetic dataset can be found in synthetic_pa_data.csv
- SQL analysis of healthcare trends and patterns
  - found in run_sql_queries.py 
- Interactive Tableau dashboard for visualizing healthcare metrics
  - found on [Tableau Public] (https://public.tableau.com/app/profile/sydney.casey/viz/EvaluationofDrugAuthorization/Story1#1)

# Technical Stack

- Data Processing: Python (pandas)
- Database: SQLite (via SQLAlchemy)
- Data Analysis: SQL, Python
- Visualization: Tableau
- Version Control: Git


# License
This project is licensed under the MIT License.

# Acknowledgments

Centers for Medicare & Medicaid Services (CMS) for providing the Provider Specific Data
U.S. Food and Drug Administration (FDA) for the National Drug Code Directory
Healthcare.gov for the Health Plan Information
