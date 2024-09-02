# Rick and Morty Data Engineering Project

## Overview

This project demonstrates the end-to-end process of extracting data from the Rick and Morty open-source API, transforming it, and loading the transformed data into an Amazon RDS MySQL database. The project leverages AWS services such as Lambda for automation and S3 for storage. The data is then connected to HeidiSQL for easy querying and analysis.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Environment Setup](#environment-setup)
3. [Data Extraction](#data-extraction)
4. [Data Transformation](#data-transformation)
5. [Data Loading](#data-loading)
6. [Connecting to RDS using HeidiSQL](#connecting-to-rds-using-heidisql)
7. [Error Handling and Troubleshooting](#error-handling-and-troubleshooting)
8. [Conclusion](#conclusion)

---

## Prerequisites

Before starting, ensure you have the following:

1. **AWS Account**: Access to an AWS account with permissions to create and manage S3, RDS, and Lambda.
2. **Python**: Python 3.8 or higher installed.
3. **AWS CLI**: Installed and configured with your credentials.
4. **HeidiSQL**: Installed on your local machine.

---

## Environment Setup

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/yourusername/data-engineering.git
   cd data-engineering
   ```

2. **Create a Virtual Environment:**
   ```bash
   python3 -m venv Mustard
   source Mustard/bin/activate
   ```

3. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **AWS Configuration:**
   Run `aws configure` to set up your AWS credentials.
   ```bash
   aws configure
   ```
   Provide your `AWS Access Key`, `Secret Key`, `Region`, and `Output format`.

---

## Data Extraction

### Script: `extraction.py`

The data is extracted from the Rick and Morty API's three main endpoints: Characters, Locations, and Episodes. The data is saved as CSV files in an S3 bucket.

**Steps:**
1. **Extract Data**: 
   - The script iterates over the API pages to collect data from each endpoint.
   - Data is saved into three separate DataFrames: `characters.csv`, `locations.csv`, and `episodes.csv`.

2. **Save Data to S3**:
   - The data is saved to the `Rick&Morty/Untransformed/` folder in your S3 bucket.

---

## Data Transformation

### Script: `transformation.py`

The transformation involves cleaning and restructuring the data to prepare it for loading into the RDS database.

**Steps:**
1. **Load Data from S3**:
   - The script reads the raw CSV files from the S3 bucket.

2. **Transform Data**:
   - Extracts necessary fields from complex data types (e.g., nested JSON structures).
   - Creates additional DataFrames such as `appearances.csv` to normalize the data.

3. **Save Transformed Data**:
   - The transformed data is saved back to the S3 bucket in the `Rick&Morty/Transformed/` folder.

---

## Data Loading

### Script: `loading.py`

This script loads the transformed data into the RDS MySQL database.

**Steps:**
1. **Connect to RDS**:
   - Uses the `mysql-connector-python` library to connect to the RDS instance.

2. **Create Tables**:
   - Executes SQL scripts to create tables (`Character_Table`, `Episode_Table`, `Appearance_Table`, `Location_Table`) in the database.

3. **Insert Data**:
   - Iterates over the DataFrames and inserts the data into the corresponding tables.

---

## Connecting to RDS using HeidiSQL

1. **Open HeidiSQL**.

2. **Create a New Session**:
   - Select `MySQL` as the connection type.
   - Fill in the connection details:
     - Hostname/IP: Your RDS endpoint.
     - User: `admin` (or your RDS username).
     - Password: Your RDS password.
     - Database: `rick-and-morty-db`.

3. **Connect**:
   - Once connected, you can view the tables and execute queries against your Rick and Morty data.

---

## Error Handling and Troubleshooting

- **NoCredentialsError**: Ensure your AWS credentials are correctly configured using `aws configure`.
- **AccessDeniedError**: Attach the necessary IAM policies to your user or role to allow S3 and RDS operations.
- **KeyError**: Ensure that the data fields being referenced exist in your DataFrames after transformation.

---

## Conclusion

This project showcases a complete ETL pipeline using AWS services and Python. The data extracted from the Rick and Morty API is transformed and stored in a MySQL database hosted on AWS RDS. Finally, the data is accessible and queryable through HeidiSQL, making it a robust solution for data analysis.
