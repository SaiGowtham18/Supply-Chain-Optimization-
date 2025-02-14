# Importing necessary libraries
import pandas as pd # type: ignore
import snowflake.connector # type: ignore
import os
from snowflake_utils import snowflake_details

# Define file paths dynamically for portability
RAW_DATA_PATH = "data/raw/supply_chain_data.xlsx"
PROCESSED_DATA_PATH = "data/processed/processed_data.csv"

# Ensure the processed data directory exists before saving
os.makedirs("data/processed", exist_ok=True)

# Load supply chain dataset
supply_chain_df = pd.read_excel(RAW_DATA_PATH)

# Data Transformation: Clean missing values and filter out unknown customer demographics
transformed_data = supply_chain_df.dropna()
transformed_data = transformed_data[transformed_data["Customer demographics"] != "Unknown"]

# Save the transformed data to CSV
transformed_data.to_csv(PROCESSED_DATA_PATH, index=False)

# Load the data into Snowflake
target_table = "SUPPLY_CHAIN_ANALYTICS"

# Establish connection with Snowflake
conn = snowflake.connector.connect(
    user=snowflake_details['user'],
    password=snowflake_details['password'],
    account=snowflake_details['account'],
    database=snowflake_details['database'],
    schema=snowflake_details['schema']
)

cur = conn.cursor()

# Create the target table in Snowflake
create_table_query = f'''
CREATE OR REPLACE TABLE {target_table} (
    Product STRING,
    SKU STRING,
    Price FLOAT,
    Availability INT,
    Sales INT,
    Revenue FLOAT,
    Customer_Demographics STRING,
    Stock_Level INT,
    Lead_Time INT,
    Order_Quantity INT,
    Shipping_Duration INT,
    Shipping_Cost FLOAT,
    Supplier STRING,
    Warehouse_Location STRING,
    Production_Volume INT,
    Manufacturing_Cost FLOAT,
    Defect_Rate FLOAT,
    Transport_Mode STRING,
    Distribution_Route STRING,
    Total_Logistics_Cost FLOAT
);
'''
cur.execute(create_table_query)

# Stage and Load data into Snowflake
staging_area = snowflake_details['staging_area']
staged_file = f"@{staging_area}/processed_data.csv"

stage_file_query = f"PUT file://{PROCESSED_DATA_PATH} {staged_file}"
cur.execute(stage_file_query)

copy_into_query = f"""
COPY INTO {target_table}
FROM {staged_file}
FILE_FORMAT = (TYPE = CSV, FIELD_OPTIONALLY_ENCLOSED_BY='"');
"""
cur.execute(copy_into_query)

# Commit transaction and close connection
conn.commit()
cur.close()
conn.close()

print("Data successfully processed and loaded into Snowflake!")
