import os
from dotenv import load_dotenv # type: ignore

# Load environment variables from .env file
load_dotenv()

# Securely store Snowflake credentials
snowflake_details = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA'),
    'staging_area': os.getenv('SNOWFLAKE_STAGE')
}
