import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import psycopg2
from minio import Minio
from io import BytesIO
import json
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Initialize MinIO client
minio_client = Minio('minio:9000', access_key='admin', secret_key='password', secure=False)

# PostgreSQL connection details
PG_HOST = "postgres"
PG_DATABASE = "warehouse"
PG_USER = "admin"
PG_PASSWORD = "password"

# Bucket names
finance_bucket = 'finance-bucket'
healthcare_bucket = 'healthcare-bucket'
ml_bucket = 'ml-bucket'

# Function to connect to PostgreSQL
def connect_postgres():
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        logger.info("Connected to PostgreSQL database.")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        return None

# Function to read CSV file from MinIO
def read_csv_from_minio(bucket_name, object_name):
    try:
        data = minio_client.get_object(bucket_name, object_name)
        df = pd.read_csv(BytesIO(data.read()))
        logger.info(f"Successfully read CSV data from {object_name}")
        return df
    except Exception as e:
        logger.error(f"Error reading CSV from MinIO: {e}")
        return pd.DataFrame()

# Function to read Parquet file from MinIO
def read_parquet_from_minio(bucket_name, object_name):
    try:
        data = minio_client.get_object(bucket_name, object_name)
        table = pq.read_table(BytesIO(data.read()))
        df = table.to_pandas()
        logger.info(f"Successfully read Parquet data from {object_name}")
        return df
    except Exception as e:
        logger.error(f"Error reading Parquet from MinIO: {e}")
        return pd.DataFrame()

# Function to read JSON data from MinIO
def read_json_from_minio(bucket_name, object_name):
    try:
        data = minio_client.get_object(bucket_name, object_name)
        json_data = json.loads(data.read().decode('utf-8'))
        df = pd.json_normalize(json_data)
        logger.info(f"Successfully read JSON data from {object_name}")
        return df
    except Exception as e:
        logger.error(f"Error reading JSON from MinIO: {e}")
        return pd.DataFrame()

# Function to load data into PostgreSQL with upsert functionality
def load_to_postgres(df, table_name, conflict_column):
    conn = connect_postgres()
    if conn is not None:
        try:
            cursor = conn.cursor()
            df_columns = ", ".join(df.columns)
            values_placeholder = ", ".join(["%s"] * len(df.columns))
            insert_query = f"""
                INSERT INTO {table_name} ({df_columns})
                VALUES ({values_placeholder})
                ON CONFLICT ({conflict_column})
                DO UPDATE SET 
                {', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col != conflict_column])}
            """
            
            for row in df.itertuples(index=False, name=None):
                cursor.execute(insert_query, row)

            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"Successfully loaded data into {table_name} table in PostgreSQL.")
        except Exception as e:
            logger.error(f"Error loading data into PostgreSQL: {e}")

# Transformations
def transform_finance_data(finance_df):
    try:
        # Convert columns to numeric, forcing errors to NaN
        finance_df['close_price'] = pd.to_numeric(finance_df['close_price'], errors='coerce')
        finance_df['market_cap'] = pd.to_numeric(finance_df['market_cap'], errors='coerce')
        finance_df['pe_ratio'] = pd.to_numeric(finance_df['pe_ratio'], errors='coerce')
        finance_df['dividend_yield'] = pd.to_numeric(finance_df['dividend_yield'], errors='coerce')

        # Drop rows with NaN values (or you can replace them with a default value)
        finance_df = finance_df.dropna(subset=['close_price', 'market_cap', 'pe_ratio', 'dividend_yield'])

        # Now perform the aggregation
        aggregated_df = finance_df.groupby('symbol').agg(
            avg_close_price=('close_price', 'mean'),
            total_market_cap=('market_cap', 'sum'),
            avg_pe_ratio=('pe_ratio', 'mean'),
            avg_dividend_yield=('dividend_yield', 'mean')
        ).reset_index()

        logger.info("Transformed finance data successfully.")
        return aggregated_df
    except Exception as e:
        logger.error(f"Error transforming finance data: {e}")
        return pd.DataFrame()

def transform_healthcare_data(healthcare_df):
    try:
        aggregated_df = healthcare_df.groupby('cholesterol_level').agg(
            avg_age=('age', 'mean'),
            avg_bmi=('bmi', 'mean'),
            count_patients=('patient_id', 'count')
        ).reset_index()
        logger.info("Transformed healthcare data successfully.")
        return aggregated_df
    except Exception as e:
        logger.error(f"Error transforming healthcare data: {e}")
        return pd.DataFrame()

def transform_ml_data(ml_df):
    try:
        aggregated_df = ml_df.groupby('cholesterol_level').agg(
            avg_blood_sugar=('blood_sugar', 'mean'),
            count_patients=('patient_id', 'count')
        ).reset_index()
        logger.info("Transformed ML data successfully.")
        return aggregated_df
    except Exception as e:
        logger.error(f"Error transforming ML data: {e}")
        return pd.DataFrame()

# ETL Job Function
def run_etl():
    try:
        # Extract data
        finance_df = read_csv_from_minio(finance_bucket, 'finance_data.csv')
        healthcare_df = read_parquet_from_minio(healthcare_bucket, 'healthcare_data.parquet')
        ml_df = read_json_from_minio(ml_bucket, 'ml_data.json')

        # Transform data
        transformed_finance_df = transform_finance_data(finance_df)
        transformed_healthcare_df = transform_healthcare_data(healthcare_df)
        transformed_ml_df = transform_ml_data(ml_df)

        # Load transformed data into PostgreSQL using upsert
        load_to_postgres(transformed_finance_df, 'finance_data', 'symbol')
        load_to_postgres(transformed_healthcare_df, 'healthcare_data', 'cholesterol_level')
        load_to_postgres(transformed_ml_df, 'ml_data', 'cholesterol_level')

        logger.info("ETL job completed successfully.")
    except Exception as e:
        logger.error(f"Error in ETL job: {e}")

# Run the ETL job continuously every 30 seconds
while True:
    run_etl()
    logger.info("Waiting for 30 seconds before next run...")
    time.sleep(30)
