import time
import pandas as pd
import random
from minio import Minio
from io import StringIO, BytesIO
import pyarrow.parquet as pq
import pyarrow as pa
import json
import logging
import threading

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Initialize MinIO client
minio_client = Minio('minio:9000', access_key='admin', secret_key='password', secure=False)

# Setup Bucket name and object names
finance_bucket = 'finance-bucket'
healthcare_bucket = 'healthcare-bucket'
ml_bucket = 'ml-bucket'
finance_object_name = 'finance_data.csv'
healthcare_object_name = 'healthcare_data.parquet'
ml_object_name = 'ml_data.json'

# Set public read policy for the given bucket
def set_public_read_policy(bucket_name):
    try:
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicReadGetObject",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket_name}/*"
                }
            ]
        }
        # Convert the policy to JSON
        policy_json = json.dumps(policy)

        # Apply the policy to the bucket
        minio_client.set_bucket_policy(bucket_name, policy_json)
        logger.info(f"Set public read policy for bucket: {bucket_name}")
    except Exception as e:
        logger.error(f"Error setting public read policy for bucket {bucket_name}: {e}")

# Create buckets if they do not exist and set public read policy
try:
    if not minio_client.bucket_exists(finance_bucket):
        minio_client.make_bucket(finance_bucket)
        logger.info(f"Created bucket: {finance_bucket}")
        set_public_read_policy(finance_bucket)

    if not minio_client.bucket_exists(healthcare_bucket):
        minio_client.make_bucket(healthcare_bucket)
        logger.info(f"Created bucket: {healthcare_bucket}")
        set_public_read_policy(healthcare_bucket)

    if not minio_client.bucket_exists(ml_bucket):
        minio_client.make_bucket(ml_bucket)
        logger.info(f"Created bucket: {ml_bucket}")
        set_public_read_policy(ml_bucket)
except Exception as e:
    logger.error(f"Error while creating buckets or setting policies: {e}")

# Generate finance CSV data with more columns
def generate_finance_data():
    try:
        timestamp = time.time()
        symbol = random.choice(['AAPL', 'GOOGL', 'AMZN', 'MSFT'])
        open_price = round(random.uniform(100, 2000), 2)
        high_price = round(open_price * random.uniform(1.01, 1.05), 2)
        low_price = round(open_price * random.uniform(0.95, 0.99), 2)
        close_price = round(random.uniform(low_price, high_price), 2)
        volume = random.randint(1000000, 10000000)
        market_cap = round(volume * close_price, 2)
        pe_ratio = round(random.uniform(10, 50), 2)
        dividend_yield = round(random.uniform(0, 0.05), 4)

        logger.info("Generated finance data.")
        
        return pd.DataFrame({
            'timestamp': [timestamp],
            'symbol': [symbol],
            'open_price': [open_price],
            'high_price': [high_price],
            'low_price': [low_price],
            'close_price': [close_price],
            'volume': [volume],
            'market_cap': [market_cap],
            'pe_ratio': [pe_ratio],
            'dividend_yield': [dividend_yield]
        })
    except Exception as e:
        logger.error(f"Error generating finance data: {e}")

# Generate healthcare Parquet data with more columns
def generate_healthcare_data():
    try:
        timestamp = time.time()
        patient_id = random.randint(1, 1000)
        age = random.randint(18, 80)
        heart_rate = random.randint(60, 100)
        blood_pressure = f"{random.randint(110, 130)}/{random.randint(70, 85)}"
        weight = round(random.uniform(50, 100), 2)
        cholesterol_level = random.choice(['Normal', 'High', 'Very High'])
        medication = random.choice(['Aspirin', 'Ibuprofen', 'None'])
        bmi = round(weight / (random.uniform(1.5, 2.0) ** 2), 2)  # BMI calculation
        glucose_level = random.randint(70, 180)

        logger.info("Generated healthcare data.")
        
        return pd.DataFrame({
            'timestamp': [timestamp],
            'patient_id': [patient_id],
            'age': [age],
            'heart_rate': [heart_rate],
            'blood_pressure': [blood_pressure],
            'weight': [weight],
            'cholesterol_level': [cholesterol_level],
            'medication': [medication],
            'bmi': [bmi],
            'glucose_level': [glucose_level]
        })
    except Exception as e:
        logger.error(f"Error generating healthcare data: {e}")

# Generate Machine Learning JSON data with more columns
def generate_ml_data():
    try:
        patient_id = random.randint(1, 1000)
        age = random.randint(18, 80)
        blood_sugar = round(random.uniform(70, 180), 2)
        bmi = round(random.uniform(18.5, 40.0), 2)
        heart_rate = random.randint(60, 100)
        cholesterol_level = random.choice(['Normal', 'High', 'Very High'])
        blood_pressure = f"{random.randint(110, 130)}/{random.randint(70, 85)}"
        medication = random.choice(['Aspirin', 'Ibuprofen', 'None'])
        class_label = random.choice([0, 1])  # Binary classification: 0 (healthy), 1 (ill)

        ml_data = {
            'patient_id': patient_id,
            'age': age,
            'blood_sugar': blood_sugar,
            'bmi': bmi,
            'heart_rate': heart_rate,
            'cholesterol_level': cholesterol_level,
            'blood_pressure': blood_pressure,
            'medication': medication,
            'class_label': class_label
        }

        logger.info("Generated machine learning data.")
        return ml_data
    except Exception as e:
        logger.error(f"Error generating ML data: {e}")

# Upload CSV data to MinIO (Finance Data)
def upload_csv_to_minio(df, bucket_name, object_name):
    try:
        # Try to get the existing object
        try:
            existing_data = minio_client.get_object(bucket_name, object_name)
            existing_data_str = existing_data.read().decode('utf-8')
            csv_data = df.to_csv(index=False)
            combined_data = existing_data_str + csv_data
        except Exception as e:
            # If the object does not exist, create the CSV file
            logger.info(f"{object_name} does not exist, creating a new one.")
            combined_data = df.to_csv(index=False)

        byte_data = combined_data.encode('utf-8')
        minio_client.put_object(bucket_name, object_name, BytesIO(byte_data), len(byte_data))
        logger.info(f"Appended finance data to {object_name} in bucket {bucket_name}.")
    except Exception as e:
        logger.error(f"Error uploading CSV to MinIO: {e}")

# Upload Parquet data to MinIO (Healthcare Data)
def upload_parquet_to_minio(df, bucket_name, object_name):
    try:
        # Check if the file exists
        try:
            existing_data = minio_client.get_object(bucket_name, object_name)
            existing_table = pq.read_table(BytesIO(existing_data.read()))
            combined_table = pa.concat_tables([existing_table, pa.Table.from_pandas(df)])
        except Exception as e:
            # If no file exists, create a new table
            combined_table = pa.Table.from_pandas(df)
        
        # Write to an in-memory buffer and upload
        pq_buffer = BytesIO()
        pq.write_table(combined_table, pq_buffer)
        pq_buffer.seek(0)
        minio_client.put_object(bucket_name, object_name, pq_buffer, len(pq_buffer.getvalue()))
        logger.info(f"Appended healthcare data to {object_name} in bucket {bucket_name}.")
    except Exception as e:
        logger.error(f"Error uploading Parquet to MinIO: {e}")

# Upload JSON data to MinIO (Machine Learning Data)
def upload_json_to_minio(json_data, bucket_name, object_name):
    try:
        # Check if the file exists
        try:
            existing_data = minio_client.get_object(bucket_name, object_name)
            existing_json = json.loads(existing_data.read().decode('utf-8'))
            existing_json.append(json_data)
        except Exception as e:
            # If no file exists, create a new list
            existing_json = [json_data]

        json_str = json.dumps(existing_json)
        byte_data = json_str.encode('utf-8')
        minio_client.put_object(bucket_name, object_name, BytesIO(byte_data), len(byte_data))
        logger.info(f"Appended ML data to {object_name} in bucket {bucket_name}.")
    except Exception as e:
        logger.error(f"Error uploading JSON to MinIO: {e}")

# Function to handle all uploads in parallel
def stream_data():
    try:
        # Simulate finance data and upload as CSV
        finance_df = generate_finance_data()
        upload_csv_to_minio(finance_df, finance_bucket, finance_object_name)

        # Simulate healthcare data and upload as Parquet
        healthcare_df = generate_healthcare_data()
        upload_parquet_to_minio(healthcare_df, healthcare_bucket, healthcare_object_name)

        # Simulate machine learning data and upload as JSON
        ml_data = generate_ml_data()
        upload_json_to_minio(ml_data, ml_bucket, ml_object_name)

        logger.info("Uploaded finance data, healthcare data, and ML data to MinIO.")

    except Exception as e:
        logger.error(f"Error during data streaming and upload: {e}")

# Start continuous data upload every 30 seconds using threading for parallel execution
while True:
    try:
        # Run the stream_data function in a separate thread to avoid blocking
        threading.Thread(target=stream_data).start()

        # Sleep for 4 seconds
        time.sleep(4)
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
        time.sleep(4)
