import csv
import io
import json
import os
from datetime import datetime, timedelta

import boto3
import redshift_connector

def lambda_handler(event, context):
    # Redshift connection details
    host = os.environ['REDSHIFT_HOST']
    port = os.environ['REDSHIFT_PORT']
    dbname = os.environ['REDSHIFT_DB_NAME']
    user = os.environ['REDSHIFT_DB_USER']
    password = os.environ['REDSHIFT_DB_PASSWORD']
    delta_days = os.environ['DELTA_DAYS']

    print(f"Redshift connection details: host={host}, port={port}, dbname={dbname}")

    # Calculate the date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=int(delta_days))
    start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
    end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')

    print(f"Date range for extraction: start_date={start_date_str}, end_date={end_date_str}")

    # S3 details
    s3_bucket = os.environ['S3_LEGACY_RECORD_BUCKET']
    today = datetime.now()
    year = today.strftime('%Y')
    month = today.strftime('%m')
    day = today.strftime('%d')
    s3_key = f'{year}/{month}/{day}/delta-file.csv'

    print(f"S3 bucket: {s3_bucket}, S3 key: {s3_key}")

    # Connect to Redshift
    print("Connecting to Redshift...")
    conn = redshift_connector.connect(
        host=host,
        port=int(port),
        database=dbname,
        user=user,
        password=password
    )
    cursor = conn.cursor()

    # Select records from the last delta_days
    select_sql = """
        SELECT * FROM public.irl_data
        WHERE created BETWEEN %s AND %s;
    """
    print(f"Executing query: {select_sql} with parameters: ({start_date_str}, {end_date_str})")
    cursor.execute(select_sql, (start_date_str, end_date_str))
    rows = cursor.fetchall()

    print(f"Number of rows retrieved: {len(rows)}")

    # Create CSV in memory
    print("Creating CSV in memory...")
    output = io.StringIO()
    csv_writer = csv.writer(output)
    csv_writer.writerow([desc[0] for desc in cursor.description])  # Write headers
    csv_writer.writerows(rows)  # Write data

    # Upload CSV to S3
    print(f"Uploading CSV to S3 bucket {s3_bucket} with key {s3_key}...")
    s3_client = boto3.client('s3')
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=output.getvalue(),
        ContentType='text/csv'
    )

    print("CSV successfully uploaded to S3.")

    # Delete records from Redshift
    delete_sql = """
        DELETE FROM public.irl_data
        WHERE created BETWEEN %s AND %s;
    """
    print(f"Executing delete query: {delete_sql} with parameters: ({start_date_str}, {end_date_str})")
    cursor.execute(delete_sql, (start_date_str, end_date_str))
    conn.commit()

    print("Records successfully deleted from Redshift.")

    # Clean up
    cursor.close()
    conn.close()
    print("Redshift connection closed.")

    return {
        'statusCode': 200,
        'body': json.dumps('CSV created and uploaded to S3, and records deleted from Redshift.')
    }
