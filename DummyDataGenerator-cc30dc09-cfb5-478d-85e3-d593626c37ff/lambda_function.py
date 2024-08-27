import json
import logging
import random
import time

import boto3
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    start_time = time.time()
    kinesis_client = boto3.client('kinesis')
    stream_name = 'generic_kinesis_data'

    try:
        logger.info("Loading CSV data")
        df = pd.read_csv('IRL_City_DataSet.csv')

        logger.info("Processing CSV data")
        city_column = 'city'
        unique_cities = df[city_column].unique()

        if len(unique_cities) < 160:
            logger.error(f"Insufficient unique cities: {len(unique_cities)}")
            return {
                'statusCode': 400,
                'body': f"Error: There are only {len(unique_cities)} unique cities in the CSV file."
            }

        logger.info("Selecting random cities")
        selected_cities = random.sample(list(unique_cities), 160)

        logger.info("Processing selected cities")
        selected_rows = []
        for city in selected_cities:
            city_rows = df[df[city_column] == city]
            random_row = city_rows.sample(n=1)
            selected_rows.append(random_row)

        selected_city_details = pd.concat(selected_rows)
        selected_city_details_list = selected_city_details.to_dict(orient='records')

        # logger.info("Inserting data into Redshift")
        # service.RedshiftService.insert_data_to_redshift(selected_city_details_list)
        logger.info("Inserting data into KINESIS")
        response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(selected_city_details_list),
            PartitionKey='partition_key')
        print("ingestion done")

        end_time = time.time()
        logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")

        return {
            'statusCode': 200,
            'body': str(selected_city_details_list)
        }

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': 'An error occurred'
        }
