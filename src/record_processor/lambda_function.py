"""
Module for Kinesis data stream consumer capabilities. This Lambda
is triggered to process Kinesis records originating from CloudFront
real-time logs and upload the data to a DynamoDB table.
"""
import os
import base64
import logging
import boto3

def dynamodb_upload(partition_key_timestamp, partition_key_distribution, sort_key_status_code,
                    sort_key_edge_location, dynamodb_client):
    """
    Uploads data from Kinesis records to DynamoDB.
    """
    # Retrieve environment variables
    dynamodb_table_name = os.environ['DYNAMODB_TABLE']

    # Configure partition key and sort key for DDB update statement
    partition_key = str(round(partition_key_timestamp)) + '.' + partition_key_distribution
    sort_key = sort_key_edge_location + '.' + sort_key_status_code

    # Calcualte ttl and convert to string
    ttl = str(round(partition_key_timestamp) + 900)

    uploader_response = dynamodb_client.update_item(
        TableName=dynamodb_table_name,
        Key={
            'timestamp.distribution': {
                'S': partition_key
            },
            'edgelocation.statuscode': {
                'S': sort_key
            }
        },
        UpdateExpression="SET status_count = if_not_exists(status_count, :start) + :inc, #ttl = :ttl, status_code = :status_code, edge_location = :edge_location",
        ExpressionAttributeValues={
            ':inc': {
                'N': '1'
            },
            ':start': {
                'N': '0'
            },
            ':ttl': {
                'N': ttl
            },
            ':status_code': {
                'S': sort_key_status_code
            },
            ':edge_location': {
                'S': sort_key_edge_location
            }
        },
        ExpressionAttributeNames={
            '#ttl': 'ttl'
        },
        ReturnValues='ALL_NEW'
    )
    return uploader_response

def lambda_handler(event, context):
    """
    Entrypoint for Lambda invocation. Decodes base64 from Kinesis
    records in Lambda event, and calls dynamodb_upload to push to
    write the data to a DynamoDB table.
    """
    # Set logging levels
    log_level = os.environ['LOGGING_LEVEL']
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Set boto3 client
    dynamodb_client = boto3.client('dynamodb')

    # Retrieve values from Kinesis record(s)
    for record in event['Records']:
        payload = base64.b64decode(record["kinesis"]["data"])

        values = payload.decode("utf-8").strip().split('\t')
        timestamp                   = values[0]
        c_ip                        = values[1]
        sc_status                   = values[2]
        cs_host                     = values[3]
        x_edge_location             = values[4]

        # Log values from Kinesis record
        logger.info('timestamp: %s   c_ip: %s   sc_status: %s   cs_host: %s   x_edge_location: %s',
                     timestamp, c_ip, sc_status, cs_host, x_edge_location)

        # Upload to DynamoDB for aggregation
        try:
            uploader_response = dynamodb_upload(float(timestamp), cs_host, sc_status, x_edge_location, dynamodb_client)
            logger.info(uploader_response)
        except Exception as error:
            logger.error(error)
            raise Exception from error
