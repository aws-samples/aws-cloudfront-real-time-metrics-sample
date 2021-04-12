"""
Module to upload metrics to CloudWatch from DynamoDB table
"""
import os
import datetime
import logging
import json
import re
import boto3

def dynamodb_query(current_timestamp, distribution_domain_name, dynamodb_paginator):
    """
    Query DynamoDB with pagination and return data from response.
    """
    # Retrieve environment variables
    dynamodb_table_name = os.environ['DYNAMODB_TABLE']

    # Construct partition key
    partition_key = str(current_timestamp) + '.' + distribution_domain_name

    # Query table with page iterator
    dynamodb_page_iterator = dynamodb_paginator.paginate(
        TableName=dynamodb_table_name,
        Select='ALL_ATTRIBUTES',
        KeyConditionExpression='#partitionkeyname = :partitionkeyval',
        ExpressionAttributeValues={
            ':partitionkeyval': {
                'S': partition_key
            }
        },
        ExpressionAttributeNames={
            '#partitionkeyname': 'timestamp.distribution'
        }
    )

    # Loop through pages and pull out edge_location, status_code, and status_count
    dynamodb_item_response = []
    for page in dynamodb_page_iterator:
        for item in page['Items']:
            update_dict = {
                'edge_location': item['edge_location']['S'],
                'status_code': item['status_code']['S'],
                'status_count': item['status_count']['N']
            }
            dynamodb_item_response.append(update_dict)

    return dynamodb_item_response

def aggregate_data_by_minute(event_timestamp_obj, distribution_domain_name, dynamodb_paginator, logger):
    """
    Aggregate data from DynamoDB queries into dictionary with data for the previous
    60 seconds.
    """
    # Reformat timestamp data
    current_timestamp = round(int(event_timestamp_obj.timestamp()))
    timestamp_counter = current_timestamp - 59

    # Instantiate put_metric_data dictionary
    put_metric_data_dict = {}

    # Loop through previous 60 seconds of timestamp values
    while timestamp_counter <= current_timestamp:

        # Augment counter
        timestamp_counter += 1

        # Invoke DynamoDB query module to return dictionary of values
        try:
            query_response = dynamodb_query(timestamp_counter, distribution_domain_name, dynamodb_paginator)
        except Exception as error:
            logger.error(error)
            raise Exception from error

        logger.info('Dictionary of values for %s has been processed.', str(timestamp_counter))

        # Build dictionary of values for the minute, aggregated by edge location
        for item in query_response:
            edge_location = item['edge_location']
            status_code = item['status_code']
            status_count = item['status_count']

            if edge_location not in put_metric_data_dict:
                put_metric_data_dict[edge_location] = {}

            if status_code not in put_metric_data_dict[edge_location]:
                put_metric_data_dict[edge_location][status_code] = 0

            # Augment status count
            put_metric_data_dict[edge_location][status_code] = put_metric_data_dict[edge_location][status_code] + int(status_count)

    return put_metric_data_dict

def lambda_handler(event, context):
    """
    Entrypoint for Lambda invocation. Uploads data to CloudWatch
    as custom metrics.
    """
    # Create cloudwatch client, set boto3 client and paginator
    cloudwatch_client = boto3.client('cloudwatch')
    dynamodb_client = boto3.client('dynamodb')
    dynamodb_paginator = dynamodb_client.get_paginator('query')

    # Set logging levels
    log_level = os.environ['LOGGING_LEVEL']
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Retrieve data from event
    params = json.loads(event)

    # Retrieve current UNIX time
    event_timestamp_obj = datetime.datetime.strptime(params['timestamp'], '%Y-%m-%dT%H:%M:%SZ')

    # Log Distribution Domain Name
    logger.info('This distribution domain name is %s', params['timestamp'])

    # Get aggregated response dictionary
    aggregated_dict = aggregate_data_by_minute(event_timestamp_obj, params['distribution'], dynamodb_paginator, logger)
    logger.info('Aggregated dictionary created.')
    logger.info(aggregated_dict)

    # Loop through dictionary of values for CloudFront Edge locations and status codes
    # and upload to CloudWatch
    for x_edge_location, status_codes in aggregated_dict.items():

        # put_metric_data total amount of requests per edge location
        status_sum = sum(status_codes.values())

        cloudwatch_client.put_metric_data(
            Namespace='CloudFront by Edge Location - Count',
            MetricData=[
                {
                    'MetricName': 'RequestCount',
                    'Timestamp': event_timestamp_obj,
                    'Dimensions': [
                        {
                            'Name': 'cs-host',
                            'Value': params['distribution']
                        },
                        {
                            'Name': 'x-edge-location',
                            'Value': x_edge_location
                        }
                    ],
                    'Value': status_sum,
                    'Unit': 'Count',
                    'StorageResolution': 60
                },
            ]
        )

        # sum requests per HTTP status code category (2xx, 4xx, 5xx)
        status_groups = {}
        for group in ['2', '4', '5']:
            vals = [int(v) for (k, v) in status_codes.items() if group in k]
            status_groups[f'{group}xx'] = sum(vals)

        # put_metric_data for absolute count of HTTP status codes by CloudFront edge location
        for sc_status_group, count in status_groups.items():
            cloudwatch_client.put_metric_data(
                Namespace= 'CloudFront by Edge Location - Count',
                MetricData=[
                    {
                        'MetricName': f'{sc_status_group}',
                        'Timestamp': event_timestamp_obj,
                        'Dimensions': [
                            {
                                'Name': 'cs-host',
                                'Value': params['distribution']
                            },
                            {
                                'Name': 'x-edge-location',
                                'Value': x_edge_location
                            }
                        ],
                        'Value': count,
                        'Unit': 'Count',
                        'StorageResolution': 60
                    },
                ]
            )

            # put_metric_data for percentage of HTTP status codes by CloudFront edge location
            cloudwatch_client.put_metric_data(
                Namespace= 'CloudFront by Edge Location - Percent',
                MetricData=[
                    {
                        'MetricName': f'{sc_status_group}',
                        'Timestamp': event_timestamp_obj,
                        'Dimensions': [
                            {
                                'Name': 'cs-host',
                                'Value': params['distribution']
                            },
                            {
                                'Name': 'x-edge-location',
                                'Value': x_edge_location
                            }
                        ],
                        'Value': round(count * 100.0 / status_sum, 2),
                        'Unit': 'Percent',
                        'StorageResolution': 60
                    },
                ]
            )
