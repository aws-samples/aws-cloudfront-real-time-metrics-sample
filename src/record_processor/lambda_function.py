"""
Module for Kinesis data stream consumer capabilities. This Lambda
is triggered to process Kinesis records originating from CloudFront
real-time logs and upload the data to a DynamoDB table.
"""
import os
import re
import base64
import logging
import boto3

def proccess_records(records):
    """
    Decodes base64 from Kinesis records in Lambda event
    and returns list
    """
    # Instantiate records_list
    records_list = []

    # Retrieve values from Kinesis record(s)
    for record in records:
        payload = base64.b64decode(record["kinesis"]["data"])

        values = payload.decode("utf-8").strip().split('\t')
        timestamp                   = values[0]
        c_ip                        = values[1]
        sc_status                   = values[2]
        cs_host                     = values[3]
        x_edge_location             = values[4]

    # Append dictionary of records to record list
        record_dict = {
                'timestamp': timestamp,
                'c_ip': c_ip,
                'sc_status': sc_status,
                'cs_host': cs_host,
                'x_edge_location': x_edge_location
            }

        records_list.append(record_dict)

    return records_list

def aggregate_data_by_minute(records_list):
    """
    Aggregate data from Kinesis records into dictionary for upload at 60-second resolution
    """
    # Instantiate put_metric_data dictionary
    put_metric_data_dict = {}

    # Loop through records and and aggregate based on minute and by edge location
    for record in records_list:
        # round timestamp down to nearest whole minute, retrieve relevent dimensions
        timestamp = int(float(record['timestamp'])//60 * 60)
        cf_distro = record['cs_host']
        x_edge_location = record['x_edge_location']
        status_code = record['sc_status']

        if cf_distro not in put_metric_data_dict:
            put_metric_data_dict[cf_distro] = {}

        if timestamp not in put_metric_data_dict[cf_distro]:
            put_metric_data_dict[cf_distro][timestamp] = {}

        if x_edge_location not in put_metric_data_dict[cf_distro][timestamp]:
            put_metric_data_dict[cf_distro][timestamp][x_edge_location] = {}

        if status_code not in put_metric_data_dict[cf_distro][timestamp][x_edge_location]:
            put_metric_data_dict[cf_distro][timestamp][x_edge_location][status_code] = 0

        # Augment status count
        put_metric_data_dict[cf_distro][timestamp][x_edge_location][status_code] = \
        put_metric_data_dict[cf_distro][timestamp][x_edge_location][status_code] + 1

    return put_metric_data_dict

def upload_to_cloudwatch(aggregated_dict):
    """
    Upload metrics to Cloudwatch
    """
    # Instantiate CloudWatch client
    cloudwatch_client = boto3.client('cloudwatch')

    # Loop through dictionary of values for CloudFront distributions, timestamps,
    # Edge locations, and status codes and upload to CloudWatch
    for cf_distro, timestamp in aggregated_dict.items():
        for timestamp, x_edge_location in timestamp.items():
            for x_edge_location, status_codes in x_edge_location.items():

                # put_metric_data total amount of requests per edge location
                status_sum = sum(status_codes.values())

                cloudwatch_client.put_metric_data(
                    Namespace='CloudFront by Edge Location - Count',
                    MetricData=[
                        {
                            'MetricName': 'RequestCount',
                            'Timestamp': timestamp,
                            'Dimensions': [
                                {
                                    'Name': 'cs-host',
                                    'Value': cf_distro
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
                    vals = [int(v) for (k, v) in status_codes.items() if re.compile(f'^{group}').match(k)]
                    status_groups[f'{group}xx'] = sum(vals)

                # put_metric_data for absolute count of HTTP status codes by CloudFront edge
                for sc_status_group, count in status_groups.items():
                    cloudwatch_client.put_metric_data(
                        Namespace= 'CloudFront by Edge Location - Count',
                        MetricData=[
                            {
                                'MetricName': f'{sc_status_group}',
                                'Timestamp': timestamp,
                                'Dimensions': [
                                    {
                                        'Name': 'cs-host',
                                        'Value': cf_distro
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

                    # put_metric_data for percentage of HTTP status codes by CloudFront edge
                    cloudwatch_client.put_metric_data(
                        Namespace= 'CloudFront by Edge Location - Percent',
                        MetricData=[
                            {
                                'MetricName': f'{sc_status_group}',
                                'Timestamp': timestamp,
                                'Dimensions': [
                                    {
                                        'Name': 'cs-host',
                                        'Value': cf_distro
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

def lambda_handler(event, context):
    """
    Entrypoint for Lambda invocation. Decodes base64 from Kinesis
    records in Lambda event, aggregates data, and uploads data to CloudWatch.
    """
    # Set logging levels
    log_level = os.environ['LOGGING_LEVEL']
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Process Kinesis records values and return list of record dictionaries
    try:
        records_list = proccess_records(event['Records'])
        logger.info(records_list)
    except Exception as error:
        logger.error(error)
        raise Exception from error

    # Aggregate data by minute, CloudFront edge location, and status code
    try:
        aggregated_dict = aggregate_data_by_minute(records_list)
        logger.info(aggregated_dict)
    except Exception as error:
        logger.error(error)
        raise Exception from error

    # Upload metrics to CloudWatch
    upload_to_cloudwatch(aggregated_dict)
