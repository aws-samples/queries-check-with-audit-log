import json
import os
import boto3
import logging
from datetime import datetime
from enum import Enum
import traceback

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class Task(Enum):
    CREATED = 'Created'
    STOPPED = 'Stopped'
    COMPLETED = 'Completed'
    IN_PROGRESS = 'In-progress'
    FAILED = 'Failed'

# Initiate ddb and sqs client
dynamodb = boto3.resource('dynamodb')
sqs = boto3.client('sqs')

task_table = dynamodb.Table(os.environ['CHECK_TASK_TABLE_NAME'])
subtask_table = dynamodb.Table(os.environ['CHECK_SUBTASK_TABLE_NAME'])
sqs_queue_url = os.environ['SQS_QUEUE_URL']
s3_client = boto3.client('s3')


def update_task_status(task_id, new_status, error_message = ""):
    try:
        # Update the item with the specified task_id
        response = task_table.update_item(
            Key={
                'task_id': task_id  # Specify the primary key (partition key)
            },
            UpdateExpression="SET #status = :new_status, #ut = :ut, #ms = :ms",  # Update expression to set new status
            ExpressionAttributeNames={
                '#status': 'status',  # Use a placeholder for the attribute name
                '#ut': 'update_time',
                '#ms': 'error_message'
            },
            ExpressionAttributeValues={
                ':new_status': new_status,  # New value for the status attribute
                ':ut': datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                ':ms': error_message
            },
            ReturnValues="UPDATED_NEW"  # Return the updated attributes
        )

        logger.info("Update succeeded:")
        logger.info(response)
        return response

    except Exception as e:
        logger.error("Error updating item:")
        traceback.print_exc()
        return None


def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event))
    task_id = event['task_id']
    s3_bucket = event['s3_bucket']
    cluster_identifier = event['cluster_identifier']
    validate_cluster_endpoint = event['validate_cluster_endpoint']
    check_percent = event['check_percent']

    prefix = 'audit-log/'+ task_id + '_' + cluster_identifier + '/'

    # Use paginator to handle large number of objects
    paginator = s3_client.get_paginator('list_objects_v2')

    exist_log_file_flag = False
    # Iterate through pages of objects in the specified bucket and prefix
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
        # Check if 'Contents' is in the page response
        if 'Contents' in page:
            for obj in page['Contents']:
                # Get the object key
                key = obj['Key']

                # Check if the object key ends with .gz
                if key.endswith('.gz'):
                    exist_log_file_flag = True
                    s3_object_key = key
                    # Put item to subtask table.
                    subtask_item = {
                        'task_id': task_id,
                        's3_object_key': s3_object_key,
                        'total_count': 0,
                        'error_count': 0,
                        'warning_count': 0,
                        'create_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                        'status': 'Created',
                        'update_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                        'completed_time': '',
                    }

                    subtask_table.put_item(Item=subtask_item)

                    logger.info(f"Subtask item written to check_subtask table: {json.dumps(subtask_item)}")

                    # Make SQS message
                    sqs_message = {
                        'task_id': task_id,
                        'cluster_identifier': cluster_identifier,
                        'validate_cluster_endpoint': validate_cluster_endpoint,
                        'check_percent': int(check_percent),
                        'rerun': event['rerun'],
                        's3_bucket': s3_bucket,
                        's3_object_key': s3_object_key
                    }

                    # Send message to queue.
                    sqs.send_message(
                        QueueUrl=sqs_queue_url,
                        MessageBody=json.dumps(sqs_message)
                    )

                    logger.info(f"Message sent to SQS: {json.dumps(sqs_message)}")

    if exist_log_file_flag:
        # Update task status to In-progress after all subtask send to queue.
        logger.info("All subtask send to SQS queue")
        update_task_status(task_id, Task.IN_PROGRESS.value)
    else:
        # Update task status to Failed if no .gz log file in S3 bucket of the time range.
        logger.info("No .gz audit log file in S3 bucket of the time range")
        update_task_status(task_id, Task.FAILED.value, "No .gz audit log file in S3 bucket of the time range")

    return {
        'statusCode': 200,
        'body': json.dumps('Generate subtask successfully')
    }