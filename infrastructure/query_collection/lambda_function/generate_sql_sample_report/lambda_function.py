import boto3
import os
import csv
import logging
from enum import Enum
import json
import traceback

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Task(Enum):
    CREATED = "Created"
    STOPPED = "Stopped"
    COMPLETED = "Completed"
    IN_PROGRESS = "In-progress"
    FAILED = "Failed"

class SubTask(Enum):
    CREATED = 'Created'
    STOPPED = 'Stopped'
    COMPLETED = 'Completed'
    IN_PROGRESS = 'In-progress'
    FAILED = 'Failed'

REGION = os.environ.get("REGION")
BUCKET_NAME = os.environ.get("BUCKET_NAME")

# S3 client
s3 = boto3.client('s3')
# dynamodb client
dynamodb = boto3.resource("dynamodb", region_name=REGION)

sql_sample_table_name = os.environ.get("SQL_SAMPLE_TABLE_NAME")
sql_sample_table = dynamodb.Table(sql_sample_table_name)

subtask_table_name = os.environ.get("CHECK_SUBTASK_TABLE_NAME")
subtask_table = dynamodb.Table(subtask_table_name)


def get_sample_items(task_id):
    csv_items = []

    response = sql_sample_table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('task_id').eq(task_id),
    )

    items = response['Items']
    for item in items:
        csv_item = [task_id, item['sql_sample'].replace("\"", ""), item['sql_mask'].replace("\"", ""), item['sql_hash']]
        csv_items.append(csv_item)

    while 'LastEvaluatedKey' in response:
        response = sql_sample_table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('task_id').eq(task_id),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items = response['Items']
        for item in items:
            csv_item = [task_id, item['sql_sample'].replace("\"", ""), item['sql_mask'].replace("\"", ""), item['sql_hash']]
            csv_items.append(csv_item)

    return csv_items


def lambda_handler(event, context):
    record = event['Records'][0]

    task_item = record['dynamodb']['NewImage']
    logger.info(task_item)

    task_id = task_item['task_id']['S']
    status = task_item['status']['S']
    cluster_identifier = task_item['cluster_identifier']['S']

    try:
        sample_sql_items = get_sample_items(task_id=task_id)

        sample_sql_report_key = 'report/{}_{}/sample_sql.csv'.format(task_id, cluster_identifier)

        # write item to csv file
        with open('/tmp/sample_sql.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerows(sample_sql_items)

        s3.upload_file('/tmp/sample_sql.csv', BUCKET_NAME, sample_sql_report_key)
        logger.info("Successfully generate sample sql report")
    except Exception as e:
        logger.error("Error generate sample sql report:")
        traceback.print_exc()
        return {
            "statusCode": 200,  # Custom success code (optional)
            "body": json.dumps("Failed to generate sample sql report!")
        }

    return {
        "statusCode": 200,  # Custom success code (optional)
        "body": json.dumps("Successfully!")
    }

