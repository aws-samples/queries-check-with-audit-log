import boto3
import os
import logging
from enum import Enum
import json
import traceback
from botocore.exceptions import ClientError
from datetime import datetime

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

task_table_name = os.environ.get("CHECK_TASK_TABLE_NAME")
task_table = dynamodb.Table(task_table_name)

subtask_table_name = os.environ.get("CHECK_SUBTASK_TABLE_NAME")
subtask_table = dynamodb.Table(subtask_table_name)


def update_subtask_status(task_id):
    try:
        sub_task_response = subtask_table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('task_id').eq(task_id)
        )
        # Loop through each item and update its status
        for item in sub_task_response['Items']:
            s3_object_key = item['s3_object_key']

            response = subtask_table.update_item(
                    Key={
                        'task_id': task_id,
                        's3_object_key': s3_object_key
                    },
                    UpdateExpression="set #status = :s, #ut = :ut",
                    ExpressionAttributeNames={
                        '#status': 'status',
                        '#ut': 'update_time'
                    },
                    ExpressionAttributeValues={
                        ':s': SubTask.STOPPED.value,
                        ':ut': datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                    },
                )

        return True
    except ClientError as e:
        logger.error(e)
        return False
    except Exception as e:
        logger.error(e)
        return False


def update_task_status(task_id, new_status):
    try:
        # Update the item with the specified task_id
        response = task_table.update_item(
            Key={
                'task_id': task_id  # Specify the primary key (partition key)
            },
            UpdateExpression="SET #status = :new_status, #ut = :ut",  # Update expression to set new status
            ExpressionAttributeNames={
                '#status': 'status',  # Use a placeholder for the attribute name
                '#ut': 'update_time'
            },
            ExpressionAttributeValues={
                ':new_status': new_status,  # New value for the status attribute
                ':ut': datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            },
            ReturnValues="UPDATED_NEW"  # Return the updated attributes
        )

        logger.info("Update succeeded:")
        logger.info(response)
        return response

    except Exception as e:
        logger.error("Error updating item:")
        logger.error(e)
        return None


def lambda_handler(event, context):
    logger.info(event)
    if event.get("body") and json.loads(event.get("body")).get("task_id"):
        task_id = json.loads(event.get("body")).get("task_id")
        try:
            key = {
                "task_id": task_id
            }
            response = task_table.get_item(
                Key=key
            )
            if "Item" in response and "task_id" in response["Item"] and "cluster_identifier" in response["Item"]:
                item = response["Item"]
                status = item['status']
                # Only process STOPPED to update all subtask status.
                if status == Task.CREATED.value:
                    resp_body = {"message": "Task " + task_id + " can not be stopped when the status is Created."}
                else:
                    update_task_status(task_id=task_id, new_status=Task.STOPPED.value)
                    update_subtask_status(task_id=task_id)
                    resp_body = {"message": "Task " + task_id + " stopped successfully."}
        except Exception as e:
            logger.error("Error in stop task function:")
            traceback.print_exc()
            resp_body = {"message": json.dumps("Failed to stop task, " + str(e))}
    else:
        resp_body = {"message": "Please input task_id."}
    return {
        "statusCode": 200,  # Custom success code (optional)
        "body": json.dumps(resp_body)
    }