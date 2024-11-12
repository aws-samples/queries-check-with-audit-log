import json
import boto3
import os
from botocore.exceptions import ClientError
from enum import Enum
import logging
import traceback
from datetime import datetime


logger = logging.getLogger()
logger.setLevel("INFO")

REGION = os.environ.get("REGION")

dynamodb = boto3.resource('dynamodb', region_name=REGION)
task_table_name = os.environ.get("DDB_TASK_TABLE")
task_table = dynamodb.Table(task_table_name)

subtask_table_name = os.environ.get("DDB_SUB_TASK_TABLE")
subtask_table = dynamodb.Table(subtask_table_name)


class Task(Enum):
    CREATED = 'Created'
    STOPPED = 'Stopped'
    COMPLETED = 'Completed'
    IN_PROGRESS = 'In-progress'
    FAILED = 'Failed'

class SubTask(Enum):
    CREATED = 'Created'
    STOPPED = 'Stopped'
    COMPLETED = 'Completed'
    IN_PROGRESS = 'In-progress'
    FAILED = 'Failed'


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
        logger.error("Error updating task:")
        logger.error(e)
        return None


def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event))
        
    for record in event['Records']:
        subtask_item = record['dynamodb']['NewImage']
        task_id = subtask_item['task_id']['S']
        total_count = int(subtask_item['total_count']['N'])
        error_count = int(subtask_item['error_count']['N'])
        warning_count = int(subtask_item['warning_count']['N'])
        try:
            response = task_table.update_item(
                Key={
                    'task_id': task_id  # Replace with your actual primary key field name and value
                },
                UpdateExpression="SET total_count = if_not_exists(total_count, :start) + :inc_total, "
                                 "error_count = if_not_exists(error_count, :start) + :inc_error, "
                                 "warning_count = if_not_exists(warning_count, :start) + :inc_warning, update_time = :ut",

                ExpressionAttributeValues={
                    ':inc_total': total_count,
                    ':inc_error': error_count,
                    ':inc_warning': warning_count,
                    ':start': 0,  # This will initialize the field if it does not exist
                    ':ut': datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                },
                ReturnValues="NONE"
            )
            logger.info("update task table sql count response: ", response)

            # Query the subtask table for all subtasks with the given task_id
            response = subtask_table.query(
                KeyConditionExpression=boto3.dynamodb.conditions.Key('task_id').eq(task_id)
            )

            # Check if any subtasks were found
            if 'Items' not in response or not response['Items']:
                logger.info(f"No subtasks found for task_id: {task_id}")
                return

            # Check the status of all subtasks
            all_completed = True
            for subtask in response['Items']:
                status = subtask.get('status')
                logger.info(
                    f"Subtask ID: {subtask['task_id']}, Status: {status}")  # Assuming there's a 'subtask_id' attribute
                if status != SubTask.COMPLETED.value:
                    all_completed = False
                    break

            # If all subtasks are completed, update the task status
            if all_completed:
                update_task_status(task_id, Task.COMPLETED.value)

        except ClientError as e:
            logger.error('Error updating task item:')
            traceback.print_exc()

    return {
        'statusCode': 200,
        'body': json.dumps('Update task successfully')
    }
