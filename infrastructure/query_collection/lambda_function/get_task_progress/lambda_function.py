import boto3
import os
import json
import logging
from enum import Enum
import traceback

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Task(Enum):
    CREATED = "Created"
    STOPPED = "Stopped"
    COMPLETED = "Completed"
    IN_PROGRESS = "In-progress"
    FAILED = "Failed"

REGION = os.environ.get("REGION")

BUCKET_NAME = os.environ.get("BUCKET_NAME")

# dynamodb client
dynamodb = boto3.resource("dynamodb", region_name=REGION)
task_table_name = os.environ.get("DDB_TASK_TABLE")
task_table = dynamodb.Table(task_table_name)

def get_value_from_dict(data_dict, key_name, value_type):
    """
    Retrieve the value from the dictionary based on the key name and type.

    Parameters:
    - data_dict (dict): The dictionary to search.
    - key_name (str): The key to look for in the dictionary.
    - value_type (type): The expected type of the value ('str' or 'int').

    Returns:
    - The value associated with the key in the specified type, or default value if not found.
    """
    if key_name in data_dict:
        value = data_dict[key_name]
        if value_type == str:
            return value  # Return as is for strings
        elif value_type == int:
            return int(value)  # Convert to int and return
        else:
            return str(value)
    # Return default values if key is not found
    return 0 if value_type == int else ""


def get_task_info(task_id: str):
    """
        Retrieves information about a task from the DynamoDB table.

        Args:
            task_id (str): The ID of the task to retrieve information for.

        Returns:
            dict: A dictionary containing information about the task, including its status, progress, and report details.
    """

    return_dict = {}
    key = {
        "task_id": task_id
    }
    try:
        response = task_table.get_item(
            Key=key
        )
        logger.info("Get check_task item success!")
        logger.info(response)
        if "Item" in response and "task_id" in response["Item"] and "cluster_identifier" in response["Item"]:
            item = response["Item"]
            return_dict["task_id"] = item["task_id"]
            return_dict["cluster_identifier"] = item["cluster_identifier"]
            return_dict["start_time"] = get_value_from_dict(item, "start_time", str)
            return_dict["end_time"] = get_value_from_dict(item, "end_time", str)
            return_dict["check_percent"] = get_value_from_dict(item, "check_percent", int)
            return_dict["rerun"] = get_value_from_dict(item, "rerun", str)
            return_dict["validate_cluster_endpoint"] = get_value_from_dict(item, "validate_cluster_endpoint", str)
            return_dict["created_time"] = get_value_from_dict(item, "created_time", str)
            return_dict["status"] = get_value_from_dict(item, "status", str)
            return_dict["update_time"] = get_value_from_dict(item, "update_time", str)

            return_dict["error_message"] = get_value_from_dict(item, "error_message", str)
            return_dict["total_count"] = get_value_from_dict(item, "total_count", int)
            return_dict["error_count"] = get_value_from_dict(item, "error_count", int)
            return_dict["warning_count"] = get_value_from_dict(item, "warning_count", int)

            if return_dict["status"] == Task.COMPLETED.value or return_dict["status"] == Task.STOPPED.value:
                return_dict["error_report"] = "s3://" + BUCKET_NAME + "/report/" + task_id + "_" +item["cluster_identifier"] +"/error.csv"
                return_dict["warning_report"] = "s3://" + BUCKET_NAME + "/report/" + task_id + "_" +item["cluster_identifier"] +"/warning.csv"
                return_dict["sample_sql_report"] = "s3://" + BUCKET_NAME + "/report/" + task_id + "_" +item["cluster_identifier"] +"/sample_sql.csv"
        else:
            return_dict["message"] = "The task_id is not in DynamoDB table, or no cluster_identifier in task item."
    except Exception as e:
        logger.error("Get check_task item failed! key = " + str(key))
        error_traceback = traceback.format_exc()
        logger.error(error_traceback)
        return_dict["message"] = str(e)

    return return_dict


def lambda_handler(event, context):
    if event.get("queryStringParameters") and event["queryStringParameters"].get("task_id"):
        resp_body = get_task_info(event["queryStringParameters"]["task_id"])
    else:
        resp_body = {"message": "Please input task_id."}
    return {
        "statusCode": 200,  # Custom success code (optional)
        "body": json.dumps(resp_body)
    }
