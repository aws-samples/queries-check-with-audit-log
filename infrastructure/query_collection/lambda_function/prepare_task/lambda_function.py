import json
from datetime import datetime
import time
import os

export_bucket = os.getenv('BUCKET_NAME')

def datetime_to_timestamp(datetime_str):
    # Parse the datetime string
    dt_object = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M')
    
    # Convert to timestamp
    timestamp = int(time.mktime(dt_object.timetuple()))
    
    return timestamp

def lambda_handler(event, context):
    
    task_id = event['execution_id'].split(':')[-1]
    cluster_identifier = event['cluster_identifier']
    
    log_group_name = f'/aws/rds/cluster/{cluster_identifier}/audit'
    
    start_time_str = event['start_time']
    end_time_str = event['end_time']
    
    start_time = datetime_to_timestamp(start_time_str)
    end_time = datetime_to_timestamp(end_time_str)
    
    export_prefix = f'audit-log/{task_id}_{cluster_identifier}'
    
    return {
        'task_id': task_id,
        'log_group_name': log_group_name,
        'start_time':start_time,
        'end_time':end_time,
        'export_prefix': export_prefix,
        'export_bucket': export_bucket
    }
