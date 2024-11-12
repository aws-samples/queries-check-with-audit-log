import boto3
import configparser
import json
import gzip
import csv
import os
from botocore.exceptions import ClientError
import ast
from datetime import datetime
from hashlib import blake2b
import re
import uvloop
import aiomysql
import traceback
import io

import asyncio

def read_config(path):
    """
    Initialize the global configuration file.
    @param path Configuration file address
    @return Configuration file DICT object"
    """
    config = configparser.ConfigParser()
    config.read(path)
    return config

config = read_config('./config.conf')

region = config.get('DEFAULT', 'region')
queue_url = config.get('DEFAULT', 'queue_url')
subtask_dynamodb_name = config.get('DEFAULT', 'subtask_dynamodb_name')
sql_sample_dynamodb_name = config.get('DEFAULT', 'sql_sample_dynamodb_name')
secrets_name = config.get('DEFAULT', 'secret_name')

max_concurrency = 20

total_count = 0
error_query = []
warning_query = []
sample_query = []

# Initialize a session using Amazon SQS
sqs = boto3.client('sqs', region_name=region)

# Initialize a session using Amazon DynamoDB
dynamodb = boto3.resource('dynamodb', region_name=region)
subtask_table = dynamodb.Table(subtask_dynamodb_name)
sql_sample_table = dynamodb.Table(sql_sample_dynamodb_name)

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

def log(message, key=''):
    time_string = datetime.now().strftime("%H:%M:%S")
    print(f'--- {time_string} --- {key}: {message}')

# process each message (a subtask)
def process_message(message):
    # global db_connection
    global total_count, error_query, warning_query, sample_query

    # parse message
    sub_task = json.loads(message)
    task_id = sub_task['task_id']
    log(message=f'****{task_id}', key='task_id')
    cluster_identifier = sub_task['cluster_identifier']
    validate_cluster_endpoint = sub_task.get('validate_cluster_endpoint','')
    s3_bucket_name = sub_task['s3_bucket']
    s3_object_key = sub_task['s3_object_key']
    check_percent = sub_task['check_percent'] # int from 1 to 10
    rerun = sub_task.get('rerun', False)

    total_count = 0
    error_query = []
    warning_query = []
    sample_query = []

    # update subtask status to In-progress
    update_result = update_subtask_status(task_id, s3_object_key, status='In-progress', condition_status='Created')

    if update_result == False:
        return
    
    log('done', key='update_subtask_status')

    logs = load_and_unzip_s3_file(s3_bucket_name, s3_object_key, check_percent, rerun)

    log('done', key='load_and_unzip_s3_file')
    log(len(logs), key='logs size')

    if rerun:
        crednetials = get_secret_from_secret_manager(secrets_name)
        log('done', key='get_secret_from_secret_manager')

        db_config = {
            'host': validate_cluster_endpoint,
            'user': crednetials['username'],
            'password': crednetials['password'],
            'charset': 'utf8mb4',
            'cursorclass': aiomysql.DictCursor
        }

        results = asyncio.run(run_tasks_with_semaphore(logs=logs, 
                                                    max_concurrency=max_concurrency, 
                                                    task_id=task_id, 
                                                    check_percent=check_percent,
                                                    db_config=db_config))
        
        log('done', key='run_tasks_with_semaphore')
        log(len(results), key='result count')

        for result in results:
            if result is not None:
                code = result.get('code', 0)
                if code == 2:
                    error_query.append(result)
        log('done', key='loop_results')

    insert_sql_sample(task_id)

    log('done', key='insert_samples')

    report_key = f'report/{task_id}_{cluster_identifier}/'
    if len(error_query) > 0:
        export_report(bucket_name=s3_bucket_name, report_type='error', prefix=report_key, data=error_query)

    log('done', key='export_report')

    # update subtask status to Completed
    update_result = update_subtask_status(task_id, s3_object_key, 'Completed', 'In-progress', total_count=total_count, error_count=len(error_query), warning_count=len(warning_query))
    if update_result == False:
        return

    log('done', key='update_subtask_status')

# get secret (mysql credentials) from secret manager
def get_secret_from_secret_manager(validate_cluster_secret_key):

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=validate_cluster_secret_key
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']
    credentials = json.loads(secret)
    return credentials

# load and unzip s3 file get each lines of log
def load_and_unzip_s3_file(bucket_name, file_key, check_percent, rerun):
  """Loads a .gz file from S3, unzips it locally, and reads each line.

  Args:
    bucket_name: The name of the S3 bucket.
    file_key: The key of the file within the bucket.
  """
  global sample_query, total_count

  logs = []

  s3 = boto3.client('s3')

  # Download the .gz file to a temporary file
  temp_file_path = '/home/ec2-user/temp.gz'
  response = s3.download_file(bucket_name, file_key, temp_file_path)

  task_count = {}

  # Unzip the file
  # Open the gzipped file
  with gzip.open(temp_file_path, 'rt') as gz_file:
        # Create a CSV reader object
        csv_reader = csv.reader(gz_file)
        # Read and process each line of the CSV file
        for line in csv_reader:
            try:
                total_count = total_count + 1
                # Process the line as needed

                time = line[0]
                user = line[2]
                src_ip = line[3]
                operation = line[6]
                database = line[7]
                if user != "rdsadmin" and operation == "QUERY":
                    query_line = line[8:len(line)-1]
                    query = ', '.join(query_line)
                    query = ast.literal_eval(query)

                    log = {
                        'time': time,
                        'database': database,
                        'query': query,
                        'user': user,
                        'src_ip': src_ip
                    }
                    log['sql_mask'] = mask_sql(log['query'])
                    log['sql_hash'] = blake2b(log['sql_mask'].encode('utf-8')).hexdigest()

                    if log['sql_hash'] in task_count:
                        task_count[log['sql_hash']] = task_count[log['sql_hash']] + 1
                    else:
                        task_count[log['sql_hash']] = 1
                        sample_query.append(log)

                    if rerun:
                        if task_count[log['sql_hash']] % 10 < check_percent and log['sql_mask'].strip().lower().startswith('select'):
                            logs.append(log)
            except Exception as e:
                print(line)
                print(f"An error occurred in load_and_unzip_s3_file for above line : {e}")
                traceback.print_exc()

                
  # remove the temp file from disc
  os.remove(temp_file_path)

  return logs

# create async tasks and all results
async def run_tasks_with_semaphore(logs, max_concurrency, task_id, check_percent, db_config):

    pool = await aiomysql.create_pool(**db_config, minsize=1, maxsize=max_concurrency)

    semaphore = asyncio.Semaphore(max_concurrency)
    
    tasks = []
    async_results = []

    async def wrapped_task(log):
        async with semaphore:
            result = await process_log(log=log, pool=pool)
            return result

    batch_count = 0

    for log in logs:
        task = asyncio.create_task(wrapped_task(log))
        tasks.append(task)

        if len(tasks) >= max_concurrency * 2:
            batch_count = batch_count + 1
            if batch_count%1000==0:
                print('1000 batches')
                print(len(async_results))
            batch_result = await asyncio.gather(*tasks)
            async_results.extend(batch_result)
            tasks = []
    
    if tasks:
        batch_result = await asyncio.gather(*tasks)
        async_results.extend(batch_result)

    
    pool.close()
    await pool.wait_closed()


    return async_results

# process each line of logs
async def process_log(log, pool):

    result = await execute_query(database=log['database'], query=log['query'], pool=pool)
    log['message']=result['message']
    log['code']=result['code']

    return log


def execute_query_print(database, query, pool):
    result = {
        'code': 0,
        'message': ''
    }
    return result

# execute sql query
async def execute_query(database, query, pool):
    result = {
        'code': 0,
        'message': ''
    }
    async with pool.acquire() as db_connection:
        try:
            await db_connection.select_db(database)
            async with db_connection.cursor() as cursor:
                await cursor.execute(query)
        except Exception as e:
            # Handle any errors that occurred during query execution
            result['code'] = 2;
            result['message'] = str(e)
            traceback.print_exc()
        
    return result

# update subtask status
def update_subtask_status(task_id, s3_object_key, status, condition_status, total_count=0, error_count=0, warning_count=0):
    try:
        response = subtask_table.update_item(
            Key={
                'task_id': task_id,
                's3_object_key': s3_object_key
            },
            UpdateExpression="set #status = :s, #tc = :tc, #ec = :ec, #wc = :wc, #ut = :ut",
            ConditionExpression = "#status = :condition",
            ExpressionAttributeNames={
                '#status': 'status',
                '#tc': 'total_count',
                '#ec': 'error_count',
                '#wc': 'warning_count',
                '#ut': 'update_time'
            },
            ExpressionAttributeValues={
                ':s': status,
                ':tc': total_count,
                ':ec': error_count,
                ':wc': warning_count,
                ':ut': datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                ':condition': condition_status
            },
        )
        
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print("The conditional check failed, item was not updated.")    
        return False   


# insert sql sample
def insert_sql_sample(task_id):
    global sample_query

    try:
        with sql_sample_table.batch_writer() as batch:
            for log in sample_query:
                batch.put_item(Item={
                    'task_id': task_id,
                    'sql_hash': log['sql_hash'],
                    'sql_mask': log['sql_mask'],
                    'sql_sample': log['query'],
                    'database': log['database']
                })
    except ClientError as e:
        print(f"Error inserting sql samples: {e.response['Error']['Message']}")

# mask sql query
def mask_sql(query):
    # Remove escaped characters
    query = query = re.sub(r"\\\0", "", query)
    query = query = re.sub(r"\\\'", "", query)
    query = query = re.sub(r'\\\"', "", query)
    query = query = re.sub(r"\\\b", "", query)
    query = query = re.sub(r"\\\n", "", query)
    query = query = re.sub(r"\\\r", "", query)
    query = query = re.sub(r"\\\t", "", query)
    query = query = re.sub(r"\\\Z", "", query)
    query = query = re.sub(r"\\\\", "", query)
    query = query = re.sub(r"\\\%", "", query)
    query = query = re.sub(r"\\\_", "", query)

    # Remove -- style comments
    query = re.sub(r'--.*?(\n|$)', '\n', query)
    
    # Remove # style comments
    query = re.sub(r'#.*?(\n|$)', '\n', query)
    
    # Remove /* */ style comments
    query = re.sub(r'/\*[\s\S]*?\*/', '', query)
    
    # Remove extra whitespace
    query = re.sub(r'\s+', ' ', query).strip()

    # Mask string value
    query = re.sub(r"'[^']*'", "''", query)

    # Mask digit value
    # Replace all matches with '1'
    query = re.sub(r'\b\d+(\.\d+)?\b', '1', query)

    return query

# export to report
def export_report(bucket_name, report_type, prefix, data):
    file_key = f'{prefix}{report_type}.csv'
    # Initialize S3 client
    fieldnames = data[0].keys()
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        existing_content = response['Body'].read().decode('utf-8')
        csv_buffer = io.StringIO(existing_content)
        csv_buffer.seek(0, io.SEEK_END)
    except s3_client.exceptions.NoSuchKey:
        csv_buffer = io.StringIO()
        # Write the header
        fieldnames = data[0].keys()
        writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
        writer.writeheader()

    # Append the new rows
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    for row in data:
        writer.writerow(row)

    csv_buffer.seek(0)

    # Upload the updated content to S3
    s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())

# start from get message from sqs
def receive_messages():
    while True:
        try:
            # Receive message from SQS queue
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,  # Adjust as needed
                WaitTimeSeconds=1,  # Long polling
                VisibilityTimeout=3600
            )

            messages = response.get('Messages', [])
            if messages:
                for message in messages:
                    # Print the message body
                    process_message(message['Body'])
                    
                    # Delete the message from the queue after processing
                    response = sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )

                    print(response)

        except Exception as e:
            print(f"An error occurred: {e}")
            traceback.print_exc()

if __name__ == "__main__":
    receive_messages()