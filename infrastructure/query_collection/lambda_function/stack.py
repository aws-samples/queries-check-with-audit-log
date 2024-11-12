from aws_cdk import (
    Duration,
    aws_lambda,
    aws_sqs,
    aws_s3,
    aws_s3_notifications as s3_notifications,
    aws_iam
)

from constructs import Construct


class LambdaFunction(Construct):
    def __init__(self, scope: Construct, construct_id: str, params: dict,
                 sqs: aws_sqs.Queue, dynamodb_tables, s3_bucket: aws_s3.Bucket, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        env_name = params['env_name']
        region = params['region']
        account = params['account']
        
        # # lambda function
        # get_db_instance_type_lambda_role = aws_iam.Role(
        #     self,
        #     "{}-get-db-instance-type-lambda-role".format(env_name),
        #     role_name="{}-get-db-instance-type-lambda-role".format(env_name),
        #     assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
        #     managed_policies=[aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")]
        # )
        #
        # get_db_instance_type_lambda_role.add_to_policy(
        #     aws_iam.PolicyStatement(
        #         effect=aws_iam.Effect.ALLOW,
        #         actions=['rds:DescribeDBClusters'],
        #         resources=[f'arn:aws:rds:{region}:{account}:cluster:*'],
        #     )
        # )
        #
        # get_db_instance_type_lambda_role.add_to_policy(
        #     aws_iam.PolicyStatement(
        #         effect=aws_iam.Effect.ALLOW,
        #         actions=['rds:DescribeDBInstances'],
        #         resources=[f'arn:aws:rds:{region}:{account}:db:*'],
        #     )
        # )
        #
        # get_db_instance_type_lambda_role.add_to_policy(
        #     aws_iam.PolicyStatement(
        #         effect=aws_iam.Effect.ALLOW,
        #         actions=['ec2:DescribeNetworkInterfaces'],
        #         resources=['*'],
        #     )
        # )

        # 1. Update task lambda function and role
        update_task_lambda_role = aws_iam.Role(
            self,
            "{}-update_task-lambda-role".format(env_name),
            role_name="{}-update_task-lambda-role".format(env_name),
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")]
        )

        self.update_task_function = aws_lambda.Function(
            self, "update_task",
            code=aws_lambda.Code.from_asset("infrastructure/query_collection/lambda_function/update_task"),
            handler="lambda_function.lambda_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(60),
            memory_size=1024,
            function_name='{}-db-check-update_task'.format(env_name),
            role=update_task_lambda_role,
            environment={'REGION': region,
                         'DDB_TASK_TABLE': params['check_task_table_name'],
                         'DDB_SUB_TASK_TABLE': params['check_subtask_table_name'],
                }
            )
        dynamodb_tables.task_table.grant_read_write_data(self.update_task_function)
        dynamodb_tables.subtask_table.grant_read_write_data(self.update_task_function)
        self.update_task_function.add_event_source(dynamodb_tables.subtask_update_count_table_source)


        #2.  Create get task progress lambda function and role
        get_task_progress_lambda_role = aws_iam.Role(
            self,
            "{}-db-check-get-task-progress-lambda-role".format(env_name),
            role_name="{}-db-check-get-task-progress-lambda-role".format(env_name),
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")]
        )

        self.get_task_progress_function = aws_lambda.Function(
            self, "get_task_progress",
            code=aws_lambda.Code.from_asset("infrastructure/query_collection/lambda_function/get_task_progress"),
            handler="lambda_function.lambda_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(60),
            function_name='{}-db-check-get-task-progress'.format(env_name),
            role=get_task_progress_lambda_role,
            environment={'REGION': region,
                         'DDB_TASK_TABLE': params['check_task_table_name'],
                         'BUCKET_NAME': s3_bucket.bucket_name},
        )
        dynamodb_tables.task_table.grant_read_write_data(self.get_task_progress_function)
        s3_bucket.grant_read_write(get_task_progress_lambda_role)

        #3. generate report lambda function
        generate_subtask_function_role = aws_iam.Role(
            self,
            "{}-generate-subtask-function-role".format(params['env_name']),
            role_name="{}-generate-subtask-function-role".format(params['env_name']),
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")]
        )

        s3_bucket.grant_read(generate_subtask_function_role)
        dynamodb_tables.subtask_table.grant_read_write_data(generate_subtask_function_role)
        dynamodb_tables.task_table.grant_read_write_data(generate_subtask_function_role)
        sqs.grant_send_messages(generate_subtask_function_role)

        self.generate_subtask_function = aws_lambda.Function(
            self, "generate_subtask_function",
            code=aws_lambda.Code.from_asset("infrastructure/query_collection/lambda_function/generate_subtask"),
            handler="lambda_function.lambda_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(120),
            role=generate_subtask_function_role,
            function_name='{}-db-check-generate-subtask'.format(params['env_name'])
        )

        self.generate_subtask_function.add_environment('BUCKET_NAME', s3_bucket.bucket_name)
        self.generate_subtask_function.add_environment('CHECK_SUBTASK_TABLE_NAME', dynamodb_tables.subtask_table.table_name)
        self.generate_subtask_function.add_environment('CHECK_TASK_TABLE_NAME', dynamodb_tables.task_table.table_name)
        self.generate_subtask_function.add_environment('SQS_QUEUE_URL', sqs.queue_url)

        # 4. Prepare task lambda function
        self.prepare_task_function = aws_lambda.Function(
            self, "prepare_task_function",
            code=aws_lambda.Code.from_asset("infrastructure/query_collection/lambda_function/prepare_task"),
            handler="lambda_function.lambda_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(10),
            function_name='{}-db-check-prepare-task'.format(params['env_name'])
        )
        self.prepare_task_function.add_environment('BUCKET_NAME', s3_bucket.bucket_name)

        # 5. Generate SQL sample report lambda function
        self.generate_sql_sample_report_function = aws_lambda.Function(
            self, "generate_sql_sample_report_function",
            code=aws_lambda.Code.from_asset("infrastructure/query_collection/lambda_function/generate_sql_sample_report"),
            handler="lambda_function.lambda_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(10),
            function_name='{}-db-check-generate-sql-sample-report'.format(params['env_name'])
        )
        self.generate_sql_sample_report_function.add_environment('BUCKET_NAME', s3_bucket.bucket_name)
        self.generate_sql_sample_report_function.add_environment('SQL_SAMPLE_TABLE_NAME',
                                                                 dynamodb_tables.sql_example_table.table_name)
        self.generate_sql_sample_report_function.add_environment('CHECK_SUBTASK_TABLE_NAME',
                                                       dynamodb_tables.subtask_table.table_name)

        self.generate_sql_sample_report_function.add_environment('REGION', region)
        s3_bucket.grant_read_write(self.generate_sql_sample_report_function.role)
        dynamodb_tables.sql_example_table.grant_read_write_data(self.generate_sql_sample_report_function.role)
        dynamodb_tables.subtask_table.grant_read_write_data(self.generate_sql_sample_report_function.role)
        self.generate_sql_sample_report_function.add_event_source(dynamodb_tables.task_table_report_source)

        # 6. stop task lambda function
        self.stop_task_function = aws_lambda.Function(
            self, "stop_task_function",
            code=aws_lambda.Code.from_asset(
                "infrastructure/query_collection/lambda_function/stop_task"),
            handler="lambda_function.lambda_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(10),
            function_name='{}-db-check-stop-task'.format(params['env_name'])
        )
        self.stop_task_function.add_environment('CHECK_TASK_TABLE_NAME',
                                                                 dynamodb_tables.task_table.table_name)
        self.stop_task_function.add_environment('CHECK_SUBTASK_TABLE_NAME',
                                                                 dynamodb_tables.subtask_table.table_name)

        self.stop_task_function.add_environment('REGION', region)

        dynamodb_tables.task_table.grant_read_write_data(self.stop_task_function.role)
        dynamodb_tables.subtask_table.grant_read_write_data(self.stop_task_function.role)


