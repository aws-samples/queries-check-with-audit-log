from aws_cdk import (
    aws_dynamodb as dynamodb,
    aws_lambda_event_sources as source,
    aws_lambda
)
from constructs import Construct


class DynamoDBTables(Construct):
    def __init__(self, scope: Construct, construct_id: str,
                 params: dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        task_table = params['check_task_table_name']
        subtask_table = params['check_subtask_table_name']
        sql_example_table = params['check_sql_example_table_name']
        task_table_gsi = params['check_task_table_gsi_name']

        # Check task table
        gsi_name =task_table_gsi
        gsi_partition_key = dynamodb.Attribute(
            name='in_progress',
            type=dynamodb.AttributeType.NUMBER
        )
        gsi_sort_key = dynamodb.Attribute(
            name='created_time',
            type=dynamodb.AttributeType.STRING
        )
        gsi = dynamodb.GlobalSecondaryIndexProps(
            index_name=gsi_name,
            partition_key=gsi_partition_key,
            sort_key=gsi_sort_key,
            projection_type=dynamodb.ProjectionType.INCLUDE,
            non_key_attributes=['task_id']
        )

        self.task_table = dynamodb.Table(
            self, "check_task_table",
            table_name=task_table,
            partition_key=dynamodb.Attribute(name="task_id", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.AWS_MANAGED,
            stream=dynamodb.StreamViewType.NEW_IMAGE,
            point_in_time_recovery=True,
        )

        self.task_table.add_global_secondary_index(
            index_name=gsi_name,
            partition_key=gsi_partition_key,
            sort_key=gsi_sort_key,
            projection_type=dynamodb.ProjectionType.INCLUDE,
            non_key_attributes=['task_id']
        )

        # Check subtask table
        self.subtask_table = dynamodb.Table(
            self, "check_subtask_table",
            table_name=subtask_table,
            partition_key=dynamodb.Attribute(name="task_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="s3_object_key", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.AWS_MANAGED,
            stream=dynamodb.StreamViewType.NEW_IMAGE,
            point_in_time_recovery=True
        )

        # Subtask table stream
        self.subtask_update_count_table_source = source.DynamoEventSource(
            self.subtask_table,
            retry_attempts=1,
            batch_size=50,
            starting_position=aws_lambda.StartingPosition.LATEST,
            filters=[aws_lambda.FilterCriteria.filter(
                {
                    "dynamodb": {"NewImage": {"status": {"S": ["Completed"]}}},
                    "eventName": aws_lambda.FilterRule.is_equal("MODIFY")
                }
            )]
        )

        # Task table stream
        self.task_table_report_source = source.DynamoEventSource(
            self.task_table,
            retry_attempts=1,
            batch_size=50,
            starting_position=aws_lambda.StartingPosition.LATEST,
            filters=[aws_lambda.FilterCriteria.filter(
                {
                    "dynamodb": {"NewImage": {"status": {"S": ["Completed", "Stopped"]}}},
                    "eventName": aws_lambda.FilterRule.is_equal("MODIFY")
                }
            )]
        )

        # Check SQL example table
        self.sql_example_table = dynamodb.Table(
            self, "check_sql_example_table",
            table_name=sql_example_table,
            partition_key=dynamodb.Attribute(name="task_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sql_hash", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.AWS_MANAGED,
            stream=dynamodb.StreamViewType.NEW_IMAGE,
            point_in_time_recovery=True
        )
