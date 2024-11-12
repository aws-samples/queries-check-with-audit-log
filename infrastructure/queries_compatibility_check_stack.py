from constructs import Construct
from aws_cdk import (
    Aws,
    Stack,
)
try:
    from aws_cdk import core as cdk
except ImportError:
    import aws_cdk as cdk

from infrastructure import stack_input
from infrastructure.query_collection.query_collection_construct import QueryCollectionConstruct
from infrastructure.shared_infrastructure.shared_infrastructure_construct import SharedInfrastructureConstruct
from cdk_nag import NagSuppressions

class QueriesCompatibilityCheckStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        stack_name = '{}-{}'.format(stack_input.env_name, construct_id)
        super().__init__(scope, stack_name, **kwargs)

        NagSuppressions.add_stack_suppressions(
            self,
            [
                {
                    "id": "AwsSolutions-IAM4",
                    "reason": "We only used managed policy (lambda basic execution policy) for lambda functions to store logs in CloudWatch."
                },{
                    "id": "AwsSolutions-S1",
                    "reason": "S3 buckets in this stack don't require server access logging"
                },{
                    "id": "AwsSolutions-APIG2",
                    "reason": "The restful api use key api to validate request."
                },{
                    "id": "AwsSolutions-APIG6",
                    "reason": "Added a access log for the entire api gateway."
                },{
                    "id": "AwsSolutions-COG4",
                    "reason": "The restful api use key api to validate request."
                },{
                    "id": "AwsSolutions-APIG4",
                    "reason": "The restful api use key api to validate request."
                },{
                    "id": "AwsSolutions-IAM5",
                    "reason": "The policy specified specific resources. The wildcard permission is for items/objects/messages within the resource."
                },{
                    "id": "AwsSolutions-L1",
                    "reason": "False alarm."
                },{
                    "id": "AwsSolutions-SQS3",
                    "reason": "We have customerized retry mechanism."
                },{
                    "id": "AwsSolutions-SQS4",
                    "reason": "We used SDK to access SQS."
                },{
                    "id": "AwsSolutions-SF1",
                    "reason": "Logging it not required for this stack."
                },{
                    "id": "AwsSolutions-SF2",
                    "reason": "Xray is not required for this stuck."
                },{
                    "id": "AwsSolutions-AS3",
                    "reason": "Notification is not required for this stack."
                },
            ]
        )

        # Parameters for CDK
        region = Aws.REGION
        account = Aws.ACCOUNT_ID

        params = {
            'region': region,
            'account': account,
            'env_name': stack_input.env_name,
            'vpc_id': stack_input.vpc_id,
            'public_subnet_ids': stack_input.public_subnet_ids,
            'keypair': stack_input.keypair,
            'secret': stack_input.secret,
            'check_task_table_name': '{}-aurora-check-task'.format(stack_input.env_name),
            'check_subtask_table_name': '{}-aurora-check-subtask'.format(stack_input.env_name),
            'check_sql_example_table_name': '{}-aurora-check-sql-sample'.format(stack_input.env_name),
            'check_task_table_gsi_name': 'in-progress-time-index'
        }

        shared_infrastructure = SharedInfrastructureConstruct(self, "SharedInfrastructureConstruct", params=params)

        query_collection = QueryCollectionConstruct(self, "QueryCollectionConstruct", 
                                                    params=params, bucket=shared_infrastructure.s3_bucket, api=shared_infrastructure.api.api, 
                                                    dynamodb_tables=shared_infrastructure.dynamodb)
