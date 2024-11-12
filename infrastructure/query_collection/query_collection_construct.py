from aws_cdk import (
    aws_ec2 as ec2,
    aws_apigateway,    
    aws_iam,
    aws_secretsmanager,
)
from constructs import Construct
from infrastructure.query_collection.sqs.stack import SQS
from infrastructure.query_collection.security_group.stack import SecurityGroup
from infrastructure.query_collection.launch_template.stack import LaunchTemplate
from infrastructure.query_collection.step_function.stack import StepFunctions
from infrastructure.query_collection.asg.stack import ASG
from infrastructure.query_collection.lambda_function.stack import LambdaFunction
from infrastructure.query_collection.api_method.stack import ApiMethod


class QueryCollectionConstruct(Construct):
    def __init__(self, scope: Construct, construct_id: str, params: dict, bucket, dynamodb_tables, api: aws_apigateway.RestApi, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        vpc = ec2.Vpc.from_lookup(self, "ExistingVPC", vpc_id=params['vpc_id'])

        public_subnets = [ec2.Subnet.from_subnet_id(self, "public-subnet-{}".format(i), subnet_id=subnet_id) for i, subnet_id in enumerate(params['public_subnet_ids'])]
        
        sg = SecurityGroup(self, 'SecurityGroup', vpc=vpc, env_name=params['env_name'])

        secret_partial_arn = f'arn:aws:secretsmanager:{params["region"]}:{params["account"]}:secret:{params["secret"]}'

        secret = aws_secretsmanager.Secret.from_secret_attributes(self, "SecretsManager", secret_partial_arn = secret_partial_arn)

        sqs = SQS(self, "queries_queue", env_name=params['env_name'])
        params['queries_queue_url'] = sqs.queue.queue_url

        launch_template = LaunchTemplate(self, "launch_template",
                                         env_name=params['env_name'], bucket=bucket.bucket,
                                         region=params['region'], sqs=sqs.queries_compatibility_check_queue,
                                         dynamodb_tables=dynamodb_tables,
                                         secret = secret,
                                         key_name=params['keypair'], sg=sg.security_group)

        asg = ASG(self, "asg", vpc=vpc, public_subnets=public_subnets, env_name=params['env_name'], 
                  launch_template=launch_template.agent_launch_template) 
        params['asg_name'] = asg.asg.auto_scaling_group_name
        params['asg_arn'] = asg.asg.auto_scaling_group_arn

        lambda_function = LambdaFunction(self, 'LambdaFunction', params,
                                         sqs=sqs.queries_compatibility_check_queue,
                                         dynamodb_tables=dynamodb_tables,
                                         s3_bucket=bucket.bucket)

        step_functions = StepFunctions(self, "step_function", params, lambda_function)
        
        api_method = ApiMethod(self, "api_method", api=api,
                               env_name=params['env_name'],
                               create_state_machine=step_functions.create_step_function,
                               stop_task_function=lambda_function.stop_task_function,
                               get_task_progress_function=lambda_function.get_task_progress_function)

        # Set properties
        self.the_vpc = vpc
        self.the_security_group = sg.security_group
        self.queue = sqs.queries_compatibility_check_queue

    @property
    def vpc(self):
        return self.the_vpc

    @property
    def security_group(self):
        return self.the_security_group

    @property
    def sqs_queue(self):
        return self.queue
