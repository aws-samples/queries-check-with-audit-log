from aws_cdk import (
    aws_apigateway,
    aws_iam,
    aws_stepfunctions as sfn,
    aws_lambda as _lambda
    )
from constructs import Construct


class ApiMethod(Construct):
    def __init__(self, scope: Construct, construct_id: str, env_name: str, api: aws_apigateway.RestApi, 
                 create_state_machine: sfn.CfnStateMachine,
                 stop_task_function: _lambda.Function,
                 get_task_progress_function: _lambda.Function, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # add resource 
        api_resource = api.root.add_resource("task")

        # execution role
        api_role = aws_iam.Role(self, "APIGatewayRole",
                        assumed_by=aws_iam.ServicePrincipal("apigateway.amazonaws.com"))
        
        execution_step_function_policy = aws_iam.Policy(
            self,
            "{}-api-execute-step-function-policy".format(env_name),
            policy_name="{}-api-execute-step-function-policy".format(env_name),
            statements=[
                aws_iam.PolicyStatement(
                    effect=aws_iam.Effect.ALLOW,
                    actions=['states:StartExecution'],
                    resources=[create_state_machine.attr_arn],
                )
            ]
        )
        api_role.attach_inline_policy(execution_step_function_policy)

        # add initiate check task method
        initiate_tasks_request_model = api.add_model(
            "post-task-request-model".format(env_name),
            model_name="PostTaskRequestModel{}".format(env_name),
            schema=aws_apigateway.JsonSchema(
                schema=None,
                type=aws_apigateway.JsonSchemaType.OBJECT,
                properties={
                    "check_percent": aws_apigateway.JsonSchema(type=aws_apigateway.JsonSchemaType.INTEGER, maximum=10, minimum=1),
                    "cluster_identifier": aws_apigateway.JsonSchema(type=aws_apigateway.JsonSchemaType.STRING),
                    "start_time": aws_apigateway.JsonSchema(type=aws_apigateway.JsonSchemaType.STRING),
                    "end_time": aws_apigateway.JsonSchema(type=aws_apigateway.JsonSchemaType.STRING),
                    "validate_cluster_endpoint": aws_apigateway.JsonSchema(type=aws_apigateway.JsonSchemaType.STRING),
                    "rerun": aws_apigateway.JsonSchema(type=aws_apigateway.JsonSchemaType.BOOLEAN)
                },
                required=["check_percent", "cluster_identifier", "start_time", "end_time", "validate_cluster_endpoint", "rerun"]
            )
        )

        initiate_tasks_response_mapping_template = """
            #set($inputRoot = $input.path('$'))
            #set($parts = $inputRoot.executionArn.split(":"))
            {
            "task_id" : "$parts[-1]",
            "message" : ""
            }
        """

        initiate_tasks_method = api_resource.add_method(
                "POST",
                aws_apigateway.AwsIntegration(
                    service="states",
                    options=aws_apigateway.IntegrationOptions(
                        credentials_role=api_role,
                        passthrough_behavior=aws_apigateway.PassthroughBehavior.WHEN_NO_TEMPLATES,
                        integration_responses=[
                            aws_apigateway.IntegrationResponse(
                                status_code="200",
                                response_templates={
                                    "application/json": initiate_tasks_response_mapping_template
                                }
                            )
                        ],
                        request_templates={
                            "application/json": "{\"input\": \"$util.escapeJavaScript($input.json('$'))\", \"stateMachineArn\": \"" + create_state_machine.attr_arn + "\"}"
                        },
                    ),
                    action='StartExecution'
                ),
                method_responses=[
                    aws_apigateway.MethodResponse(status_code="200")
                ],
                request_validator_options=aws_apigateway.RequestValidatorOptions(
                    validate_request_body=True,
                    validate_request_parameters=False
                ),
                api_key_required=True,
                request_models={
                    "application/json": initiate_tasks_request_model
                }
            )
        
        # add stop task method
        stop_tasks_request_model = api.add_model(
            "put-task-request-model".format(env_name),
            model_name="PutTaskRequestModel{}".format(env_name),
            schema=aws_apigateway.JsonSchema(
                schema=None,
                type=aws_apigateway.JsonSchemaType.OBJECT,
                properties={
                    "task_id": aws_apigateway.JsonSchema(type=aws_apigateway.JsonSchemaType.STRING, max_length=36, min_length=36)
                },
                required=["task_id"]
            )
        )

        stop_tasks_response_mapping_template = """
            {
            "message" : ""
            }
        """

        initiate_tasks_method = api_resource.add_method(
                "PUT",
                aws_apigateway.LambdaIntegration(stop_task_function),
                method_responses=[
                    aws_apigateway.MethodResponse(status_code="200")
                ],
                request_validator_options=aws_apigateway.RequestValidatorOptions(
                    validate_request_body=True,
                    validate_request_parameters=False
                ),
                api_key_required=True,
                request_models={
                    "application/json": stop_tasks_request_model
                }
            )

        lambda_integration = aws_apigateway.LambdaIntegration(get_task_progress_function)
        api_resource.add_method('GET', lambda_integration, api_key_required=True,)
