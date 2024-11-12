import json
from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_iam as iam
)
from constructs import Construct
from infrastructure.query_collection.lambda_function.stack import LambdaFunction

class StepFunctions(Construct):
    def __init__(self, scope: Construct, construct_id: str, 
                 params: dict, lambda_functions: LambdaFunction, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        env_name = params['env_name']
        region = params["region"]
        account = params["account"]
        check_task_table_name = params["check_task_table_name"]
        check_subtask_table_name = params["check_subtask_table_name"]
        sqs_queue_name = "{}-queries-check-audit-log-queue".format(env_name)
        create_step_function_arn = f"arn:aws:states:{region}:{account}:execution:CreateTaskStateMachine_{env_name}"

        # Create policy document
        policy = iam.Policy(
            self,
            "{}-StepFunctionPolicy".format(env_name),
            policy_name="{}-step-function-policy".format(env_name),
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['dynamodb:PutItem',
                             'dynamodb:GetItem',
                             'dynamodb:UpdateItem',
                             'dynamodb:Query' ],
                    resources=[f'arn:aws:dynamodb:{region}:{account}:table/{check_task_table_name}',
                               f'arn:aws:dynamodb:{region}:{account}:table/{check_task_table_name}/index/*',
                               f'arn:aws:dynamodb:{region}:{account}:table/{check_subtask_table_name}',
                               f'arn:aws:dynamodb:{region}:{account}:table/{check_subtask_table_name}/index/*'],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['lambda:InvokeFunction'],
                    resources=[
                        f'{lambda_functions.prepare_task_function.function_arn}:*', 
                        lambda_functions.prepare_task_function.function_arn,
                        f'{lambda_functions.generate_subtask_function.function_arn}:*', 
                        lambda_functions.generate_subtask_function.function_arn
                        ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['logs:DescribeExportTasks','logs:CreateExportTask'],
                    resources=[f'arn:aws:logs:{region}:{account}:log-group:*'],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['sqs:PurgeQueue'],
                    resources=[f'arn:aws:sqs:{region}:{account}:{sqs_queue_name}'],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['states:StopExecution'],
                    resources=['{}:*'.format(create_step_function_arn)],
                )
            ]
        )

        role = iam.Role(self,  "{}-StepFunctionRole".format(env_name),
                        role_name="{}-step-function-role".format(env_name),
                        assumed_by=iam.ServicePrincipal("states.amazonaws.com")
                        )

        role.attach_inline_policy(policy)

        create_function_definition = '''
            {
              "Comment": "A description of my state machine",
              "StartAt": "prepare_task",
              "States": {
                "prepare_task": {
                  "Type": "Task",
                  "Resource": "arn:aws:states:::lambda:invoke",
                  "Parameters": {
                    "FunctionName": "arn:aws:lambda:ap-southeast-1:637423544808:function:jtaudit-db-check-prepare-task",
                    "Payload": {
                      "execution_id.$": "$$.Execution.Id",
                      "cluster_identifier.$": "$.cluster_identifier",
                      "start_time.$": "$.start_time",
                      "end_time.$": "$.end_time"
                    }
                  },
                  "Retry": [
                    {
                      "ErrorEquals": [
                        "Lambda.ServiceException",
                        "Lambda.AWSLambdaException",
                        "Lambda.SdkClientException",
                        "Lambda.TooManyRequestsException"
                      ],
                      "IntervalSeconds": 1,
                      "MaxAttempts": 3,
                      "BackoffRate": 2
                    }
                  ],
                  "Next": "create_task",
                  "ResultPath": "$.prepare_task",
                  "ResultSelector": {
                    "task_id.$": "$.Payload.task_id",
                    "log_group_name.$": "$.Payload.log_group_name",
                    "start_time.$": "$.Payload.start_time",
                    "end_time.$": "$.Payload.end_time",
                    "export_bucket.$": "$.Payload.export_bucket",
                    "export_prefix.$": "$.Payload.export_prefix"
                  }
                },
                "create_task": {
                  "Type": "Task",
                  "Resource": "arn:aws:states:::dynamodb:putItem",
                  "Parameters": {
                    "TableName": "jtaudit-aurora-check-task",
                    "Item": {
                      "task_id": {
                        "S.$": "$.prepare_task.task_id"
                      },
                      "cluster_identifier": {
                        "S.$": "$.cluster_identifier"
                      },
                      "start_time": {
                        "S.$": "$.start_time"
                      },
                      "end_time": {
                        "S.$": "$.end_time"
                      },
                      "check_percent": {
                        "N.$": "States.JsonToString($.check_percent)"
                      },
                      "validate_cluster_endpoint": {
                        "S.$": "$.validate_cluster_endpoint"
                      },
                      "error_message": {
                        "S": ""
                      },
                      "status": {
                        "S": "Created"
                      },
                      "created_time": {
                        "S.$": "$$.State.EnteredTime"
                      },
                      "rerun": {
                        "BOOL.$": "$.rerun"
                      }
                    }
                  },
                  "Next": "create_export_task",
                  "ResultPath": "$.create_task"
                },
                "create_export_task": {
                  "Type": "Task",
                  "Parameters": {
                    "Destination.$": "$.prepare_task.export_bucket",
                    "DestinationPrefix.$": "$.prepare_task.export_prefix",
                    "From.$": "$.prepare_task.start_time",
                    "LogGroupName.$": "$.prepare_task.log_group_name",
                    "To.$": "$.prepare_task.end_time"
                  },
                  "Resource": "arn:aws:states:::aws-sdk:cloudwatchlogs:createExportTask",
                  "Next": "describe_export_task",
                  "ResultPath": "$.create_export_task",
                  "Catch": [
                    {
                      "ErrorEquals": [
                        "States.ALL"
                      ],
                      "Next": "update_task_failed",
                      "ResultPath": "$.create_export_task_error"
                    }
                  ]
                },
                "describe_export_task": {
                  "Type": "Task",
                  "Parameters": {
                    "TaskId.$": "$.create_export_task.TaskId"
                  },
                  "Resource": "arn:aws:states:::aws-sdk:cloudwatchlogs:describeExportTasks",
                  "Next": "is_finished",
                  "ResultSelector": {
                    "status.$": "$.ExportTasks[0].Status"
                  },
                  "ResultPath": "$.describe_export_task"
                },
                "is_finished": {
                  "Type": "Choice",
                  "Choices": [
                    {
                      "Variable": "$.describe_export_task.status.Code",
                      "StringEquals": "RUNNING",
                      "Next": "wait"
                    },
                    {
                      "Variable": "$.describe_export_task.status.Code",
                      "StringEquals": "COMPLETED",
                      "Next": "generate_sub_task"
                    }
                  ],
                  "Default": "update_task_failed"
                },
                "update_task_failed": {
                  "Type": "Task",
                  "Resource": "arn:aws:states:::dynamodb:updateItem",
                  "Parameters": {
                    "TableName": "jtaudit-aurora-check-task",
                    "Key": {
                      "task_id": {
                        "S.$": "$.prepare_task.task_id"
                      }
                    },
                    "UpdateExpression": "SET #s = :exported, #ut = :ut",
                    "ExpressionAttributeNames": {
                      "#s": "status",
                      "#ut": "update_time"
                    },
                    "ExpressionAttributeValues": {
                      ":exported": "ExportFailed",
                      ":ut.$": "$$.State.EnteredTime"
                    }
                  },
                  "End": true
                },
                "wait": {
                  "Type": "Wait",
                  "Seconds": 10,
                  "Next": "describe_export_task"
                },
                "generate_sub_task": {
                  "Type": "Task",
                  "Resource": "arn:aws:states:::lambda:invoke",
                  "OutputPath": "$.Payload",
                  "Parameters": {
                    "FunctionName": "arn:aws:lambda:ap-southeast-1:637423544808:function:jtaudit-db-check-generate-subtask",
                    "Payload": {
                      "task_id.$": "$.prepare_task.task_id",
                      "cluster_identifier.$": "$.cluster_identifier",
                      "validate_cluster_endpoint.$": "$.validate_cluster_endpoint",
                      "check_percent.$": "$.check_percent",
                      "rerun.$": "$.rerun",
                      "s3_bucket.$": "$.prepare_task.export_bucket"
                    }
                  },
                  "Retry": [
                    {
                      "ErrorEquals": [
                        "Lambda.ServiceException",
                        "Lambda.AWSLambdaException",
                        "Lambda.SdkClientException",
                        "Lambda.TooManyRequestsException"
                      ],
                      "IntervalSeconds": 1,
                      "MaxAttempts": 3,
                      "BackoffRate": 2
                    }
                  ],
                  "End": true
                }
              }
            }
        '''

        create_function_definition_dict = json.loads(create_function_definition)

        create_function_definition_dict['States']['create_task']['Parameters']['TableName'] = params['check_task_table_name']
        create_function_definition_dict['States']['update_task_failed']['Parameters']['TableName'] = params['check_task_table_name']

        create_function_definition_dict['States']['prepare_task']['Parameters']['FunctionName'] = lambda_functions.prepare_task_function.function_arn
        create_function_definition_dict['States']['generate_sub_task']['Parameters']['FunctionName'] = lambda_functions.generate_subtask_function.function_arn

        self.create_step_function = sfn.CfnStateMachine(
            self,
            "{}_createTaskStateMachine".format(env_name),
            state_machine_name="{}_createTaskStateMachine".format(env_name),
            role_arn=role.role_arn,
            definition_string=json.dumps(create_function_definition_dict)
        )

        # execution_policy = iam.Policy(
        #     self,
        #     "{}-StepFunctionExecutionPolicy".format(env_name),
        #     policy_name="{}-step-function-execution-policy".format(env_name),
        #     statements=[
        #         iam.PolicyStatement(
        #             effect=iam.Effect.ALLOW,
        #             actions=['states:StartExecution'],
        #             resources=[self.stop_step_function.attr_arn],
        #         )
        #     ]
        # )
        #
        # role.attach_inline_policy(execution_policy)

