
from aws_cdk import (
    aws_apigateway,
    aws_logs,
)

from constructs import Construct


class API(Construct):
    def __init__(self, scope: Construct, construct_id: str, env_name: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        api_log_group = aws_logs.LogGroup(self, 
                                          "db-check-api-log-group", 
                                          log_group_name="{}-db-check-api-log-group".format(env_name),
                                          )

        endpoint_configuration=aws_apigateway.EndpointConfiguration(types=[aws_apigateway.EndpointType.REGIONAL])

        self.api = aws_apigateway.RestApi(
            self, "db-check-api",
            rest_api_name="{}-db-check-api".format(env_name),
            description="db check api",
            cloud_watch_role=True,
            deploy=True,
            endpoint_configuration=endpoint_configuration,
            deploy_options=aws_apigateway.StageOptions(
                                 access_log_destination=aws_apigateway.LogGroupLogDestination(api_log_group),
                                 access_log_format=aws_apigateway.AccessLogFormat.clf(),
                                 throttling_burst_limit=1000,
                                 throttling_rate_limit=10000
                             ),
            )
        
        api_key = self.api.add_api_key(
            "db-check-api-key",
            api_key_name="{}-db-check-api-key".format(env_name),
            description="db check api key",
        )

        usage_plan = self.api.add_usage_plan(
            "db-check-api-usage-plan",
            name="{}-db-check-api-usage-plan".format(env_name),
            description="db check api usage plan",
            throttle=aws_apigateway.ThrottleSettings(rate_limit=1000, burst_limit=1000),
            quota=aws_apigateway.QuotaSettings(limit=10000, period=aws_apigateway.Period.DAY),
        )
            
        usage_plan.add_api_stage(
            stage=self.api.deployment_stage,
        )
        usage_plan.add_api_key(api_key)

        

        