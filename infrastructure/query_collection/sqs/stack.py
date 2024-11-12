from aws_cdk import (
    aws_sqs as sqs,
    Duration
)
from constructs import Construct


class SQS(Construct):
    def __init__(self, scope: Construct, construct_id: str, env_name: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.queries_compatibility_check_queue = sqs.Queue(
            self, "queries_compatibility_check_queue",
            queue_name="{}-queries-check-audit-log-queue".format(env_name),
            visibility_timeout=Duration.minutes(20),
            encryption=sqs.QueueEncryption.KMS_MANAGED,
            )

    @property
    def queue(self):
        return self.queries_compatibility_check_queue