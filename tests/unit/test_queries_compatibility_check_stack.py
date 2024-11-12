import aws_cdk as core
import aws_cdk.assertions as assertions
from infrastructure.queries_compatibility_check_stack import QueriesCompatibilityCheckStack


def test_sqs_queue_created():
    app = core.App()
    stack = QueriesCompatibilityCheckStack(app, "queries-compatibility-check")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties("AWS::SQS::Queue", {
        "VisibilityTimeout": 300
    })


def test_sns_topic_created():
    app = core.App()
    stack = QueriesCompatibilityCheckStack(app, "queries-compatibility-check")
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::SNS::Topic", 1)
