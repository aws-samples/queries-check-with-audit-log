#!/usr/bin/env python3
import os
import aws_cdk as cdk

from infrastructure.queries_compatibility_check_stack import QueriesCompatibilityCheckStack
from infrastructure import stack_input
from cdk_nag import AwsSolutionsChecks, NagSuppressions, SuppressionIgnoreErrors
from aws_cdk import Aspects

app = cdk.App()
stack_input.init(app)
QueriesCompatibilityCheckStack(app, "QueriesCheckStack",
                               env=cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), 
                                                   region=os.getenv('CDK_DEFAULT_REGION')),)

Aspects.of(app).add(AwsSolutionsChecks())



app.synth()
