try:
    from aws_cdk import core as cdk
except ImportError:
    import aws_cdk as cdk

from aws_cdk import (
    aws_ec2,
    aws_iam,
    aws_autoscaling,
)
from constructs import Construct

class ASG(Construct):
    def __init__(self, scope: Construct, construct_id: str, env_name: str, 
                 launch_template, vpc, public_subnets, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.db_check_asg = aws_autoscaling.AutoScalingGroup(
            self, "db_check_asg",
            launch_template=launch_template,
            vpc=vpc,
            vpc_subnets=aws_ec2.SubnetSelection(subnets=public_subnets),
            min_capacity=1,
            max_capacity=1,
            auto_scaling_group_name="{}_db_check_asg".format(env_name),
            desired_capacity=1,
            )

    @property
    def asg(self):
        return self.db_check_asg