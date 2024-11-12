try:
    from aws_cdk import core as cdk
except ImportError:
    import aws_cdk as cdk

from aws_cdk import (
    aws_ec2,
    aws_iam,
    aws_dynamodb,
    aws_sqs,
    aws_s3,
    aws_secretsmanager,
)
from constructs import Construct


class LaunchTemplate(Construct):
    def __init__(self, scope: Construct, construct_id: str, env_name: str, bucket: aws_s3.Bucket, 
                 dynamodb_tables, secret: aws_secretsmanager.Secret, 
                 region: str, sqs: aws_sqs.Queue, key_name: str, sg, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        source_code = 's3://{}/code/'.format(bucket.bucket_name)

        # create agent role
        role_name = "db_check_agent_role_{}".format(env_name)
        self.agent_role = aws_iam.Role(self, "db_check_agent_role",
                                  assumed_by=aws_iam.ServicePrincipal("ec2.amazonaws.com"),
                                  role_name=role_name)
        self.agent_role.add_managed_policy(aws_iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore"))

        bucket.grant_read_write(self.agent_role)
        sqs.grant_consume_messages(self.agent_role)
        dynamodb_tables.subtask_table.grant_read_write_data(self.agent_role)
        dynamodb_tables.sql_example_table.grant_read_write_data(self.agent_role)
        secret.grant_read(self.agent_role)

        # user data
        user_data = aws_ec2.UserData.for_linux()
        user_data.add_commands('echo "user-data script start>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"')
        user_data.add_commands('cd /home/ec2-user/')
        user_data.add_commands('sudo -u ec2-user mkdir agent')
        user_data.add_commands('cd agent')
        user_data.add_commands('sudo -u ec2-user aws s3 sync {} .'.format(source_code))
        user_data.add_commands('echo "region={}" >> /home/ec2-user/agent/config.conf'.format(region))
        user_data.add_commands('echo "queue_url={}" >> /home/ec2-user/agent/config.conf'.format(sqs.queue_url))
        user_data.add_commands('echo "subtask_dynamodb_name={}" >> /home/ec2-user/agent/config.conf'.format(dynamodb_tables.subtask_table.table_name))
        user_data.add_commands('echo "sql_sample_dynamodb_name={}" >> /home/ec2-user/agent/config.conf'.format(dynamodb_tables.sql_example_table.table_name))
        user_data.add_commands('echo "secret_name={}" >> /home/ec2-user/agent/config.conf'.format(secret.secret_name))
        user_data.add_commands('sh setup.sh')
        user_data.add_commands('echo "user-data script end>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"')

        key_pair = aws_ec2.KeyPair.from_key_pair_name(self, "KeyPair", key_name)

        # launch template
        self.agent_launch_template = aws_ec2.LaunchTemplate(
            self, "db_check_launch_template",
            machine_image=aws_ec2.MachineImage.latest_amazon_linux2023(cpu_type=aws_ec2.AmazonLinuxCpuType.ARM_64),
            launch_template_name="db_check_launch_template_{}".format(env_name),
            instance_type=aws_ec2.InstanceType.of(aws_ec2.InstanceClass.C6GN, aws_ec2.InstanceSize.XLARGE2),
            key_pair=key_pair,
            associate_public_ip_address = True,
            security_group=sg,
            role=self.agent_role,
            require_imdsv2=True,
            user_data=user_data,
            block_devices=[aws_ec2.BlockDevice(device_name="/dev/xvda", volume=aws_ec2.BlockDeviceVolume.ebs(100, encrypted=True))],
            )
        
        @property
        def asg(self):
            return self.agent_launch_template