from aws_cdk import (
    aws_s3,
    aws_s3_deployment as s3deploy,
    RemovalPolicy,
    Duration,
    CfnOutput,
    aws_iam as iam,
    )
from constructs import Construct


class Bucket(Construct):
    def __init__(self, scope: Construct, construct_id: str, account: str, region: str, env_name: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        # provision a s3 bucket
        self.bucket = aws_s3.Bucket(
            self, "db-check-bucket",
            bucket_name="{}-db-check-bucket-{}-{}".format(env_name, account, region),
            encryption=aws_s3.BucketEncryption.S3_MANAGED,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            lifecycle_rules=[
                aws_s3.LifecycleRule(
                    enabled=True,
                    expiration=Duration.days(3650),
                )
            ]
            )

        # Upload agent code and file to S3 bucket
        s3deploy.BucketDeployment(self, "DeployFiles",
                                  sources=[s3deploy.Source.asset("agent/")],
                                  destination_bucket=self.bucket,
                                  destination_key_prefix="code"
                                  )
        
        # Add bucket policy

        get_bucket_acl_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=['s3:GetBucketAcl'],
            resources=[
                self.bucket.bucket_arn
            ],
            principals=[iam.ServicePrincipal(f'logs.{region}.amazonaws.com')],
            conditions={
                'StringEquals': {
                    'aws:SourceAccount': f'{account}'
                },
                'ArnLike': {
                    'aws:SourceArn': f'arn:aws:logs:{region}:{account}:log-group:*'
                }
            }
        )

        put_object_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=['s3:PutObject'],
            resources=[
                self.bucket.arn_for_objects('*')
            ],
            principals=[iam.ServicePrincipal(f'logs.{region}.amazonaws.com')],
            conditions={
                'StringEquals': {
                    's3:x-amz-acl': 'bucket-owner-full-control',
                    'aws:SourceAccount': account
                },
                'ArnLike': {
                    'aws:SourceArn': f'arn:aws:logs:{region}:{account}:log-group:*'
                }
            }
        )

        self.bucket.add_to_resource_policy(get_bucket_acl_policy)
        self.bucket.add_to_resource_policy(put_object_policy)

        CfnOutput(
            self,
            "S3BucketName",
            value=self.bucket.bucket_name,
            description="The name of the S3 bucket for audit log and reports.",
        )