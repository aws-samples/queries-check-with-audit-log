from constructs import Construct
from infrastructure.shared_infrastructure.bucket.stack import Bucket
from infrastructure.shared_infrastructure.dynamodb.stack import DynamoDBTables
from infrastructure.shared_infrastructure.api_gateway.stack import API


class SharedInfrastructureConstruct(Construct):
    def __init__(self, scope: Construct, construct_id: str, params: dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.db_check_bucket = Bucket(self, "bucket", account=params['account'], region=params['region'],
                                      env_name=params['env_name'])
        self.dynamodb = DynamoDBTables(self, "ddb", params=params)

        self.api = API(self, "api", env_name=params['env_name'])

    @property
    def s3_bucket(self):
        return self.db_check_bucket
