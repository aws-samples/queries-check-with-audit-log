from constructs import Construct

env_name = None
vpc_id = None
private_subnet_ids = []
public_subnet_ids = []
keypair = None
secret = None

def _init_from_context(scope: Construct, name: str, default=None, array=False, array_spliter=",", formatter=str):
    value = scope.node.try_get_context(name)
    if not value:  # None or empty string
        if default is not None:
            value = default
        else:
            print(f"context variable {name} is not set!")
            exit(1)

    if type(value) != list and array:
        items = value.split(array_spliter)
        value = []
        for item in items:
            v = formatter(item.strip())
            if v is not None:
                value.append(v)
    else:
        value = formatter(value)

    return value


def init(scope: Construct):
    global env_name, vpc_id, private_subnet_ids, public_subnet_ids, keypair, secret

    env_name = _init_from_context(scope, 'env', 'dev')
    vpc_id = _init_from_context(scope, 'vpc', None)
    private_subnet_ids = _init_from_context(scope, 'private_subnets', [], array=True)
    public_subnet_ids = _init_from_context(scope, 'public_subnets', [], array=True)
    keypair = _init_from_context(scope, 'keypair', None)
    secret = _init_from_context(scope, 'secret', None)
    