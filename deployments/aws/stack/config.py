import typing

import pydantic


class StackSettings(pydantic.BaseSettings):
    name: str = 'zarr-proxy-deployment'
    stage: typing.Literal['dev', 'prod'] = 'prod'

    owner: typing.Optional[str] = None
    client: typing.Optional[str] = None

    env: dict = {}

    ###########################################################################
    # AWS LAMBDA
    # The following settings only apply to AWS Lambda deployment
    # more about lambda config: https://docs.aws.amazon.com/lambda/latest/dg/welcome.html
    timeout: int = 10
    memory: int = 1536

    # The maximum of concurrent executions you want to reserve for the function.
    # Default: - No specific limit - account limit.
    max_concurrent: typing.Optional[int] = None

    class Config:
        env_prefix = 'ZARR_PROXY_'
        env_file = '.env'
