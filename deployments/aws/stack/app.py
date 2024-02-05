"""Construct App."""

import pathlib
import typing

from aws_cdk import aws_apigatewayv2 as apigw
from aws_cdk import aws_apigatewayv2_integrations as apigw_integrations
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda, core
from aws_cdk import aws_logs as logs
from config import StackSettings

settings = StackSettings()


class ZarrProxyLambdaStack(core.Stack):
    def __init__(
        self,
        scope: core.Construct,
        id: str,
        memory: int = 1024,
        timeout: int = 30,
        runtime: aws_lambda.Runtime = aws_lambda.Runtime.PYTHON_3_9,
        concurrent: typing.Optional[int] = None,
        permissions: typing.Optional[list[iam.PolicyStatement]] = None,
        environment: typing.Optional[dict] = None,
        code_dir: str = "./",
        **kwargs: typing.Any,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        permissions = permissions or []
        environment = environment or {}

        lambda_function = aws_lambda.Function(
            self,
            f"{id}-lambda",
            runtime=runtime,
            code=aws_lambda.Code.from_docker_build(
                path=str(pathlib.Path(code_dir).absolute()),
                file="lambda/Dockerfile",
            ),
            handler="handler.handler",
            memory_size=memory,
            timeout=core.Duration.seconds(timeout),
            environment=environment,
            reserved_concurrent_executions=concurrent,
            log_retention=logs.RetentionDays.ONE_WEEK,
        )

        for permission in permissions:
            lambda_function.add_to_role_policy(permission)

        api = apigw.HttpApi(
            self,
            f"{id}-endpoint",
            default_integration=apigw_integrations.HttpLambdaIntegration(
                f"{id}-integration", handler=lambda_function
            ),
        )

        core.CfnOutput(self, "Endpoint", value=api.url)


app = core.App()
# Tag infrastructure
for key, value in {
    "Project": settings.name,
    "Stack": settings.stage,
    "Owner": settings.owner,
    "Client": settings.client,
}.items():
    if value:
        core.Tags.of(app).add(key, value)


perms = []
lambda_stackname = f"{settings.name}-lambda-{settings.stage}"
ZarrProxyLambdaStack(
    app,
    lambda_stackname,
    memory=settings.memory,
    timeout=settings.timeout,
    concurrent=settings.max_concurrent,
    permissions=perms,
    environment=settings.env,
)

app.synth()
