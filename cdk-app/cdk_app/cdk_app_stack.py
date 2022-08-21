from aws_cdk import (
    Stack,
    aws_secretsmanager as secretsmanager,
    SecretValue,
    CfnParameter,
    aws_lambda as _lambda,
    Duration,
    RemovalPolicy,
    aws_logs as logs,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as events_targets
)

from constructs import Construct


class CdkAppStack(Stack):
    def _create_github_access_token_secret(self):
        return secretsmanager.Secret(self, 'GitHubTokenSecret',
                                     secret_string_value=SecretValue.cfn_parameter(
                                         CfnParameter(
                                             self, 'GitHubAccessToken',
                                             no_echo=True
                                         )
                                     ))

    def _create_lambda_function_and_layer(self, secret, table):
        # Explicitly create the log group that will be used by Lambda so that it's cleaned when deleting the stack
        log_group = logs.LogGroup(self, 'FunctionGetRepositoriesTrafficLogGroup',
                                  log_group_name='/aws/lambda/getRepositoriesTraffic',
                                  removal_policy=RemovalPolicy.DESTROY,
                                  retention=logs.RetentionDays.SIX_MONTHS)

        # Create a Lambda layer that contains the requests library
        layer = _lambda.LayerVersion(self, 'RequestsLayer',
                                     layer_version_name='RequestsLayer',
                                     removal_policy=RemovalPolicy.DESTROY,
                                     code=_lambda.Code.from_asset('assets/lambda_layer'),
                                     compatible_runtimes=[_lambda.Runtime.PYTHON_3_9])

        func = _lambda.Function(self, 'FunctionGetRepositoriesTraffic',
                                function_name='getRepositoriesTraffic',
                                runtime=_lambda.Runtime.PYTHON_3_9,
                                timeout=Duration.minutes(2),
                                code=_lambda.Code.from_asset('assets/func_get_repositories_traffic'),
                                retry_attempts=0,
                                handler='index.handler',
                                layers=[layer],
                                environment={
                                    'SECRET_ARN': secret.secret_arn,
                                    'TABLE_NAME': table.table_name
                                })

        # Grant the Lambda function write permission to the DynamoDB table
        func.add_to_role_policy(
            iam.PolicyStatement(
                actions=['dynamodb:BatchWriteItem'],
                effect=iam.Effect.ALLOW,
                resources=[table.table_arn]
            )
        )

        # Add the log group as a dependency of the Lambda function
        func.node.add_dependency(log_group)

        return func

    def _create_dynamodb_table(self):
        return dynamodb.Table(self, 'TrafficTable',
                              table_name='RepoTraffic',
                              encryption=dynamodb.TableEncryption.AWS_MANAGED,
                              partition_key=dynamodb.Attribute(name="repo-name", type=dynamodb.AttributeType.STRING),
                              sort_key=dynamodb.Attribute(name="timestamp", type=dynamodb.AttributeType.STRING),
                              removal_policy=RemovalPolicy.DESTROY)

    def _create_event_bridge_rule(self, function):
        events.Rule(self, 'RuleGetRepoTraffic',
                    targets=[events_targets.LambdaFunction(function)],
                    rule_name='RuleGetRepoTraffic',
                    schedule=events.Schedule.cron(
                        hour='9',
                        minute='0'
                    ))

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create a Secrets Manager Secret to store in a secure way the GitHub access token
        secret = self._create_github_access_token_secret()

        # Create a DynamoDB table for storing repo traffic
        table = self._create_dynamodb_table()

        # Create the Lambda function that will retrieve the traffic of the GitHub repositories
        func = self._create_lambda_function_and_layer(secret, table)

        # Grant the Lambda function permission to read the Secret
        secret.grant_read(func.role)

        # Create an EventBridge rule that executes the Lambda function every day at 9 AM
        self._create_event_bridge_rule(func)
