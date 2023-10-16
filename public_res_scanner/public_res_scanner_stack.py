from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_s3 as s3,
    RemovalPolicy,
    Duration,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as events_targets,
    CfnParameter,
)
from constructs import Construct


class PublicResScannerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        access_logs_bucket = CfnParameter(
            self,
            "accessbucket",
            type="String",
            description="Name of the bucket for public resource bucket access logs",
        )
        reports_bucket = CfnParameter(
            self,
            "reportsbucket",
            type="String",
            description="Name of the bucket for public resource reports",
        )

        aggregator = CfnParameter(
            self,
            "aggregator",
            type="String",
            description="Name of AWS Config Aggregator",
            default="aws-controltower-GuardrailsComplianceAggregator",
        )

        lifecycle_rule_bucket = s3.LifecycleRule(
            id="LifecycleRuleOnPublicResourcesData",
            abort_incomplete_multipart_upload_after=Duration.days(30),
        )

        import_i_bucket = s3.Bucket.from_bucket_name(
            self,
            "ImportAccessBucket",
            bucket_name=access_logs_bucket.value_as_string,
        )

        public_resources_data = s3.Bucket(
            self,
            "BucketForPublicResourcesData",
            bucket_name=reports_bucket.value_as_string,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[lifecycle_rule_bucket],
            public_read_access=False,
            versioned=True,
            server_access_logs_bucket=import_i_bucket,
            server_access_logs_prefix="public-res-bucket",
        )

        public_resources_data.add_to_resource_policy(
            iam.PolicyStatement(
                effect=iam.Effect.DENY,
                principals=[iam.AnyPrincipal()],
                actions=["s3:*"],
                resources=[
                    public_resources_data.bucket_arn,
                    public_resources_data.bucket_arn + "/*",
                ],
                conditions={"Bool": {"aws:SecureTransport": "false"}},
            )
        )

        lambda_scanner_role = iam.Role(
            self,
            "PublicResourcesScannerLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            inline_policies={
                "PublicResourcesScannerAccess": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                "config:SelectAggregateResourceConfig",
                            ],
                            resources=["*"],
                            effect=iam.Effect.ALLOW,
                            conditions={
                                "StringEquals": {
                                    "aws:ResourceAccount": Stack.of(self).account
                                }
                            },
                        ),
                    ]
                )
            },
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
            ],
        )

        public_scanner_lambda = _lambda.Function(
            self,
            "PublicResourcesScannerLambda",
            description="Public resources scanner",
            runtime=_lambda.Runtime.PYTHON_3_10,
            code=_lambda.Code.from_asset("public_res_scanner/compute/"),
            handler="aggregator.lambda_handler",
            architecture=_lambda.Architecture.ARM_64,
            environment={
                "BUCKET_NAME": public_resources_data.bucket_name,
                "AGGREGATOR_NAME": aggregator.value_as_string,
            },
            tracing=_lambda.Tracing.ACTIVE,
            role=lambda_scanner_role,
            timeout=Duration.seconds(600),
            memory_size=1024,
        )

        public_resources_data.grant_write(public_scanner_lambda)

        events.Rule(
            self,
            "RuleToTriggerLambdaForPublicResScan",
            description="Rule to trigger lambda for public resources scan",
            schedule=events.Schedule.expression("rate(24 hours)"),
            targets=[events_targets.LambdaFunction(public_scanner_lambda)],
            enabled=True,
        )
