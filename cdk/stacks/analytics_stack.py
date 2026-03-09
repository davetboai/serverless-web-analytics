from pathlib import Path

import aws_cdk as cdk
from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    BundlingOptions,
    DockerImage,
    aws_dynamodb as dynamodb,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
    aws_certificatemanager as acm,
    aws_route53 as route53,
    aws_route53_targets as targets,
    aws_apigatewayv2 as apigwv2,
    CfnOutput,
)
from aws_cdk.aws_apigatewayv2_integrations import HttpLambdaIntegration
from aws_cdk.aws_apigatewayv2_authorizers import HttpJwtAuthorizer
from constructs import Construct

SRC = Path(__file__).resolve().parent.parent.parent / "src"


class AnalyticsStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        domain_name: str,
        hosted_zone_id: str,
        hosted_zone_name: str,
        certificate_arn: str,
        cognito_user_pool_id: str,
        cognito_client_id: str,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        # --- DynamoDB ---
        table = dynamodb.Table(
            self,
            "Events",
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
            time_to_live_attribute="ttl",
        )

        # --- Collector Lambda (public — no auth needed for tracking beacons) ---
        collector_fn = _lambda.Function(
            self,
            "Collector",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="index.handler",
            code=_lambda.Code.from_asset(str(SRC / "collector")),
            environment={"TABLE_NAME": table.table_name},
            timeout=Duration.seconds(10),
            memory_size=128,
        )
        table.grant_write_data(collector_fn)
        collector_url = collector_fn.add_function_url(
            auth_type=_lambda.FunctionUrlAuthType.NONE,
        )

        # --- Query Lambda (auth handled by API Gateway JWT authorizer) ---
        query_fn = _lambda.Function(
            self,
            "Query",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="index.handler",
            code=_lambda.Code.from_asset(str(SRC / "query")),
            environment={"TABLE_NAME": table.table_name},
            timeout=Duration.seconds(30),
            memory_size=256,
        )
        table.grant_read_write_data(query_fn)

        # --- HTTP API Gateway with Cognito JWT authorizer ---
        region = kwargs.get("env", cdk.Environment()).region or "us-east-1"
        # Cognito pool may be in a different region — extract from pool ID (e.g. us-west-2_abc)
        cognito_region = cognito_user_pool_id.rsplit("_", 1)[0] if "_" in cognito_user_pool_id else region
        cognito_issuer = f"https://cognito-idp.{cognito_region}.amazonaws.com/{cognito_user_pool_id}"

        jwt_authorizer = HttpJwtAuthorizer(
            "CognitoAuthorizer",
            jwt_issuer=cognito_issuer,
            jwt_audience=[cognito_client_id],
        )

        query_integration = HttpLambdaIntegration("QueryIntegration", handler=query_fn)

        api = apigwv2.HttpApi(
            self,
            "QueryApi",
            cors_preflight=apigwv2.CorsPreflightOptions(
                allow_origins=["*"],
                allow_methods=[
                    apigwv2.CorsHttpMethod.GET,
                    apigwv2.CorsHttpMethod.POST,
                    apigwv2.CorsHttpMethod.PUT,
                    apigwv2.CorsHttpMethod.DELETE,
                    apigwv2.CorsHttpMethod.OPTIONS,
                ],
                allow_headers=["Authorization", "Content-Type"],
            ),
            default_authorizer=jwt_authorizer,
        )
        api.add_routes(
            path="/api/query",
            methods=[apigwv2.HttpMethod.GET],
            integration=query_integration,
        )
        api.add_routes(
            path="/api/sites",
            methods=[
                apigwv2.HttpMethod.GET,
                apigwv2.HttpMethod.POST,
                apigwv2.HttpMethod.PUT,
                apigwv2.HttpMethod.DELETE,
            ],
            integration=query_integration,
        )
        api.add_routes(
            path="/api/live",
            methods=[apigwv2.HttpMethod.GET],
            integration=query_integration,
        )
        api.add_routes(
            path="/api/events",
            methods=[apigwv2.HttpMethod.GET],
            integration=query_integration,
        )
        api.add_routes(
            path="/api/recent",
            methods=[apigwv2.HttpMethod.GET],
            integration=query_integration,
        )
        api.add_routes(
            path="/api/goals",
            methods=[
                apigwv2.HttpMethod.GET,
                apigwv2.HttpMethod.POST,
                apigwv2.HttpMethod.DELETE,
            ],
            integration=query_integration,
        )

        # --- S3 bucket for dashboard + tracker script ---
        site_bucket = s3.Bucket(
            self,
            "DashboardBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # --- Build dashboard React app via Docker ---
        dashboard_asset = s3deploy.Source.asset(
            str(SRC / "dashboard"),
            bundling=BundlingOptions(
                image=DockerImage.from_registry("node:20-slim"),
                command=[
                    "bash", "-c",
                    "cp -r /asset-input/. /tmp/build && cd /tmp/build"
                    " && npm install"
                    " && npx vite build"
                    " && cp -r /tmp/build/dist/* /asset-output/",
                ],
                environment={
                    "VITE_COGNITO_USER_POOL_ID": cognito_user_pool_id,
                    "VITE_COGNITO_CLIENT_ID": cognito_client_id,
                },
                user="root",
            ),
        )

        # --- CloudFront distribution ---
        certificate = acm.Certificate.from_certificate_arn(
            self, "Cert", certificate_arn
        )
        oac = cloudfront.S3OriginAccessControl(self, "OAC")
        s3_origin = origins.S3BucketOrigin.with_origin_access_control(
            site_bucket, origin_access_control=oac
        )
        collector_origin = origins.FunctionUrlOrigin(collector_url)

        # API Gateway origin for authenticated query endpoints
        api_domain = f"{api.api_id}.execute-api.{region}.amazonaws.com"
        query_origin = origins.HttpOrigin(api_domain)

        distribution = cloudfront.Distribution(
            self,
            "Distribution",
            default_behavior=cloudfront.BehaviorOptions(
                origin=s3_origin,
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
            ),
            additional_behaviors={
                "/script.js": cloudfront.BehaviorOptions(
                    origin=s3_origin,
                    viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                    cache_policy=cloudfront.CachePolicy.CACHING_OPTIMIZED,
                ),
                "/api/collect": cloudfront.BehaviorOptions(
                    origin=collector_origin,
                    viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                    cache_policy=cloudfront.CachePolicy.CACHING_DISABLED,
                    origin_request_policy=cloudfront.OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
                    allowed_methods=cloudfront.AllowedMethods.ALLOW_ALL,
                ),
                "/api/query": cloudfront.BehaviorOptions(
                    origin=query_origin,
                    viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                    cache_policy=cloudfront.CachePolicy.CACHING_DISABLED,
                    origin_request_policy=cloudfront.OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
                    allowed_methods=cloudfront.AllowedMethods.ALLOW_GET_HEAD_OPTIONS,
                ),
                "/api/sites": cloudfront.BehaviorOptions(
                    origin=query_origin,
                    viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                    cache_policy=cloudfront.CachePolicy.CACHING_DISABLED,
                    origin_request_policy=cloudfront.OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
                    allowed_methods=cloudfront.AllowedMethods.ALLOW_ALL,
                ),
                "/api/live": cloudfront.BehaviorOptions(
                    origin=query_origin,
                    viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                    cache_policy=cloudfront.CachePolicy.CACHING_DISABLED,
                    origin_request_policy=cloudfront.OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
                    allowed_methods=cloudfront.AllowedMethods.ALLOW_GET_HEAD_OPTIONS,
                ),
                "/api/events": cloudfront.BehaviorOptions(
                    origin=query_origin,
                    viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                    cache_policy=cloudfront.CachePolicy.CACHING_DISABLED,
                    origin_request_policy=cloudfront.OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
                    allowed_methods=cloudfront.AllowedMethods.ALLOW_GET_HEAD_OPTIONS,
                ),
                "/api/recent": cloudfront.BehaviorOptions(
                    origin=query_origin,
                    viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                    cache_policy=cloudfront.CachePolicy.CACHING_DISABLED,
                    origin_request_policy=cloudfront.OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
                    allowed_methods=cloudfront.AllowedMethods.ALLOW_GET_HEAD_OPTIONS,
                ),
                "/api/goals": cloudfront.BehaviorOptions(
                    origin=query_origin,
                    viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                    cache_policy=cloudfront.CachePolicy.CACHING_DISABLED,
                    origin_request_policy=cloudfront.OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
                    allowed_methods=cloudfront.AllowedMethods.ALLOW_ALL,
                ),
            },
            domain_names=[domain_name],
            certificate=certificate,
            default_root_object="index.html",
            error_responses=[
                cloudfront.ErrorResponse(
                    http_status=403,
                    response_http_status=200,
                    response_page_path="/index.html",
                    ttl=Duration.seconds(0),
                ),
                cloudfront.ErrorResponse(
                    http_status=404,
                    response_http_status=200,
                    response_page_path="/index.html",
                    ttl=Duration.seconds(0),
                ),
            ],
        )

        # --- Deploy dashboard build + tracker script to S3 ---
        s3deploy.BucketDeployment(
            self,
            "DeployDashboard",
            sources=[dashboard_asset],
            destination_bucket=site_bucket,
            distribution=distribution,
        )
        s3deploy.BucketDeployment(
            self,
            "DeployTracker",
            sources=[s3deploy.Source.asset(str(SRC / "tracker"))],
            destination_bucket=site_bucket,
            distribution=distribution,
        )

        # --- Route53 ---
        zone = route53.HostedZone.from_hosted_zone_attributes(
            self, "Zone", hosted_zone_id=hosted_zone_id, zone_name=hosted_zone_name
        )
        route53.ARecord(
            self, "ARecord", zone=zone, record_name=domain_name,
            target=route53.RecordTarget.from_alias(targets.CloudFrontTarget(distribution)),
        )
        route53.AaaaRecord(
            self, "AAAARecord", zone=zone, record_name=domain_name,
            target=route53.RecordTarget.from_alias(targets.CloudFrontTarget(distribution)),
        )

        # --- Outputs ---
        CfnOutput(self, "DashboardUrl", value=f"https://{domain_name}")
        CfnOutput(
            self,
            "TrackingScript",
            value=f'<script defer src="https://{domain_name}/script.js" data-website-id="YOUR_SITE_ID"></script>',
        )
        CfnOutput(self, "TableName", value=table.table_name)
