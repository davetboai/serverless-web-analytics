from pathlib import Path

import aws_cdk as cdk
from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_dynamodb as dynamodb,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
    aws_certificatemanager as acm,
    aws_route53 as route53,
    aws_route53_targets as targets,
    CfnOutput,
)
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
        dashboard_password: str,
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

        # --- Collector Lambda ---
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

        # --- Query Lambda ---
        query_fn = _lambda.Function(
            self,
            "Query",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="index.handler",
            code=_lambda.Code.from_asset(str(SRC / "query")),
            environment={
                "TABLE_NAME": table.table_name,
                "DASHBOARD_PASSWORD": dashboard_password,
            },
            timeout=Duration.seconds(30),
            memory_size=256,
        )
        table.grant_read_data(query_fn)
        query_url = query_fn.add_function_url(
            auth_type=_lambda.FunctionUrlAuthType.NONE,
        )

        # --- S3 bucket for dashboard + tracker script ---
        site_bucket = s3.Bucket(
            self,
            "DashboardBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # --- CloudFront Function for dashboard basic auth ---
        auth_fn = cloudfront.Function(
            self,
            "DashboardAuth",
            code=cloudfront.FunctionCode.from_inline(
                f"""
function handler(event) {{
    var request = event.request;
    var uri = request.uri;

    // Allow tracker script and collect API without auth
    if (uri === '/script.js' || uri.startsWith('/api/')) {{
        return request;
    }}

    var password = '{dashboard_password}';
    if (!password) return request;

    var headers = request.headers;
    var auth = headers.authorization;
    var expected = 'Basic ' + '{{}}'.replace('{{}}',
        // base64 of admin:<password>
        _btoa('admin:' + password)
    );

    if (!auth || auth.value !== expected) {{
        return {{
            statusCode: 401,
            statusDescription: 'Unauthorized',
            headers: {{
                'www-authenticate': {{ value: 'Basic realm="Analytics"' }}
            }}
        }};
    }}
    return request;
}}

function _btoa(s) {{
    var chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
    var out = '';
    for (var i = 0; i < s.length; i += 3) {{
        var b = (s.charCodeAt(i) << 16)
              | ((i+1 < s.length ? s.charCodeAt(i+1) : 0) << 8)
              | (i+2 < s.length ? s.charCodeAt(i+2) : 0);
        for (var j = 0; j < 4; j++) {{
            if (i * 8 + j * 6 > s.length * 8) out += '=';
            else out += chars.charAt((b >>> (18 - j*6)) & 63);
        }}
    }}
    return out;
}}
"""
            ),
            runtime=cloudfront.FunctionRuntime.JS_2_0,
        )

        # --- CloudFront distribution ---
        certificate = acm.Certificate.from_certificate_arn(
            self, "Cert", certificate_arn
        )

        # OAC for S3
        oac = cloudfront.S3OriginAccessControl(self, "OAC")
        s3_origin = origins.S3BucketOrigin.with_origin_access_control(
            site_bucket, origin_access_control=oac
        )

        # Lambda Function URL origins
        collector_origin = origins.FunctionUrlOrigin(collector_url)
        query_origin = origins.FunctionUrlOrigin(query_url)

        distribution = cloudfront.Distribution(
            self,
            "Distribution",
            default_behavior=cloudfront.BehaviorOptions(
                origin=s3_origin,
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                function_associations=[
                    cloudfront.FunctionAssociation(
                        function=auth_fn,
                        event_type=cloudfront.FunctionEventType.VIEWER_REQUEST,
                    )
                ],
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
                    allowed_methods=cloudfront.AllowedMethods.ALLOW_ALL,
                ),
                "/api/sites": cloudfront.BehaviorOptions(
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
        )

        # --- Deploy dashboard + tracker to S3 ---
        s3deploy.BucketDeployment(
            self,
            "DeployDashboard",
            sources=[
                s3deploy.Source.asset(str(SRC / "dashboard")),
                s3deploy.Source.asset(str(SRC / "tracker")),
            ],
            destination_bucket=site_bucket,
            distribution=distribution,
        )

        # --- Route53 ---
        zone = route53.HostedZone.from_hosted_zone_attributes(
            self, "Zone", hosted_zone_id=hosted_zone_id, zone_name=hosted_zone_name
        )
        route53.ARecord(
            self,
            "ARecord",
            zone=zone,
            record_name=domain_name,
            target=route53.RecordTarget.from_alias(targets.CloudFrontTarget(distribution)),
        )
        route53.AaaaRecord(
            self,
            "AAAARecord",
            zone=zone,
            record_name=domain_name,
            target=route53.RecordTarget.from_alias(targets.CloudFrontTarget(distribution)),
        )

        # --- Outputs ---
        CfnOutput(self, "DashboardUrl", value=f"https://{domain_name}")
        CfnOutput(self, "TrackingScript", value=f'<script defer src="https://{domain_name}/script.js" data-website-id="YOUR_SITE_ID"></script>')
        CfnOutput(self, "TableName", value=table.table_name)
