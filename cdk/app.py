#!/usr/bin/env python3
import os

import aws_cdk as cdk
from dotenv import load_dotenv
from stacks.analytics_stack import AnalyticsStack

load_dotenv()

app = cdk.App()


def config(context_key: str, env_key: str) -> str | None:
    return app.node.try_get_context(context_key) or os.environ.get(env_key)


domain_name = config("domain_name", "DOMAIN_NAME")  # e.g. analytics.example.com
hosted_zone_id = config("hosted_zone_id", "HOSTED_ZONE_ID")
hosted_zone_name = config("hosted_zone_name", "HOSTED_ZONE_NAME")
certificate_arn = config("certificate_arn", "CERTIFICATE_ARN")
cognito_user_pool_id = config("cognito_user_pool_id", "COGNITO_USER_POOL_ID")
cognito_client_id = config("cognito_client_id", "COGNITO_CLIENT_ID")

AnalyticsStack(
    app,
    "ServerlessWebAnalytics",
    domain_name=domain_name,
    hosted_zone_id=hosted_zone_id,
    hosted_zone_name=hosted_zone_name,
    certificate_arn=certificate_arn,
    cognito_user_pool_id=cognito_user_pool_id,
    cognito_client_id=cognito_client_id,
    env=cdk.Environment(region="us-east-1"),
)

app.synth()
