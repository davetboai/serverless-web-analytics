#!/usr/bin/env python3
import aws_cdk as cdk
from stacks.analytics_stack import AnalyticsStack

app = cdk.App()

domain_name = app.node.try_get_context("domain_name")  # e.g. analytics.davetbo.ai
hosted_zone_id = app.node.try_get_context("hosted_zone_id")
hosted_zone_name = app.node.try_get_context("hosted_zone_name")
certificate_arn = app.node.try_get_context("certificate_arn")
cognito_user_pool_id = app.node.try_get_context("cognito_user_pool_id")
cognito_client_id = app.node.try_get_context("cognito_client_id")

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
