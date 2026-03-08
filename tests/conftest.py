import os
import json
import importlib.util
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

SRC = Path(__file__).parent.parent / "src"


def _load_module(name, path):
    """Load a Python module from a specific file path, avoiding sys.path collisions."""
    spec = importlib.util.spec_from_file_location(name, str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def load_collector():
    return _load_module("collector_index", SRC / "collector" / "index.py")


def load_query():
    return _load_module("query_index", SRC / "query" / "index.py")


@pytest.fixture
def aws_env(monkeypatch):
    """Set minimal AWS env vars for moto."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("TABLE_NAME", "test-events")


@pytest.fixture
def ddb_table(aws_env):
    """Create a mock DynamoDB table."""
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        client.create_table(
            TableName="test-events",
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        yield boto3.resource("dynamodb", region_name="us-east-1").Table("test-events")


def make_event(body, headers=None, method="POST", path="/api/collect"):
    """Build a Lambda Function URL / API Gateway v2 event."""
    return {
        "requestContext": {"http": {"method": method}},
        "rawPath": path,
        "headers": headers or {
            "user-agent": "Mozilla/5.0 Test",
            "x-forwarded-for": "1.2.3.4",
            "cloudfront-viewer-country": "US",
        },
        "body": json.dumps(body) if isinstance(body, dict) else body,
    }
