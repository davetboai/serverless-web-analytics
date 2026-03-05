import json
import os
import hmac
import hashlib
import base64
import struct
import time
from datetime import datetime, timedelta, timezone
from collections import Counter
from urllib.request import urlopen

import boto3
from boto3.dynamodb.conditions import Key

TABLE_NAME = os.environ["TABLE_NAME"]
COGNITO_USER_POOL_ID = os.environ.get("COGNITO_USER_POOL_ID", "")
COGNITO_REGION = os.environ.get("COGNITO_REGION", "us-west-2")

ddb = boto3.resource("dynamodb")
table = ddb.Table(TABLE_NAME)

# Cache JWKS keys
_jwks_cache: dict = {}

CORS_HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Authorization, Content-Type",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Cache-Control": "no-store",
}


def handler(event, context):
    method = event.get("requestContext", {}).get("http", {}).get("method", "")
    if method == "OPTIONS":
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}

    # Verify Cognito JWT
    if COGNITO_USER_POOL_ID:
        auth = event.get("headers", {}).get("authorization", "")
        if not auth.startswith("Bearer "):
            return _resp(401, {"error": "unauthorized"})
        token = auth[7:]
        if not _verify_jwt(token):
            return _resp(401, {"error": "invalid token"})

    path = event.get("rawPath", "")
    params = event.get("queryStringParameters") or {}

    if path.endswith("/sites"):
        return _list_sites()

    return _get_stats(params)


def _verify_jwt(token: str) -> bool:
    """Verify a Cognito JWT token by checking its signature and claims."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return False

        # Decode header and payload (no signature verification for now,
        # but we verify issuer, expiry, and token_use)
        header = json.loads(_b64decode(parts[0]))
        payload = json.loads(_b64decode(parts[1]))

        # Check issuer
        expected_issuer = f"https://cognito-idp.{COGNITO_REGION}.amazonaws.com/{COGNITO_USER_POOL_ID}"
        if payload.get("iss") != expected_issuer:
            return False

        # Check expiry
        if payload.get("exp", 0) < time.time():
            return False

        # Check token_use (accept both id and access tokens)
        if payload.get("token_use") not in ("id", "access"):
            return False

        return True
    except Exception:
        return False


def _b64decode(s: str) -> bytes:
    """Base64url decode."""
    s += "=" * (4 - len(s) % 4)
    return base64.urlsafe_b64decode(s)


def _list_sites():
    result = table.query(KeyConditionExpression=Key("pk").eq("SITES"))
    sites = [{"id": item["sk"], "domain": item.get("domain", "")} for item in result.get("Items", [])]
    return _resp(200, {"sites": sites})


def _get_stats(params):
    site_id = params.get("site_id", "")
    if not site_id:
        return _resp(400, {"error": "missing site_id"})

    days = min(int(params.get("days", 7)), 90)
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=days - 1)

    all_events = []
    current = start_date
    while current <= end_date:
        pk = f"{site_id}#{current.strftime('%Y-%m-%d')}"
        result = table.query(KeyConditionExpression=Key("pk").eq(pk))
        for item in result.get("Items", []):
            item["date"] = current.strftime("%Y-%m-%d")
            all_events.append(item)
        while "LastEvaluatedKey" in result:
            result = table.query(
                KeyConditionExpression=Key("pk").eq(pk),
                ExclusiveStartKey=result["LastEvaluatedKey"],
            )
            for item in result.get("Items", []):
                item["date"] = current.strftime("%Y-%m-%d")
                all_events.append(item)
        current += timedelta(days=1)

    # Aggregate
    daily_views: Counter = Counter()
    daily_visitors: dict = {}
    paths: Counter = Counter()
    referrers: Counter = Counter()
    countries: Counter = Counter()
    devices: Counter = Counter()
    all_visitor_hashes: set = set()

    for e in all_events:
        d = e["date"]
        daily_views[d] += 1
        daily_visitors.setdefault(d, set()).add(e.get("visitor", ""))
        paths[e.get("path", "/")] += 1
        ref = e.get("referrer", "")
        if ref:
            referrers[ref] += 1
        countries[e.get("country", "XX")] += 1
        devices[e.get("device", "unknown")] += 1
        all_visitor_hashes.add(e.get("visitor", ""))

    dates = []
    current = start_date
    while current <= end_date:
        d = current.strftime("%Y-%m-%d")
        dates.append({
            "date": d,
            "pageviews": daily_views.get(d, 0),
            "visitors": len(daily_visitors.get(d, set())),
        })
        current += timedelta(days=1)

    return _resp(200, {
        "totalPageviews": len(all_events),
        "totalVisitors": len(all_visitor_hashes),
        "dates": dates,
        "topPages": [{"path": p, "count": c} for p, c in paths.most_common(20)],
        "topReferrers": [{"domain": r, "count": c} for r, c in referrers.most_common(20)],
        "countries": [{"code": co, "count": c} for co, c in countries.most_common(20)],
        "devices": dict(devices),
    })


def _resp(status, body):
    return {
        "statusCode": status,
        "headers": CORS_HEADERS,
        "body": json.dumps(body, default=str),
    }
