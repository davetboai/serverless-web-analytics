import json
import os
from datetime import datetime, timedelta, timezone
from collections import Counter

import boto3
from boto3.dynamodb.conditions import Key

TABLE_NAME = os.environ["TABLE_NAME"]
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "")
ddb = boto3.resource("dynamodb")
table = ddb.Table(TABLE_NAME)

CORS_HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Cache-Control": "no-store",
}


def handler(event, context):
    method = event.get("requestContext", {}).get("http", {}).get("method", "")
    if method == "OPTIONS":
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}

    # Auth
    if DASHBOARD_PASSWORD:
        auth = event.get("headers", {}).get("authorization", "")
        if auth != f"Bearer {DASHBOARD_PASSWORD}":
            return _resp(401, {"error": "unauthorized"})

    path = event.get("rawPath", "")
    params = event.get("queryStringParameters") or {}

    if path.endswith("/sites"):
        return _list_sites()

    return _get_stats(params)


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

    # Fetch all events in date range
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
    daily_views = Counter()
    daily_visitors = {}
    paths = Counter()
    referrers = Counter()
    countries = Counter()
    devices = Counter()
    all_visitor_hashes = set()

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

    # Build date series
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
