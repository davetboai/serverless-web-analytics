import json
import os
from datetime import datetime, timedelta, timezone
from collections import Counter

import boto3
from boto3.dynamodb.conditions import Key

TABLE_NAME = os.environ["TABLE_NAME"]

ddb = boto3.resource("dynamodb")
table = ddb.Table(TABLE_NAME)

CORS_HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Authorization, Content-Type",
    "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
    "Cache-Control": "no-store",
}


def handler(event, context):
    # Auth is handled by API Gateway JWT authorizer — no verification needed here
    path = event.get("rawPath", "")
    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
    params = event.get("queryStringParameters") or {}

    if path.endswith("/sites"):
        if method == "GET":
            return _list_sites()
        body = {}
        try:
            body = json.loads(event.get("body", "{}") or "{}")
        except (json.JSONDecodeError, TypeError):
            pass
        if method == "POST":
            return _create_site(body)
        if method == "PUT":
            return _rename_site(body)
        if method == "DELETE":
            return _delete_site(body)

    if path.endswith("/live"):
        return _get_live(params)

    return _get_stats(params)


def _list_sites():
    result = table.query(KeyConditionExpression=Key("pk").eq("SITES"))
    sites = [{"id": item["sk"], "domain": item.get("domain", "")} for item in result.get("Items", [])]
    return _resp(200, {"sites": sites})


def _create_site(body):
    site_id = (body.get("id") or "").strip()
    if not site_id or len(site_id) > 64:
        return _resp(400, {"error": "invalid site id"})
    domain = (body.get("domain") or site_id)[:256]
    table.put_item(
        Item={"pk": "SITES", "sk": site_id, "domain": domain},
        ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
    )
    return _resp(201, {"id": site_id, "domain": domain})


def _rename_site(body):
    site_id = (body.get("id") or "").strip()
    domain = (body.get("domain") or "").strip()
    if not site_id or not domain:
        return _resp(400, {"error": "id and domain required"})
    table.update_item(
        Key={"pk": "SITES", "sk": site_id},
        UpdateExpression="SET #d = :d",
        ExpressionAttributeNames={"#d": "domain"},
        ExpressionAttributeValues={":d": domain[:256]},
    )
    return _resp(200, {"id": site_id, "domain": domain})


def _delete_site(body):
    site_id = (body.get("id") or "").strip()
    if not site_id:
        return _resp(400, {"error": "id required"})
    table.delete_item(Key={"pk": "SITES", "sk": site_id})
    return _resp(200, {"deleted": site_id})


def _get_stats(params):
    site_id = params.get("site_id", "")
    if not site_id:
        return _resp(400, {"error": "missing site_id"})

    days = min(int(params.get("days", 7)), 90)
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=days - 1)

    # Read pre-aggregated daily summaries (1 item per day)
    summaries = table.query(
        KeyConditionExpression=(
            Key("pk").eq(f"SUMMARY#{site_id}")
            & Key("sk").between(start_date.isoformat(), end_date.isoformat())
        ),
    ).get("Items", [])
    summary_by_date = {s["sk"]: s for s in summaries}

    # Also read session duration records for avg duration
    all_sessions = []
    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        session_pk = f"SESSION#{site_id}#{date_str}"
        all_sessions.extend(_query_all(session_pk))
        current += timedelta(days=1)

    # Aggregate from summaries
    total_pageviews = 0
    all_visitors: set = set()
    all_session_ids: set = set()
    paths: Counter = Counter()
    referrers: Counter = Counter()
    countries: Counter = Counter()
    devices: Counter = Counter()
    dates = []

    current = start_date
    while current <= end_date:
        d = current.strftime("%Y-%m-%d")
        summary = summary_by_date.get(d, {})
        day_views = int(summary.get("pageviews", 0))
        day_visitors = summary.get("visitors", set())
        if isinstance(day_visitors, set):
            all_visitors.update(day_visitors)

        day_sessions = summary.get("sessions", set())
        if isinstance(day_sessions, set):
            all_session_ids.update(day_sessions)

        total_pageviews += day_views

        for p, c in (summary.get("paths") or {}).items():
            paths[p] += int(c)
        for r, c in (summary.get("referrers") or {}).items():
            referrers[r] += int(c)
        for co, c in (summary.get("countries") or {}).items():
            countries[co] += int(c)
        for dv, c in (summary.get("devices") or {}).items():
            devices[dv] += int(c)

        dates.append({
            "date": d,
            "pageviews": day_views,
            "visitors": len(day_visitors) if isinstance(day_visitors, set) else 0,
        })
        current += timedelta(days=1)

    # Session metrics
    total_sessions = max(len(all_session_ids), 1)
    # Bounce rate not available from summaries alone (would need per-session page counts)
    # Use session duration: sessions with 0 duration are likely bounces
    bounce_sessions = sum(1 for s in all_sessions if int(s.get("duration", 0)) == 0)
    bounce_rate = round(bounce_sessions / max(len(all_sessions), 1) * 100, 1) if all_sessions else 0

    total_duration = sum(int(s.get("duration", 0)) for s in all_sessions)
    avg_duration = round(total_duration / len(all_sessions)) if all_sessions else 0

    return _resp(200, {
        "totalPageviews": total_pageviews,
        "totalVisitors": len(all_visitors),
        "totalSessions": total_sessions,
        "bounceRate": bounce_rate,
        "avgDuration": avg_duration,
        "dates": dates,
        "topPages": [{"path": p, "count": c} for p, c in paths.most_common(20)],
        "topReferrers": [{"domain": r, "count": c} for r, c in referrers.most_common(20)],
        "countries": [{"code": co, "count": c} for co, c in countries.most_common(20)],
        "devices": dict(devices),
    })


def _get_live(params):
    site_id = params.get("site_id", "")
    if not site_id:
        return _resp(400, {"error": "missing site_id"})
    # DynamoDB TTL deletes are async (up to 48h), so filter by current time
    now_ts = int(datetime.now(timezone.utc).timestamp())
    items = _query_all(f"LIVE#{site_id}")
    live_count = sum(1 for item in items if int(item.get("ttl", 0)) > now_ts)
    return _resp(200, {"liveVisitors": live_count})


def _query_all(pk):
    """Query all items for a partition key, handling pagination."""
    items = []
    result = table.query(KeyConditionExpression=Key("pk").eq(pk))
    items.extend(result.get("Items", []))
    while "LastEvaluatedKey" in result:
        result = table.query(
            KeyConditionExpression=Key("pk").eq(pk),
            ExclusiveStartKey=result["LastEvaluatedKey"],
        )
        items.extend(result.get("Items", []))
    return items


def _resp(status, body):
    return {
        "statusCode": status,
        "headers": CORS_HEADERS,
        "body": json.dumps(body, default=str),
    }
