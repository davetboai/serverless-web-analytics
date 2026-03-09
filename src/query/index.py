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

    if path.endswith("/events"):
        return _get_events(params)

    if path.endswith("/recent"):
        return _get_recent(params)

    if path.endswith("/goals"):
        body = {}
        try:
            body = json.loads(event.get("body", "{}") or "{}")
        except (json.JSONDecodeError, TypeError):
            pass
        if method == "POST":
            return _create_goal(body)
        if method == "DELETE":
            return _delete_goal(body)
        return _get_goals(params)

    if path.endswith("/perf"):
        return _get_perf(params)

    if path.endswith("/compare"):
        return _get_compare(params)

    return _get_stats(params)


def _list_sites():
    result = table.query(KeyConditionExpression=Key("pk").eq("SITES"))
    sites = [{
        "id": item["sk"],
        "domain": item.get("domain", ""),
        "ttl_days": int(item.get("ttl_days", 395)),
    } for item in result.get("Items", [])]
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
    ttl_days = body.get("ttl_days")
    update_expr = "SET #d = :d"
    names = {"#d": "domain"}
    values = {":d": domain[:256]}
    if ttl_days is not None:
        ttl_val = max(30, min(int(ttl_days), 730))  # 1 month to 2 years
        update_expr += ", #ttl_days = :ttl_days"
        names["#ttl_days"] = "ttl_days"
        values[":ttl_days"] = ttl_val
    table.update_item(
        Key={"pk": "SITES", "sk": site_id},
        UpdateExpression=update_expr,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
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
    browsers: Counter = Counter()
    oses: Counter = Counter()
    languages: Counter = Counter()
    utm_sources: Counter = Counter()
    utm_mediums: Counter = Counter()
    utm_campaigns: Counter = Counter()
    channels: Counter = Counter()
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
        for br, c in (summary.get("browsers") or {}).items():
            browsers[br] += int(c)
        for o, c in (summary.get("oses") or {}).items():
            oses[o] += int(c)
        for lang, c in (summary.get("languages") or {}).items():
            languages[lang] += int(c)
        for src, c in (summary.get("utm_sources") or {}).items():
            utm_sources[src] += int(c)
        for med, c in (summary.get("utm_mediums") or {}).items():
            utm_mediums[med] += int(c)
        for cmp, c in (summary.get("utm_campaigns") or {}).items():
            utm_campaigns[cmp] += int(c)
        for ch, c in (summary.get("channels") or {}).items():
            channels[ch] += int(c)

        dates.append({
            "date": d,
            "pageviews": day_views,
            "visitors": len(day_visitors) if isinstance(day_visitors, set) else 0,
        })
        current += timedelta(days=1)

    # Session metrics
    total_sessions = max(len(all_session_ids), 1)
    bounce_sessions = sum(1 for s in all_sessions if int(s.get("duration", 0)) == 0)
    bounce_rate = round(bounce_sessions / max(len(all_sessions), 1) * 100, 1) if all_sessions else 0

    total_duration = sum(int(s.get("duration", 0)) for s in all_sessions)
    avg_duration = round(total_duration / len(all_sessions)) if all_sessions else 0

    # Entry/exit pages from session records
    entry_pages: Counter = Counter()
    exit_pages: Counter = Counter()
    for s in all_sessions:
        ep = s.get("entry_page")
        xp = s.get("exit_page")
        if ep:
            entry_pages[ep] += 1
        if xp:
            exit_pages[xp] += 1

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
        "browsers": [{"name": b, "count": c} for b, c in browsers.most_common(20)],
        "oses": [{"name": o, "count": c} for o, c in oses.most_common(20)],
        "languages": [{"code": lang, "count": c} for lang, c in languages.most_common(20)],
        "utmSources": [{"name": s, "count": c} for s, c in utm_sources.most_common(20)],
        "utmMediums": [{"name": m, "count": c} for m, c in utm_mediums.most_common(20)],
        "utmCampaigns": [{"name": cp, "count": c} for cp, c in utm_campaigns.most_common(20)],
        "entryPages": [{"path": p, "count": c} for p, c in entry_pages.most_common(20)],
        "exitPages": [{"path": p, "count": c} for p, c in exit_pages.most_common(20)],
        "channels": [{"name": ch, "count": c} for ch, c in channels.most_common(20)],
    })


def _get_events(params):
    """Return custom event stats for a site over a date range."""
    site_id = params.get("site_id", "")
    if not site_id:
        return _resp(400, {"error": "missing site_id"})

    days = min(int(params.get("days", 7)), 90)
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=days - 1)

    event_summaries = table.query(
        KeyConditionExpression=(
            Key("pk").eq(f"EVENTS#{site_id}")
            & Key("sk").between(start_date.isoformat(), end_date.isoformat())
        ),
    ).get("Items", [])

    events: Counter = Counter()
    total = 0
    all_visitors: set = set()
    for s in event_summaries:
        total += int(s.get("total_events", 0))
        visitors = s.get("event_visitors", set())
        if isinstance(visitors, set):
            all_visitors.update(visitors)
        for name, count in (s.get("events") or {}).items():
            events[name] += int(count)

    return _resp(200, {
        "totalEvents": total,
        "uniqueVisitors": len(all_visitors),
        "events": [{"name": n, "count": c} for n, c in events.most_common(50)],
    })


def _get_recent(params):
    """Return recent pageviews (last ~30 min) for real-time feed."""
    site_id = params.get("site_id", "")
    if not site_id:
        return _resp(400, {"error": "missing site_id"})

    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    cutoff = (now - timedelta(minutes=30)).isoformat()

    items = table.query(
        KeyConditionExpression=(
            Key("pk").eq(f"{site_id}#{date_str}")
            & Key("sk").gte(cutoff)
        ),
        ScanIndexForward=False,
        Limit=50,
    ).get("Items", [])

    recent = []
    for item in items:
        recent.append({
            "time": item["sk"].split("#")[0],
            "path": item.get("path", ""),
            "country": item.get("country", ""),
            "device": item.get("device", ""),
            "browser": item.get("browser", ""),
            "referrer": item.get("referrer", ""),
        })

    return _resp(200, {"recent": recent})


def _create_goal(body):
    """Create a goal. Types: 'page' (path match) or 'event' (event name match)."""
    site_id = (body.get("site_id") or "").strip()
    goal_name = (body.get("name") or "").strip()
    goal_type = (body.get("type") or "").strip()  # "page" or "event"
    goal_value = (body.get("value") or "").strip()  # path or event name
    if not site_id or not goal_name or goal_type not in ("page", "event") or not goal_value:
        return _resp(400, {"error": "site_id, name, type (page|event), and value required"})
    goal_id = goal_name.lower().replace(" ", "-")[:32]
    table.put_item(Item={
        "pk": f"GOALS#{site_id}",
        "sk": goal_id,
        "name": goal_name,
        "goal_type": goal_type,
        "value": goal_value,
    })
    return _resp(201, {"id": goal_id, "name": goal_name, "type": goal_type, "value": goal_value})


def _delete_goal(body):
    site_id = (body.get("site_id") or "").strip()
    goal_id = (body.get("id") or "").strip()
    if not site_id or not goal_id:
        return _resp(400, {"error": "site_id and id required"})
    table.delete_item(Key={"pk": f"GOALS#{site_id}", "sk": goal_id})
    return _resp(200, {"deleted": goal_id})


def _get_goals(params):
    """List goals with conversion data for the date range."""
    site_id = params.get("site_id", "")
    if not site_id:
        return _resp(400, {"error": "missing site_id"})

    days = min(int(params.get("days", 7)), 90)
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=days - 1)

    # Fetch goal definitions
    goal_items = _query_all(f"GOALS#{site_id}")
    if not goal_items:
        return _resp(200, {"goals": []})

    # Fetch summaries for visitor count and path data
    summaries = table.query(
        KeyConditionExpression=(
            Key("pk").eq(f"SUMMARY#{site_id}")
            & Key("sk").between(start_date.isoformat(), end_date.isoformat())
        ),
    ).get("Items", [])

    all_visitors: set = set()
    path_counts: Counter = Counter()
    for s in summaries:
        visitors = s.get("visitors", set())
        if isinstance(visitors, set):
            all_visitors.update(visitors)
        for p, c in (s.get("paths") or {}).items():
            path_counts[p] += int(c)

    total_visitors = max(len(all_visitors), 1)

    # Fetch event summaries for event goals
    event_summaries = table.query(
        KeyConditionExpression=(
            Key("pk").eq(f"EVENTS#{site_id}")
            & Key("sk").between(start_date.isoformat(), end_date.isoformat())
        ),
    ).get("Items", [])

    event_counts: Counter = Counter()
    for es in event_summaries:
        for name, count in (es.get("events") or {}).items():
            event_counts[name] += int(count)

    # Compute conversions per goal
    goals = []
    for g in goal_items:
        goal_type = g.get("goal_type", "")
        value = g.get("value", "")
        completions = 0
        if goal_type == "page":
            # Match paths that equal or start with the goal value
            for p, c in path_counts.items():
                if p == value or p.startswith(value.rstrip("/") + "/"):
                    completions += c
        elif goal_type == "event":
            completions = event_counts.get(value, 0)

        conv_rate = round(completions / total_visitors * 100, 1) if total_visitors > 0 else 0
        goals.append({
            "id": g["sk"],
            "name": g.get("name", g["sk"]),
            "type": goal_type,
            "value": value,
            "completions": completions,
            "conversionRate": conv_rate,
        })

    return _resp(200, {"goals": goals, "totalVisitors": total_visitors})


def _get_perf(params):
    """Return page load performance stats."""
    site_id = params.get("site_id", "")
    if not site_id:
        return _resp(400, {"error": "missing site_id"})

    days = min(int(params.get("days", 7)), 90)
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=days - 1)

    all_perf = []
    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        items = _query_all(f"PERF#{site_id}#{date_str}")
        all_perf.extend(items)
        current += timedelta(days=1)

    if not all_perf:
        return _resp(200, {
            "sampleCount": 0,
            "avgLoad": 0, "avgTtfb": 0, "avgDom": 0,
            "p75Load": 0, "p75Ttfb": 0,
            "byPage": [],
        })

    loads = sorted(int(p.get("load", 0)) for p in all_perf)
    ttfbs = sorted(int(p.get("ttfb", 0)) for p in all_perf)
    doms = [int(p.get("dom", 0)) for p in all_perf]
    n = len(all_perf)

    # Per-page averages
    page_loads: dict = {}
    page_counts: Counter = Counter()
    for p in all_perf:
        path = p.get("path", "/")
        page_counts[path] += 1
        page_loads.setdefault(path, []).append(int(p.get("load", 0)))

    by_page = []
    for path, count in page_counts.most_common(20):
        vals = page_loads[path]
        by_page.append({
            "path": path,
            "count": count,
            "avgLoad": round(sum(vals) / len(vals)),
        })

    return _resp(200, {
        "sampleCount": n,
        "avgLoad": round(sum(loads) / n),
        "avgTtfb": round(sum(ttfbs) / n),
        "avgDom": round(sum(doms) / n),
        "p75Load": loads[int(n * 0.75)] if n > 0 else 0,
        "p75Ttfb": ttfbs[int(n * 0.75)] if n > 0 else 0,
        "byPage": by_page,
    })


def _aggregate_period(site_id, start_date, end_date):
    """Aggregate summary data for a date range. Returns (pageviews, visitors_count)."""
    summaries = table.query(
        KeyConditionExpression=(
            Key("pk").eq(f"SUMMARY#{site_id}")
            & Key("sk").between(start_date.isoformat(), end_date.isoformat())
        ),
    ).get("Items", [])

    total_pageviews = 0
    all_visitors: set = set()
    for s in summaries:
        total_pageviews += int(s.get("pageviews", 0))
        visitors = s.get("visitors", set())
        if isinstance(visitors, set):
            all_visitors.update(visitors)

    return total_pageviews, len(all_visitors)


def _get_compare(params):
    """Compare current period vs previous period of same length."""
    site_id = params.get("site_id", "")
    if not site_id:
        return _resp(400, {"error": "missing site_id"})

    days = min(int(params.get("days", 7)), 90)
    end_date = datetime.now(timezone.utc).date()
    current_start = end_date - timedelta(days=days - 1)
    prev_end = current_start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=days - 1)

    curr_views, curr_visitors = _aggregate_period(site_id, current_start, end_date)
    prev_views, prev_visitors = _aggregate_period(site_id, prev_start, prev_end)

    def pct_change(curr, prev):
        if prev == 0:
            return 100.0 if curr > 0 else 0.0
        return round((curr - prev) / prev * 100, 1)

    return _resp(200, {
        "current": {
            "start": current_start.isoformat(),
            "end": end_date.isoformat(),
            "pageviews": curr_views,
            "visitors": curr_visitors,
        },
        "previous": {
            "start": prev_start.isoformat(),
            "end": prev_end.isoformat(),
            "pageviews": prev_views,
            "visitors": prev_visitors,
        },
        "change": {
            "pageviews": pct_change(curr_views, prev_views),
            "visitors": pct_change(curr_visitors, prev_visitors),
        },
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
