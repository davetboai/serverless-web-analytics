import json
import os
import hashlib
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3

TABLE_NAME = os.environ["TABLE_NAME"]
ddb = boto3.resource("dynamodb")
table = ddb.Table(TABLE_NAME)

BOT_PATTERNS = [
    "bot", "crawler", "spider", "headless", "phantom", "selenium",
    "lighthouse", "pagespeed", "gtmetrix", "pingdom", "uptimerobot",
]

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Cache-Control": "no-store",
}


def handler(event, context):
    method = event.get("requestContext", {}).get("http", {}).get("method", "")
    if method == "OPTIONS":
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}

    try:
        body = json.loads(event.get("body", "{}"))
    except (json.JSONDecodeError, TypeError):
        return _resp(400, "invalid json")

    site_id = body.get("sid", "").strip()
    if not site_id or len(site_id) > 64:
        return _resp(400, "invalid sid")

    # Bot filtering
    ua = event.get("headers", {}).get("user-agent", "").lower()
    if any(b in ua for b in BOT_PATTERNS):
        return _resp(200, "ok")

    # Visitor hash (privacy: never store raw IP)
    ip = event.get("headers", {}).get("x-forwarded-for", "").split(",")[0].strip()
    visitor_hash = hashlib.sha256(f"{ip}:{ua}:{site_id}".encode()).hexdigest()[:16]

    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")

    # Device type from screen width
    sw = int(body.get("sw", 0) or 0)
    device = "mobile" if sw < 768 else "tablet" if sw < 1024 else "desktop"

    # Referrer domain
    ref = body.get("ref", "") or ""
    ref_domain = ""
    if ref:
        try:
            ref_domain = urlparse(ref).netloc
        except Exception:
            pass

    event_type = body.get("type", "pageview")
    session_id = (body.get("ses") or "")[:16]

    # TTL: 13 months
    ttl = int(now.timestamp()) + (86400 * 395)

    # Update live visitor presence (5-minute TTL)
    live_ttl = int(now.timestamp()) + 300
    table.put_item(Item={
        "pk": f"LIVE#{site_id}",
        "sk": visitor_hash,
        "ttl": live_ttl,
    })

    if event_type == "ping":
        # Update session duration — write a session heartbeat record
        duration = min(int(body.get("dur", 0) or 0), 86400)
        if session_id and duration > 0:
            table.update_item(
                Key={
                    "pk": f"SESSION#{site_id}#{date_str}",
                    "sk": f"{visitor_hash}#{session_id}",
                },
                UpdateExpression="SET #dur = if_not_exists(#dur, :zero) + :dur, #ttl = :ttl",
                ExpressionAttributeNames={"#dur": "duration", "#ttl": "ttl"},
                ExpressionAttributeValues={":dur": duration, ":zero": 0, ":ttl": ttl},
            )
        return _resp(200, "ok")

    path_val = (body.get("url") or "/")[:512]
    country = event.get("headers", {}).get("cloudfront-viewer-country", "XX")

    # Pageview event (raw)
    item = {
        "pk": f"{site_id}#{date_str}",
        "sk": f"{now.isoformat()}#{uuid.uuid4().hex[:8]}",
        "path": path_val,
        "referrer": ref_domain[:256],
        "country": country,
        "device": device,
        "screen": f"{sw}x{int(body.get('sh', 0) or 0)}",
        "language": (body.get("lang") or "")[:16],
        "visitor": visitor_hash,
        "session": session_id,
        "ttl": ttl,
    }
    table.put_item(Item=item)

    # Update daily summary counters (atomic increments)
    _update_summary(site_id, date_str, ttl, path_val, ref_domain, country, device, visitor_hash, session_id)

    _register_site(site_id)

    return _resp(200, "ok")


def _update_summary(site_id, date_str, ttl, path_val, ref_domain, country, device, visitor_hash, session_id):
    """Atomically increment daily summary counters (two-step for nested maps)."""
    safe_path = path_val.replace("#", "_").replace(".", "_")[:64]
    safe_ref = ref_domain.replace("#", "_").replace(".", "_")[:64] if ref_domain else ""
    key = {"pk": f"SUMMARY#{site_id}", "sk": date_str}

    try:
        # Step 1: Ensure item exists with map scaffolding + increment top-level counters
        add_parts = ["pageviews :one", "visitors :vset"]
        attr_values = {
            ":one": 1, ":vset": {visitor_hash},
            ":ttl": ttl, ":empty_map": {},
        }
        if session_id:
            add_parts.append("sessions :sset")
            attr_values[":sset"] = {f"{visitor_hash}#{session_id}"}

        table.update_item(
            Key=key,
            UpdateExpression=(
                "SET #paths = if_not_exists(#paths, :empty_map), "
                "#countries = if_not_exists(#countries, :empty_map), "
                "#devices = if_not_exists(#devices, :empty_map), "
                "#referrers = if_not_exists(#referrers, :empty_map), "
                "#ttl = :ttl "
                "ADD " + ", ".join(add_parts)
            ),
            ExpressionAttributeNames={
                "#paths": "paths", "#countries": "countries",
                "#devices": "devices", "#referrers": "referrers", "#ttl": "ttl",
            },
            ExpressionAttributeValues=attr_values,
        )

        # Step 2: Increment nested map counters
        set_parts = [
            "paths.#p = if_not_exists(paths.#p, :zero) + :one",
            "countries.#c = if_not_exists(countries.#c, :zero) + :one",
            "devices.#d = if_not_exists(devices.#d, :zero) + :one",
        ]
        names = {"#p": safe_path, "#c": country, "#d": device}
        vals = {":one": 1, ":zero": 0}

        if safe_ref:
            set_parts.append("referrers.#r = if_not_exists(referrers.#r, :zero) + :one")
            names["#r"] = safe_ref

        table.update_item(
            Key=key,
            UpdateExpression="SET " + ", ".join(set_parts),
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=vals,
        )
    except Exception:
        # Don't fail the request if summary update fails — raw data is already written
        pass


def _register_site(site_id):
    try:
        table.put_item(
            Item={"pk": "SITES", "sk": site_id},
            ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
        )
    except ddb.meta.client.exceptions.ConditionalCheckFailedException:
        pass  # Already registered


def _resp(status, body):
    return {
        "statusCode": status,
        "headers": CORS_HEADERS,
        "body": body if isinstance(body, str) else json.dumps(body),
    }
