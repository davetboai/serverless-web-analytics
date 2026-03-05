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

    # TTL: 13 months
    ttl = int(now.timestamp()) + (86400 * 395)

    item = {
        "pk": f"{site_id}#{date_str}",
        "sk": f"{now.isoformat()}#{uuid.uuid4().hex[:8]}",
        "path": (body.get("url") or "/")[:512],
        "referrer": ref_domain[:256],
        "country": event.get("headers", {}).get("cloudfront-viewer-country", "XX"),
        "device": device,
        "screen": f"{sw}x{int(body.get('sh', 0) or 0)}",
        "language": (body.get("lang") or "")[:16],
        "visitor": visitor_hash,
        "ttl": ttl,
    }
    table.put_item(Item=item)

    # Auto-register site
    table.put_item(
        Item={"pk": "SITES", "sk": site_id, "domain": ref_domain or site_id},
        ConditionExpression="attribute_not_exists(pk)",
    ) if False else _register_site(site_id)

    return _resp(200, "ok")


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
