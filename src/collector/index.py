import json
import os
import hashlib
import re
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

import boto3

TABLE_NAME = os.environ["TABLE_NAME"]
ddb = boto3.resource("dynamodb")
table = ddb.Table(TABLE_NAME)

BOT_PATTERNS = [
    "bot", "crawler", "spider", "headless", "phantom", "selenium",
    "lighthouse", "pagespeed", "gtmetrix", "pingdom", "uptimerobot",
]

UTM_PARAMS = ("utm_source", "utm_medium", "utm_campaign")

# Browser detection patterns (order matters — check specific before generic)
BROWSER_PATTERNS = [
    (r"Edg[eA]?/", "Edge"),
    (r"OPR/|Opera/", "Opera"),
    (r"SamsungBrowser/", "Samsung"),
    (r"UCBrowser/", "UC Browser"),
    (r"Chrome/", "Chrome"),
    (r"Safari/", "Safari"),
    (r"Firefox/", "Firefox"),
]

OS_PATTERNS = [
    (r"iPhone|iPad|iPod", "iOS"),
    (r"Android", "Android"),
    (r"Windows", "Windows"),
    (r"Macintosh|Mac OS", "macOS"),
    (r"Linux", "Linux"),
    (r"CrOS", "ChromeOS"),
]


def _parse_utm(url_path):
    """Extract UTM params from URL query string."""
    try:
        qs = url_path.split("?", 1)[1] if "?" in url_path else ""
        params = parse_qs(qs)
        return {k: params[k][0][:64] for k in UTM_PARAMS if k in params}
    except Exception:
        return {}


def _parse_browser(ua):
    """Detect browser name from User-Agent string."""
    for pattern, name in BROWSER_PATTERNS:
        if re.search(pattern, ua):
            return name
    return "Other"


def _parse_os(ua):
    """Detect OS from User-Agent string."""
    for pattern, name in OS_PATTERNS:
        if re.search(pattern, ua):
            return name
    return "Other"

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

    raw_url = (body.get("url") or "/")[:512]
    country = event.get("headers", {}).get("cloudfront-viewer-country", "XX")

    # Extract UTM params before stripping them from stored path
    utm = _parse_utm(raw_url)
    # Store path without query string for cleaner aggregation
    path_val = raw_url.split("?")[0] or "/"

    # Browser & OS from UA (use original case UA)
    raw_ua = event.get("headers", {}).get("user-agent", "")
    browser = _parse_browser(raw_ua)
    os_name = _parse_os(raw_ua)
    language = (body.get("lang") or "")[:16]

    # Pageview event (raw)
    item = {
        "pk": f"{site_id}#{date_str}",
        "sk": f"{now.isoformat()}#{uuid.uuid4().hex[:8]}",
        "path": path_val,
        "referrer": ref_domain[:256],
        "country": country,
        "device": device,
        "browser": browser,
        "os": os_name,
        "screen": f"{sw}x{int(body.get('sh', 0) or 0)}",
        "language": language,
        "visitor": visitor_hash,
        "session": session_id,
        "ttl": ttl,
    }
    if utm:
        item["utm"] = utm
    table.put_item(Item=item)

    # Update daily summary counters (atomic increments)
    _update_summary(site_id, date_str, ttl, path_val, ref_domain, country, device,
                    visitor_hash, session_id, browser, os_name, language, utm)

    # Track entry/exit pages per session
    if session_id:
        _update_session_pages(site_id, date_str, visitor_hash, session_id, path_val, ttl)

    _register_site(site_id)

    return _resp(200, "ok")


def _update_summary(site_id, date_str, ttl, path_val, ref_domain, country, device,
                    visitor_hash, session_id, browser, os_name, language, utm):
    """Atomically increment daily summary counters (two-step for nested maps)."""
    safe_path = path_val.replace("#", "_").replace(".", "_")[:64]
    safe_ref = ref_domain.replace("#", "_").replace(".", "_")[:64] if ref_domain else ""
    safe_browser = browser.replace("#", "_").replace(".", "_")[:32]
    safe_os = os_name.replace("#", "_").replace(".", "_")[:32]
    safe_lang = language.replace("#", "_").replace(".", "_")[:16] if language else ""
    key = {"pk": f"SUMMARY#{site_id}", "sk": date_str}

    try:
        # Step 1: Ensure item exists with map scaffolding + increment top-level counters
        add_parts = ["pageviews :one", "visitors :vset"]
        map_names = {
            "#paths": "paths", "#countries": "countries",
            "#devices": "devices", "#referrers": "referrers",
            "#browsers": "browsers", "#oses": "oses",
            "#languages": "languages", "#utm_sources": "utm_sources",
            "#utm_mediums": "utm_mediums", "#utm_campaigns": "utm_campaigns",
            "#ttl": "ttl",
        }
        attr_values = {
            ":one": 1, ":vset": {visitor_hash},
            ":ttl": ttl, ":empty_map": {},
        }
        if session_id:
            add_parts.append("sessions :sset")
            attr_values[":sset"] = {f"{visitor_hash}#{session_id}"}

        set_scaffolding = ", ".join(
            f"{alias} = if_not_exists({alias}, :empty_map)"
            for alias in map_names if alias != "#ttl"
        )

        table.update_item(
            Key=key,
            UpdateExpression=(
                f"SET {set_scaffolding}, #ttl = :ttl "
                "ADD " + ", ".join(add_parts)
            ),
            ExpressionAttributeNames=map_names,
            ExpressionAttributeValues=attr_values,
        )

        # Step 2: Increment nested map counters
        set_parts = [
            "paths.#p = if_not_exists(paths.#p, :zero) + :one",
            "countries.#c = if_not_exists(countries.#c, :zero) + :one",
            "devices.#d = if_not_exists(devices.#d, :zero) + :one",
            "browsers.#br = if_not_exists(browsers.#br, :zero) + :one",
            "oses.#os = if_not_exists(oses.#os, :zero) + :one",
        ]
        names = {"#p": safe_path, "#c": country, "#d": device,
                 "#br": safe_browser, "#os": safe_os}
        vals = {":one": 1, ":zero": 0}

        if safe_ref:
            set_parts.append("referrers.#r = if_not_exists(referrers.#r, :zero) + :one")
            names["#r"] = safe_ref

        if safe_lang:
            set_parts.append("languages.#lang = if_not_exists(languages.#lang, :zero) + :one")
            names["#lang"] = safe_lang

        # UTM aggregation
        utm_source = (utm.get("utm_source") or "").replace("#", "_").replace(".", "_")[:64]
        utm_medium = (utm.get("utm_medium") or "").replace("#", "_").replace(".", "_")[:64]
        utm_campaign = (utm.get("utm_campaign") or "").replace("#", "_").replace(".", "_")[:64]
        if utm_source:
            set_parts.append("utm_sources.#us = if_not_exists(utm_sources.#us, :zero) + :one")
            names["#us"] = utm_source
        if utm_medium:
            set_parts.append("utm_mediums.#um = if_not_exists(utm_mediums.#um, :zero) + :one")
            names["#um"] = utm_medium
        if utm_campaign:
            set_parts.append("utm_campaigns.#uc = if_not_exists(utm_campaigns.#uc, :zero) + :one")
            names["#uc"] = utm_campaign

        table.update_item(
            Key=key,
            UpdateExpression="SET " + ", ".join(set_parts),
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=vals,
        )
    except Exception:
        # Don't fail the request if summary update fails — raw data is already written
        pass


def _update_session_pages(site_id, date_str, visitor_hash, session_id, path_val, ttl):
    """Track entry page (first seen) and exit page (latest seen) per session."""
    try:
        table.update_item(
            Key={
                "pk": f"SESSION#{site_id}#{date_str}",
                "sk": f"{visitor_hash}#{session_id}",
            },
            UpdateExpression=(
                "SET entry_page = if_not_exists(entry_page, :path), "
                "exit_page = :path, #ttl = :ttl"
            ),
            ExpressionAttributeNames={"#ttl": "ttl"},
            ExpressionAttributeValues={":path": path_val, ":ttl": ttl},
        )
    except Exception:
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
