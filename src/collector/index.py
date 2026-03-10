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

# Referrer channel classification
SEARCH_ENGINES = {"google", "bing", "yahoo", "duckduckgo", "baidu", "yandex", "ecosia", "brave"}
SOCIAL_NETWORKS = {
    "facebook", "twitter", "x", "linkedin", "reddit", "youtube", "instagram",
    "pinterest", "tiktok", "mastodon", "threads", "hacker news",
}
EMAIL_PROVIDERS = {"mail", "email", "outlook", "gmail", "yahoo"}


def _classify_channel(ref_domain, utm):
    """Classify traffic source into a channel."""
    if utm.get("utm_medium") in ("cpc", "ppc", "paid", "ad", "ads", "banner", "display"):
        return "Paid"
    if utm.get("utm_medium") == "email" or utm.get("utm_source") == "email":
        return "Email"
    if not ref_domain:
        return "Direct"
    domain_lower = ref_domain.lower()
    # Strip www. and common TLDs for matching
    base = domain_lower.replace("www.", "").split(".")[0]
    if base in SEARCH_ENGINES or "search" in domain_lower:
        return "Search"
    if base in SOCIAL_NETWORKS or "t.co" in domain_lower:
        return "Social"
    for ep in EMAIL_PROVIDERS:
        if ep in domain_lower:
            return "Email"
    return "Referral"


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

    path = event.get("rawPath", "")
    if path.endswith("/ingest"):
        return _handle_server_event(event)

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
    # For IPv6, use only the /64 prefix (first 4 groups) to avoid hash churn
    # from privacy extensions rotating the interface identifier
    if ":" in ip:
        parts = ip.split(":")
        ip = ":".join(parts[:4])
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

    # Look up site-specific TTL override
    site_ttl_days = _get_site_ttl(site_id)
    if site_ttl_days:
        ttl = int(now.timestamp()) + (86400 * site_ttl_days)

    if event_type == "perf":
        # Page load performance metrics
        perf = body.get("perf") or {}
        perf_item = {
            "pk": f"PERF#{site_id}#{date_str}",
            "sk": f"{now.isoformat()}#{uuid.uuid4().hex[:8]}",
            "path": (body.get("url") or "/").split("?")[0][:512],
            "dns": min(int(perf.get("dns", 0) or 0), 60000),
            "tcp": min(int(perf.get("tcp", 0) or 0), 60000),
            "ttfb": min(int(perf.get("ttfb", 0) or 0), 60000),
            "load": min(int(perf.get("load", 0) or 0), 60000),
            "dom": min(int(perf.get("dom", 0) or 0), 60000),
            "visitor": visitor_hash,
            "ttl": ttl,
        }
        table.put_item(Item=perf_item)
        return _resp(200, "ok")

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

    # Classify traffic channel
    channel = _classify_channel(ref_domain, utm)

    if event_type == "event":
        # Custom event — store with event name and optional properties
        event_name = (body.get("name") or "")[:64]
        if not event_name:
            return _resp(400, "missing event name")
        event_props = body.get("props") or {}
        # Sanitize props: string keys/values, max 10 props
        clean_props = {}
        for k, v in list(event_props.items())[:10]:
            clean_props[str(k)[:32]] = str(v)[:128]

        item = {
            "pk": f"EVENT#{site_id}#{date_str}",
            "sk": f"{now.isoformat()}#{uuid.uuid4().hex[:8]}",
            "event_name": event_name,
            "path": path_val,
            "visitor": visitor_hash,
            "session": session_id,
            "ttl": ttl,
        }
        if clean_props:
            item["props"] = clean_props
        table.put_item(Item=item)

        # Update event summary
        _update_event_summary(site_id, date_str, ttl, event_name, visitor_hash)
        return _resp(200, "ok")

    # Pageview event (raw)
    item = {
        "pk": f"{site_id}#{date_str}",
        "sk": f"{now.isoformat()}#{uuid.uuid4().hex[:8]}",
        "path": path_val,
        "referrer": ref_domain[:256],
        "channel": channel,
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
                    visitor_hash, session_id, browser, os_name, language, utm, channel)

    # Track entry/exit pages per session
    if session_id:
        _update_session_pages(site_id, date_str, visitor_hash, session_id, path_val, ttl)

    _register_site(site_id)

    return _resp(200, "ok")


def _update_summary(site_id, date_str, ttl, path_val, ref_domain, country, device,
                    visitor_hash, session_id, browser, os_name, language, utm, channel):
    """Atomically increment daily summary counters (two-step for nested maps)."""
    safe_path = path_val.replace("#", "_").replace(".", "_")[:64]
    safe_ref = ref_domain.replace("#", "_").replace(".", "_")[:64] if ref_domain else ""
    safe_browser = browser.replace("#", "_").replace(".", "_")[:32]
    safe_os = os_name.replace("#", "_").replace(".", "_")[:32]
    safe_lang = language.replace("#", "_").replace(".", "_")[:16] if language else ""
    safe_channel = channel.replace("#", "_").replace(".", "_")[:32]
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
            "#channels": "channels",
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
            "channels.#ch = if_not_exists(channels.#ch, :zero) + :one",
        ]
        names = {"#p": safe_path, "#c": country, "#d": device,
                 "#br": safe_browser, "#os": safe_os, "#ch": safe_channel}
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


def _update_event_summary(site_id, date_str, ttl, event_name, visitor_hash):
    """Atomically increment custom event counters."""
    safe_name = event_name.replace("#", "_").replace(".", "_")[:64]
    key = {"pk": f"EVENTS#{site_id}", "sk": date_str}
    try:
        table.update_item(
            Key=key,
            UpdateExpression=(
                "SET #events = if_not_exists(#events, :empty_map), #ttl = :ttl "
                "ADD #total :one, #visitors :vset"
            ),
            ExpressionAttributeNames={
                "#events": "events", "#ttl": "ttl", "#total": "total_events",
                "#visitors": "event_visitors",
            },
            ExpressionAttributeValues={
                ":empty_map": {}, ":ttl": ttl, ":one": 1,
                ":vset": {visitor_hash},
            },
        )
        table.update_item(
            Key=key,
            UpdateExpression="SET events.#n = if_not_exists(events.#n, :zero) + :one",
            ExpressionAttributeNames={"#n": safe_name},
            ExpressionAttributeValues={":one": 1, ":zero": 0},
        )
    except Exception:
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


_site_ttl_cache = {}


def _get_site_ttl(site_id):
    """Get configured TTL days for a site (cached per Lambda invocation)."""
    if site_id in _site_ttl_cache:
        return _site_ttl_cache[site_id]
    try:
        result = table.get_item(Key={"pk": "SITES", "sk": site_id})
        item = result.get("Item", {})
        ttl_days = int(item.get("ttl_days", 0) or 0)
        _site_ttl_cache[site_id] = ttl_days if ttl_days > 0 else None
    except Exception:
        _site_ttl_cache[site_id] = None
    return _site_ttl_cache[site_id]


def _handle_server_event(event):
    """Server-side event API — accepts events with API key auth."""
    try:
        body = json.loads(event.get("body", "{}"))
    except (json.JSONDecodeError, TypeError):
        return _resp(400, "invalid json")

    site_id = (body.get("site_id") or "").strip()
    api_key = (body.get("api_key") or "").strip()
    if not site_id or not api_key:
        return _resp(400, "site_id and api_key required")

    # Validate API key against stored site record
    if not _validate_api_key(site_id, api_key):
        return _resp(403, "invalid api_key")

    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")

    site_ttl_days = _get_site_ttl(site_id)
    ttl = int(now.timestamp()) + (86400 * (site_ttl_days or 395))

    event_name = (body.get("name") or "").strip()[:64]
    if not event_name:
        return _resp(400, "name required")

    # Use provided user_id or generate from event data
    user_id = (body.get("user_id") or "")[:64]
    visitor_hash = hashlib.sha256(f"server:{user_id}:{site_id}".encode()).hexdigest()[:16] if user_id else uuid.uuid4().hex[:16]

    props = body.get("props") or {}
    clean_props = {}
    for k, v in list(props.items())[:10]:
        clean_props[str(k)[:32]] = str(v)[:128]

    item = {
        "pk": f"EVENT#{site_id}#{date_str}",
        "sk": f"{now.isoformat()}#{uuid.uuid4().hex[:8]}",
        "event_name": event_name,
        "path": (body.get("url") or "/")[:512],
        "visitor": visitor_hash,
        "session": "server",
        "ttl": ttl,
    }
    if clean_props:
        item["props"] = clean_props
    table.put_item(Item=item)

    _update_event_summary(site_id, date_str, ttl, event_name, visitor_hash)
    return _resp(200, "ok")


_api_key_cache = {}


def _validate_api_key(site_id, api_key):
    """Check API key against the site record (cached)."""
    cache_key = f"{site_id}:{api_key}"
    if cache_key in _api_key_cache:
        return _api_key_cache[cache_key]
    try:
        result = table.get_item(Key={"pk": "SITES", "sk": site_id})
        item = result.get("Item", {})
        stored_key = item.get("api_key", "")
        valid = stored_key and stored_key == api_key
        _api_key_cache[cache_key] = valid
    except Exception:
        _api_key_cache[cache_key] = False
    return _api_key_cache[cache_key]


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
