import json

from conftest import make_event, load_collector


def test_valid_pageview(ddb_table):
    collector = load_collector()

    event = make_event({
        "sid": "test-site",
        "url": "/hello",
        "ref": "https://google.com/search?q=test",
        "sw": 1920,
        "sh": 1080,
        "lang": "en-US",
        "ses": "abc123",
    })

    result = collector.handler(event, None)
    assert result["statusCode"] == 200

    # Verify pageview was written
    items = ddb_table.scan()["Items"]
    pageview_items = [
        i for i in items
        if not i["pk"].startswith(("LIVE#", "SITES", "SUMMARY#", "SESSION#"))
    ]
    assert len(pageview_items) == 1
    item = pageview_items[0]
    assert item["path"] == "/hello"
    assert item["referrer"] == "google.com"
    assert item["device"] == "desktop"
    assert item["country"] == "US"
    assert len(item["visitor"]) == 16


def test_bot_filtering(ddb_table):
    collector = load_collector()

    event = make_event(
        {"sid": "test-site", "url": "/"},
        headers={
            "user-agent": "Googlebot/2.1",
            "x-forwarded-for": "1.2.3.4",
            "cloudfront-viewer-country": "US",
        },
    )
    result = collector.handler(event, None)
    assert result["statusCode"] == 200

    items = ddb_table.scan()["Items"]
    pageview_items = [
        i for i in items
        if not i["pk"].startswith(("LIVE#", "SITES", "SUMMARY#", "SESSION#"))
    ]
    assert len(pageview_items) == 0


def test_invalid_json(ddb_table):
    collector = load_collector()

    event = make_event("not json{{{")
    result = collector.handler(event, None)
    assert result["statusCode"] == 400


def test_missing_sid(ddb_table):
    collector = load_collector()

    event = make_event({"url": "/"})
    result = collector.handler(event, None)
    assert result["statusCode"] == 400


def test_device_detection(ddb_table):
    collector = load_collector()

    # Mobile
    event = make_event({"sid": "s", "url": "/", "sw": 375, "sh": 812})
    collector.handler(event, None)

    # Tablet
    event = make_event({"sid": "s", "url": "/", "sw": 800, "sh": 1024})
    collector.handler(event, None)

    items = ddb_table.scan()["Items"]
    pageviews = [i for i in items if i["pk"].startswith("s#")]
    devices = [i["device"] for i in pageviews]
    assert "mobile" in devices
    assert "tablet" in devices


def test_options_request(ddb_table):
    collector = load_collector()

    event = make_event({}, method="OPTIONS")
    result = collector.handler(event, None)
    assert result["statusCode"] == 200
    assert "Access-Control-Allow-Origin" in result["headers"]


def test_ping_event(ddb_table):
    collector = load_collector()

    event = make_event({
        "sid": "test-site",
        "type": "ping",
        "url": "/page",
        "ses": "sess1",
        "dur": 30,
    })
    result = collector.handler(event, None)
    assert result["statusCode"] == 200

    # Should write session record but no pageview
    items = ddb_table.scan()["Items"]
    pageviews = [
        i for i in items
        if not i["pk"].startswith(("LIVE#", "SITES", "SUMMARY#", "SESSION#"))
    ]
    sessions = [i for i in items if i["pk"].startswith("SESSION#")]
    assert len(pageviews) == 0
    assert len(sessions) == 1
    assert int(sessions[0]["duration"]) == 30


def test_summary_written(ddb_table):
    """Verify that a pageview writes a SUMMARY record with correct counters."""
    collector = load_collector()

    event = make_event({
        "sid": "test-site",
        "url": "/about",
        "ref": "https://twitter.com/post",
        "sw": 1920,
        "sh": 1080,
        "ses": "s1",
    })
    collector.handler(event, None)

    items = ddb_table.scan()["Items"]
    summaries = [i for i in items if i["pk"].startswith("SUMMARY#")]
    assert len(summaries) == 1
    s = summaries[0]
    assert int(s["pageviews"]) == 1
    assert isinstance(s.get("visitors"), set)
    assert len(s["visitors"]) == 1
    assert "paths" in s
    assert "countries" in s


def test_live_visitor_written(ddb_table):
    """Verify that any event writes a LIVE presence record."""
    collector = load_collector()

    event = make_event({"sid": "test-site", "url": "/"})
    collector.handler(event, None)

    items = ddb_table.scan()["Items"]
    live = [i for i in items if i["pk"].startswith("LIVE#")]
    assert len(live) == 1


def test_utm_extraction(ddb_table):
    """Verify UTM parameters are extracted and stored."""
    collector = load_collector()

    event = make_event({
        "sid": "test-site",
        "url": "/landing?utm_source=google&utm_medium=cpc&utm_campaign=spring",
        "ses": "s1",
    })
    collector.handler(event, None)

    items = ddb_table.scan()["Items"]
    pageview = [i for i in items if i["pk"].startswith("test-site#")][0]
    # Path should be stripped of query string
    assert pageview["path"] == "/landing"
    assert pageview["utm"]["utm_source"] == "google"
    assert pageview["utm"]["utm_medium"] == "cpc"
    assert pageview["utm"]["utm_campaign"] == "spring"

    # Summary should have UTM maps
    summary = [i for i in items if i["pk"].startswith("SUMMARY#")][0]
    assert "utm_sources" in summary
    assert int(summary["utm_sources"]["google"]) == 1


def test_browser_os_detection(ddb_table):
    """Verify browser and OS are parsed from User-Agent."""
    collector = load_collector()

    event = make_event(
        {"sid": "test-site", "url": "/", "sw": 1920, "sh": 1080},
        headers={
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "x-forwarded-for": "1.2.3.4",
            "cloudfront-viewer-country": "US",
        },
    )
    collector.handler(event, None)

    items = ddb_table.scan()["Items"]
    pageview = [i for i in items if i["pk"].startswith("test-site#")][0]
    assert pageview["browser"] == "Chrome"
    assert pageview["os"] == "macOS"

    summary = [i for i in items if i["pk"].startswith("SUMMARY#")][0]
    assert "browsers" in summary
    assert "oses" in summary
    assert int(summary["browsers"]["Chrome"]) == 1
    assert int(summary["oses"]["macOS"]) == 1


def test_language_aggregation(ddb_table):
    """Verify language is aggregated into SUMMARY."""
    collector = load_collector()

    event = make_event({
        "sid": "test-site",
        "url": "/",
        "lang": "en-US",
        "ses": "s1",
    })
    collector.handler(event, None)

    items = ddb_table.scan()["Items"]
    summary = [i for i in items if i["pk"].startswith("SUMMARY#")][0]
    assert "languages" in summary
    assert int(summary["languages"]["en-US"]) == 1


def test_entry_exit_pages(ddb_table):
    """Verify entry/exit pages are tracked per session."""
    collector = load_collector()

    # First pageview — should set both entry and exit
    event = make_event({"sid": "test-site", "url": "/home", "ses": "sess1"})
    collector.handler(event, None)

    # Second pageview in same session — entry stays, exit updates
    event = make_event({"sid": "test-site", "url": "/about", "ses": "sess1"})
    collector.handler(event, None)

    items = ddb_table.scan()["Items"]
    sessions = [i for i in items if i["pk"].startswith("SESSION#")]
    assert len(sessions) == 1
    assert sessions[0]["entry_page"] == "/home"
    assert sessions[0]["exit_page"] == "/about"


def test_browser_detection_edge(ddb_table):
    """Edge should be detected before Chrome (since Edge UA contains Chrome)."""
    collector = load_collector()

    event = make_event(
        {"sid": "test-site", "url": "/"},
        headers={
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
            "x-forwarded-for": "1.2.3.4",
            "cloudfront-viewer-country": "US",
        },
    )
    collector.handler(event, None)

    items = ddb_table.scan()["Items"]
    pageview = [i for i in items if i["pk"].startswith("test-site#")][0]
    assert pageview["browser"] == "Edge"
    assert pageview["os"] == "Windows"


def test_channel_classification(ddb_table):
    """Verify referrer domain is classified into a traffic channel."""
    collector = load_collector()

    # Google referrer → Search
    event = make_event({
        "sid": "test-site",
        "url": "/",
        "ref": "https://www.google.com/search?q=test",
    })
    collector.handler(event, None)

    items = ddb_table.scan()["Items"]
    pageview = [i for i in items if i["pk"].startswith("test-site#")][0]
    assert pageview["channel"] == "Search"

    summary = [i for i in items if i["pk"].startswith("SUMMARY#")][0]
    assert "channels" in summary
    assert int(summary["channels"]["Search"]) == 1


def test_channel_direct(ddb_table):
    """No referrer → Direct channel."""
    collector = load_collector()
    event = make_event({"sid": "test-site", "url": "/", "ref": ""})
    collector.handler(event, None)

    items = ddb_table.scan()["Items"]
    pageview = [i for i in items if i["pk"].startswith("test-site#")][0]
    assert pageview["channel"] == "Direct"


def test_channel_social(ddb_table):
    """Twitter referrer → Social channel."""
    collector = load_collector()
    event = make_event({
        "sid": "test-site",
        "url": "/",
        "ref": "https://twitter.com/user/status/123",
    })
    collector.handler(event, None)

    items = ddb_table.scan()["Items"]
    pageview = [i for i in items if i["pk"].startswith("test-site#")][0]
    assert pageview["channel"] == "Social"


def test_channel_paid_utm(ddb_table):
    """UTM medium=cpc → Paid channel even with google referrer."""
    collector = load_collector()
    event = make_event({
        "sid": "test-site",
        "url": "/?utm_medium=cpc&utm_source=google",
        "ref": "https://www.google.com/",
    })
    collector.handler(event, None)

    items = ddb_table.scan()["Items"]
    pageview = [i for i in items if i["pk"].startswith("test-site#")][0]
    assert pageview["channel"] == "Paid"


def test_custom_event(ddb_table):
    """Verify custom events are stored and summarized."""
    collector = load_collector()

    event = make_event({
        "sid": "test-site",
        "type": "event",
        "name": "signup",
        "props": {"plan": "pro"},
        "url": "/pricing",
        "ses": "s1",
    })
    result = collector.handler(event, None)
    assert result["statusCode"] == 200

    items = ddb_table.scan()["Items"]
    events = [i for i in items if i["pk"].startswith("EVENT#")]
    assert len(events) == 1
    assert events[0]["event_name"] == "signup"
    assert events[0]["props"]["plan"] == "pro"

    event_summaries = [i for i in items if i["pk"].startswith("EVENTS#")]
    assert len(event_summaries) == 1
    assert int(event_summaries[0]["total_events"]) == 1
    assert int(event_summaries[0]["events"]["signup"]) == 1


def test_custom_event_missing_name(ddb_table):
    """Custom event without name should return 400."""
    collector = load_collector()

    event = make_event({
        "sid": "test-site",
        "type": "event",
        "url": "/",
    })
    result = collector.handler(event, None)
    assert result["statusCode"] == 400


def test_path_strips_query_string(ddb_table):
    """Stored path should not include query parameters."""
    collector = load_collector()

    event = make_event({
        "sid": "test-site",
        "url": "/search?q=hello&page=2",
    })
    collector.handler(event, None)

    items = ddb_table.scan()["Items"]
    pageview = [i for i in items if i["pk"].startswith("test-site#")][0]
    assert pageview["path"] == "/search"
