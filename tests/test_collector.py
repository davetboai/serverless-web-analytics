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
