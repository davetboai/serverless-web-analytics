import json
from datetime import datetime, timezone

from conftest import make_event, load_query


def test_list_sites_empty(ddb_table):
    query = load_query()
    event = make_event({}, method="GET", path="/api/sites")
    result = query.handler(event, None)
    body = json.loads(result["body"])
    assert result["statusCode"] == 200
    assert body["sites"] == []


def test_list_sites_with_data(ddb_table):
    query = load_query()
    ddb_table.put_item(Item={"pk": "SITES", "sk": "my-site", "domain": "example.com"})

    event = make_event({}, method="GET", path="/api/sites")
    result = query.handler(event, None)
    body = json.loads(result["body"])
    assert len(body["sites"]) == 1
    assert body["sites"][0]["id"] == "my-site"
    assert body["sites"][0]["domain"] == "example.com"


def test_get_stats_missing_site_id(ddb_table):
    query = load_query()
    event = make_event({}, method="GET", path="/api/query")
    event["queryStringParameters"] = {}
    result = query.handler(event, None)
    assert result["statusCode"] == 400


def test_get_stats_with_summary(ddb_table):
    query = load_query()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    ddb_table.put_item(Item={
        "pk": "SUMMARY#test-site",
        "sk": today,
        "pageviews": 42,
        "visitors": {"v1", "v2", "v3"},
        "sessions": {"v1#s1", "v2#s2"},
        "paths": {"/": 30, "/about": 12},
        "countries": {"US": 35, "UK": 7},
        "devices": {"desktop": 30, "mobile": 12},
        "referrers": {"google_com": 20},
        "ttl": 9999999999,
    })

    event = make_event({}, method="GET", path="/api/query")
    event["queryStringParameters"] = {"site_id": "test-site", "days": "1"}
    result = query.handler(event, None)
    body = json.loads(result["body"])

    assert result["statusCode"] == 200
    assert body["totalPageviews"] == 42
    assert body["totalVisitors"] == 3
    assert len(body["dates"]) == 1
    assert body["dates"][0]["pageviews"] == 42


def test_get_stats_empty(ddb_table):
    query = load_query()
    event = make_event({}, method="GET", path="/api/query")
    event["queryStringParameters"] = {"site_id": "nonexistent", "days": "7"}
    result = query.handler(event, None)
    body = json.loads(result["body"])

    assert result["statusCode"] == 200
    assert body["totalPageviews"] == 0
    assert body["totalVisitors"] == 0


def test_create_site(ddb_table):
    query = load_query()
    event = make_event(
        {"id": "new-site", "domain": "new.example.com"},
        method="POST",
        path="/api/sites",
    )
    result = query.handler(event, None)
    body = json.loads(result["body"])
    assert result["statusCode"] == 201
    assert body["id"] == "new-site"

    item = ddb_table.get_item(Key={"pk": "SITES", "sk": "new-site"}).get("Item")
    assert item is not None
    assert item["domain"] == "new.example.com"


def test_delete_site(ddb_table):
    query = load_query()
    ddb_table.put_item(Item={"pk": "SITES", "sk": "del-site", "domain": "del.com"})

    event = make_event({"id": "del-site"}, method="DELETE", path="/api/sites")
    result = query.handler(event, None)
    body = json.loads(result["body"])
    assert result["statusCode"] == 200
    assert body["deleted"] == "del-site"

    item = ddb_table.get_item(Key={"pk": "SITES", "sk": "del-site"}).get("Item")
    assert item is None


def test_rename_site(ddb_table):
    query = load_query()
    ddb_table.put_item(Item={"pk": "SITES", "sk": "my-site", "domain": "old.com"})

    event = make_event(
        {"id": "my-site", "domain": "new.com"},
        method="PUT",
        path="/api/sites",
    )
    result = query.handler(event, None)
    assert result["statusCode"] == 200

    item = ddb_table.get_item(Key={"pk": "SITES", "sk": "my-site"}).get("Item")
    assert item["domain"] == "new.com"


def test_get_stats_with_new_fields(ddb_table):
    """Verify browsers, oses, languages, UTM, and entry/exit pages in query response."""
    query = load_query()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    ddb_table.put_item(Item={
        "pk": "SUMMARY#test-site",
        "sk": today,
        "pageviews": 10,
        "visitors": {"v1"},
        "sessions": {"v1#s1"},
        "paths": {"/": 10},
        "countries": {"US": 10},
        "devices": {"desktop": 10},
        "referrers": {},
        "browsers": {"Chrome": 7, "Firefox": 3},
        "oses": {"Windows": 6, "macOS": 4},
        "languages": {"en-US": 8, "es": 2},
        "utm_sources": {"google": 3},
        "utm_mediums": {"cpc": 3},
        "utm_campaigns": {"spring": 3},
        "ttl": 9999999999,
    })

    # Add session records with entry/exit pages
    ddb_table.put_item(Item={
        "pk": f"SESSION#test-site#{today}",
        "sk": "v1#s1",
        "duration": 120,
        "entry_page": "/home",
        "exit_page": "/pricing",
        "ttl": 9999999999,
    })

    event = make_event({}, method="GET", path="/api/query")
    event["queryStringParameters"] = {"site_id": "test-site", "days": "1"}
    result = query.handler(event, None)
    body = json.loads(result["body"])

    assert result["statusCode"] == 200
    assert len(body["browsers"]) == 2
    assert body["browsers"][0]["name"] == "Chrome"
    assert body["browsers"][0]["count"] == 7
    assert len(body["oses"]) == 2
    assert len(body["languages"]) == 2
    assert body["utmSources"][0]["name"] == "google"
    assert body["utmMediums"][0]["name"] == "cpc"
    assert body["utmCampaigns"][0]["name"] == "spring"
    assert body["entryPages"][0]["path"] == "/home"
    assert body["exitPages"][0]["path"] == "/pricing"


def test_get_live_empty(ddb_table):
    query = load_query()
    event = make_event({}, method="GET", path="/api/live")
    event["queryStringParameters"] = {"site_id": "test-site"}
    result = query.handler(event, None)
    body = json.loads(result["body"])
    assert result["statusCode"] == 200
    assert body["liveVisitors"] == 0
