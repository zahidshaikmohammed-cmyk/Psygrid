from rbi_news import _parse_rss


SAMPLE_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
<channel>
<title>RBI Press Releases</title>
<item>
<title>RBI announces monetary policy</title>
<link>https://www.rbi.org.in/press/12345</link>
<description>Summary text here</description>
<pubDate>Sat, 19 Sep 2026 10:00:00 GMT</pubDate>
<guid>https://www.rbi.org.in/press/12345</guid>
</item>
</channel>
</rss>
"""


def test_parses_rss_items_with_all_fields():
    items = _parse_rss(SAMPLE_RSS, "press_releases")
    assert len(items) == 1
    item = items[0]
    assert item["headline"] == "RBI announces monetary policy"
    assert item["source"] == "RBI_OFFICIAL"
    assert item["category"] == "press_releases"
    assert item["url"] == "https://www.rbi.org.in/press/12345"
    assert item["country"] == "IN"
    assert item["published_at"] is not None


def test_never_classifies_bullish_bearish():
    items = _parse_rss(SAMPLE_RSS, "press_releases")
    import json
    text = json.dumps(items).upper()
    for forbidden in ("BULLISH", "BEARISH", "SIGNAL", "BUY", "SELL"):
        assert forbidden not in text


def test_malformed_xml_returns_empty_not_crash():
    assert _parse_rss("<not valid xml", "press_releases") == []
