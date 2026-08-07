import sqlite3
import json
from ghost_score import init_db, record_observation, score_all, compute_posting_key


def make_posting(title, company, description, external_id=None, posted_date=None,
                 source="remoteok", salary=None, location=None, url=None):
    d = {
        "title": title,
        "company": company,
        "description": description,
        "external_id": external_id,
        "posted_date": posted_date,
        "source": source,
        "salary": salary,
        "url": url,
        "location": location,
    }
    return d


def test_record_observation_and_score_all_retitle_and_counts():
    conn = sqlite3.connect(":memory:")
    init_db(conn)

    # Two different titles but identical long description -> triggers GH-009 retitle
    long_desc = "This is a long description " + ("x" * 500)
    p1 = make_posting("Software Engineer I", "Acme Inc", long_desc, external_id="a1",
                      posted_date="2026-06-01", source="remoteok")
    p2 = make_posting("Junior Developer", "Acme Inc", long_desc, external_id="a2",
                      posted_date="2026-06-02", source="remoteok")

    k1 = record_observation(conn, p1, observed_at="2026-07-01")
    k2 = record_observation(conn, p2, observed_at="2026-07-02")

    assert k1 and k2 and k1 != k2

    n = score_all(conn, today="2026-08-07")
    assert n == 2

    rows = conn.execute("SELECT posting_key, score, verdict, tags, reasons FROM ghost_scores").fetchall()
    assert len(rows) == 2

    # At least one of the scored rows should include GH-009 in reasons
    any_retitle = False
    for _, score, verdict, tags, reasons in rows:
        rs = json.loads(reasons)
        if any(r.get("rule") == "GH-009" for r in rs):
            any_retitle = True
    assert any_retitle


def test_record_observation_repost_and_floor_behavior():
    conn = sqlite3.connect(":memory:")
    init_db(conn)

    desc = "A normal-ish description " + ("y" * 500)
    posting = make_posting("Support Engineer", "Beta LLC", desc, external_id="id1",
                           posted_date="2026-07-01", source="remoteok")

    # First observation
    key = record_observation(conn, posting, observed_at="2026-07-01")
    assert key

    # Second observation with a different external_id and a different claimed date
    posting2 = posting.copy()
    posting2["external_id"] = "id2"
    posting2["posted_date"] = "2026-07-20"
    record_observation(conn, posting2, observed_at="2026-07-15")

    # Now score
    n = score_all(conn, today="2026-08-07")
    assert n == 1

    row = conn.execute("SELECT posting_key, score, verdict, tags, reasons FROM ghost_scores").fetchone()
    assert row is not None
    reasons = json.loads(row[4])
    # GH-002 should appear for multiple external ids
    assert any(r.get("rule") == "GH-002" for r in reasons)
    # FLOOR should appear because two distinct claimed dates exist
    assert any(r.get("rule") == "FLOOR" for r in reasons)
