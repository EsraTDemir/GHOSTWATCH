import re
from ghost_score import compute_posting_key, content_hash, score_posting


def test_compute_posting_key_consistent():
    k1 = compute_posting_key('Software Engineer', 'Acme Corp', 'Remote')
    k2 = compute_posting_key('Software Engineer', 'Acme Corp', 'Remote')
    assert isinstance(k1, str) and len(k1) == 20
    assert k1 == k2


def test_content_hash_differs():
    h1 = content_hash('hello world')
    h2 = content_hash('hello world!')
    assert isinstance(h1, str) and len(h1) == 20
    assert h1 != h2


def test_score_posting_basic():
    t = {
        'posting_key': 'k',
        'title': 'Entry Level Analyst',
        'company': 'Acme',
        'first_seen': '2026-06-01',
        'last_seen': '2026-06-01',
        'times_seen': 1,
        'external_ids': '[]',
        'claimed_dates': '[]',
        'sources': '[]',
        'desc_hashes': '[]',
        'last_salary': None,
        'last_desc_len': 100,
        'last_desc_text': 'This is an entry-level position requiring no prior experience.',
        'last_url': ''
    }
    score, verdict, tags, reasons = score_posting(t, today='2026-08-07')
    assert isinstance(score, int) and 0 <= score <= 100
    assert isinstance(verdict, str)
    assert isinstance(tags, list)
    assert isinstance(reasons, list)
