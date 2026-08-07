from ghost_score import (
    sig_date_refresh, sig_repost, sig_days_open, sig_aggregator_only,
    sig_evergreen_language, sig_no_salary, sig_vague_description,
    sig_requirements_mismatch, tag_staffing, tag_perm, score_posting,
    DATE_GAMING_FLOOR, WEIGHTS
)


def make_base():
    return {
        'posting_key': 'k',
        'title': 'Software Engineer',
        'company': 'Acme Corp',
        'first_seen': '2026-07-01',
        'last_seen': '2026-07-01',
        'times_seen': 1,
        'external_ids': '[]',
        'claimed_dates': '[]',
        'sources': '[]',
        'desc_hashes': '[]',
        'last_salary': None,
        'last_desc_len': 500,
        'last_desc_text': 'A detailed description.' ,
        'last_url': ''
    }


def test_sig_date_refresh_true():
    t = make_base()
    t['claimed_dates'] = '["2026-07-15"]'
    t['first_seen'] = '2026-07-01'
    fired, ev = sig_date_refresh(t)
    assert fired and 'posted' in ev


def test_sig_repost_true():
    t = make_base()
    t['external_ids'] = '["a","b"]'
    fired, ev = sig_repost(t)
    assert fired and 'distinct posting IDs' in ev


def test_sig_days_open_bands():
    t = make_base()
    # days > 60
    rule, ev = sig_days_open({'first_seen': '2026-05-01'}, '2026-08-07')
    assert rule == 'GH-003c'
    # days > 45
    rule, ev = sig_days_open({'first_seen': '2026-06-01'}, '2026-08-07')
    assert rule == 'GH-003b'
    # days > 30
    rule, ev = sig_days_open({'first_seen': '2026-06-20'}, '2026-08-07')
    assert rule == 'GH-003a'


def test_sig_aggregator_only_true():
    t = make_base()
    t['sources'] = '["remoteok"]'
    fired, ev = sig_aggregator_only(t)
    assert fired and 'aggregator-only' in ev


def test_sig_evergreen_and_tags():
    t = make_base()
    t['last_desc_text'] = 'We are always hiring and building a talent pipeline.'
    fired, ev = sig_evergreen_language(t)
    assert fired and 'evergreen' in ev
    # staffing tag
    t['company'] = 'Acme Recruiting'
    assert tag_staffing(t)


def test_sig_no_salary_and_vague_description():
    t = make_base()
    t['last_salary'] = None
    t['last_desc_len'] = 100
    fired, _ = sig_no_salary(t)
    assert fired
    fired, _ = sig_vague_description(t)
    assert fired


def test_sig_requirements_mismatch():
    t = make_base()
    t['title'] = 'Entry-level Analyst'
    t['last_desc_text'] = 'Candidates must have CISSP certification.'
    fired, ev = sig_requirements_mismatch(t)
    assert fired and 'CISSP' in ev


def test_tag_perm_true():
    t = make_base()
    t['last_desc_text'] = 'This role mentions PERM and labor certification processes.'
    assert tag_perm(t)


def test_score_floor_on_claimed_dates():
    t = make_base()
    # no other signals, but 2 claimed dates should floor the score
    t['claimed_dates'] = '["2026-07-20","2026-07-01"]'
    score, verdict, tags, reasons = score_posting(t, today='2026-08-07')
    assert score >= DATE_GAMING_FLOOR
    assert any(r.get('rule') == 'FLOOR' for r in reasons)


def test_score_sum_of_weights():
    t = make_base()
    # trigger repost + no salary + vague description
    t['external_ids'] = '["a","b"]'
    t['last_salary'] = None
    t['last_desc_len'] = 100
    score, verdict, tags, reasons = score_posting(t, today='2026-08-07')
    expected = WEIGHTS['GH-002'] + WEIGHTS['GH-006'] + WEIGHTS['GH-007']
    assert any(r['rule'] == 'GH-002' for r in reasons)
    assert score == expected
