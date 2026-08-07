# Extracted from GhostWatch.ipynb
# Verify and tidy before production use


# ============================================================
#  GHOSTWATCH  --  just press the play button (round arrow, top-left)
#  You never need to read or edit the code below.
#
#  (Optional, later: the company watchlist you can edit lives
#   right here.)
# ============================================================

WATCHLIST = {
    "greenhouse": ["cloudflare", "datadog", "elastic", "gitlab", "figma"],
    "lever": ["1password"],
    "ashby": [],
}

#!/usr/bin/env python3
"""
ghost_score.py — longitudinal ghost-job detection for the job-search pipeline.

Design: detection-as-code. Deterministic rules only (no LLM calls), each with
an ID, a weight, and an evidence string, so every verdict ships with its
reasons — like closing a ticket with notes. Scores are 0-100.

Verdicts:
    active       (0-24)   nothing notable
    watch        (25-49)  aging or thin posting; deprioritize below fresh ones
    likely_ghost (50-74)  multiple corroborating signals
    ghost        (75-100) strong longitudinal evidence
Tags (categorical, independent of score):
    evergreen    staffing-style always-open role; real but rolling
    compliance   PERM-style legally mandated ad; not fillable by you

Integration (two lines in each poller, after dedupe):
    from ghost_score import record_observation
    record_observation(conn, posting_dict)

CLI:
    python ghost_score.py --db jobs.db --init
    python ghost_score.py --db jobs.db --backfill --table postings \
        --title-col title --company-col company --date-col scraped_at \
        [--source-col source] [--url-col url] [--posted-col posted_date] \
        [--salary-col salary] [--desc-col description] [--extid-col external_id]
    python ghost_score.py --db jobs.db --score
    python ghost_score.py --db jobs.db --report
"""

import argparse
import hashlib
import json
import re
import sqlite3
import sys
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Tunables — adjust weights here, nowhere else.
# ---------------------------------------------------------------------------

WEIGHTS = {
    "GH-001": 35,  # date refresh (claimed posted-date younger than our first_seen)
    "GH-002": 30,  # repost cycle (same posting identity, new external id)
    "GH-002b": 10, # additional repost cycles beyond the first
    "GH-003a": 15, # open 31-45 days
    "GH-003b": 25, # open 46-60 days
    "GH-003c": 35, # open 60+ days
    "GH-004": 25,  # aggregator-only: absent from company's own ATS board
    "GH-005": 15,  # evergreen/pipeline language
    "GH-006": 3,   # no salary information (weak: ~80% of postings omit it)
    "GH-007": 7,   # vague / very short description
    "GH-008": 12,  # requirements mismatch (entry-level + senior demands)
    "GH-009": 25,  # retitle repost (identical description, different title)
}

VERDICT_BANDS = [(75, "ghost"), (50, "likely_ghost"), (25, "watch"), (0, "active")]

# Two or more distinct claimed posted-dates for the same content = active date
# gaming; floor the score at likely_ghost regardless of other signals.
DATE_GAMING_FLOOR = 50

EVERGREEN_PATTERNS = [
    r"always (accepting|hiring|looking)",
    r"talent (community|network|pool|pipeline)",
    r"future (openings|opportunities|roles)",
    r"general application",
    r"ongoing basis",
    r"evergreen",
    r"join our (network|pipeline)",
]

STAFFING_PATTERNS = [
    r"\bstaffing\b", r"\brecruiting\b", r"\brecruitment\b", r"\btalent\b",
    r"\bworkforce\b", r"\bconsultants?\b", r"\bsolutions\b", r"\bsearch group\b",
]

PERM_PATTERNS = [
    r"\bPERM\b",
    r"labor certification",
    r"mail (your )?(resume|résumé|cv)",
    r"reference (job|req(uisition)?) ?(code|number|#)",
    r"send (resume|résumé|cv) to .*(hr|human resources)",
]

ENTRY_LEVEL_PATTERNS = [
    r"entry[- ]level",
    r"\bjunior\b",
    r"\b0\s*(?:-|–|to)\s*[12]\s*years?\b",
    r"early[- ]career",
]

# Certs that themselves require years of experience (CISSP alone needs five).
ADVANCED_CERT_PATTERN = r"\b(CISSP|CISM|CISA|CCSP|CRISC|OSCP|OSEP|CCIE|GSE)\b"
SENIOR_YEARS_PATTERN = r"\b([5-9]|1[0-9])\s*\+?\s*years?\b"

MIN_DESC_CHARS = 400


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------

def _norm(s):
    return re.sub(r"[^a-z0-9 ]", "", (s or "").lower()).strip()


def compute_posting_key(title, company, location=None):
    """Stable identity for 'the same role at the same company'.
    External IDs change on reposts; this key does not — that gap is the signal.
    """
    basis = "|".join([_norm(title), _norm(company), _norm(location or "remote")])
    return hashlib.sha256(basis.encode()).hexdigest()[:20]


def content_hash(description):
    return hashlib.sha256(_norm(description or "").encode()).hexdigest()[:20]


# ---------------------------------------------------------------------------
# Schema (additive — touches nothing in the existing pipeline tables)
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS posting_tracking (
    posting_key     TEXT PRIMARY KEY,
    title           TEXT,
    company         TEXT,
    first_seen      TEXT NOT NULL,
    last_seen       TEXT NOT NULL,
    times_seen      INTEGER NOT NULL DEFAULT 1,
    external_ids    TEXT NOT NULL DEFAULT '[]',   -- JSON list
    claimed_dates   TEXT NOT NULL DEFAULT '[]',   -- JSON list of distinct posted-dates
    sources         TEXT NOT NULL DEFAULT '[]',   -- JSON list
    desc_hashes     TEXT NOT NULL DEFAULT '[]',   -- JSON list
    last_salary     TEXT,
    last_desc_len   INTEGER,
    last_desc_text  TEXT,
    last_url        TEXT
);
CREATE TABLE IF NOT EXISTS ghost_scores (
    posting_key TEXT PRIMARY KEY,
    score       INTEGER NOT NULL,
    verdict     TEXT NOT NULL,
    tags        TEXT NOT NULL,
    reasons     TEXT NOT NULL,   -- JSON list of {rule, weight, evidence}
    scored_at   TEXT NOT NULL
);
"""

# Company boards the ats_poller watches directly (Greenhouse/Lever/Ashby).
# Used by GH-004: if a company is watchable but the posting only ever shows
# up on aggregators, that absence is evidence.
WATCHLIST_SOURCES = {"greenhouse", "lever", "ashby"}
AGGREGATOR_SOURCES = {"remoteok", "remotive", "themuse", "adzuna", "usajobs"}


def init_db(conn):
    conn.executescript(DDL)
    conn.commit()


# ---------------------------------------------------------------------------
# Observation recording — call this from both pollers after dedupe
# ---------------------------------------------------------------------------

# Field aliases so the pollers can pass their dicts as-is. Edit to match yours.
FIELD_MAP = {
    "title":       ["title", "job_title", "position"],
    "company":     ["company", "company_name", "employer"],
    "location":    ["location", "job_location"],
    "source":      ["source", "board", "site"],
    "external_id": ["external_id", "job_id", "id", "req_id"],
    "posted_date": ["posted_date", "date_posted", "posted", "created_at"],
    "salary":      ["salary", "salary_range", "compensation", "pay"],
    "description": ["description", "job_description", "desc", "text"],
    "url":         ["url", "link", "job_url"],
}


def _pick(d, field):
    for k in FIELD_MAP[field]:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None


def record_observation(conn, posting, observed_at=None):
    """Upsert one sighting of a posting into posting_tracking.

    `posting` is a dict from either poller; unknown keys are ignored and
    missing keys degrade gracefully. `observed_at` (ISO date) exists for
    backfill and tests; defaults to now.
    """
    title = _pick(posting, "title")
    company = _pick(posting, "company")
    if not title or not company:
        return None  # can't build an identity; skip quietly

    key = compute_posting_key(title, company, _pick(posting, "location"))
    now = observed_at or datetime.now(timezone.utc).date().isoformat()
    source = str(_pick(posting, "source") or "unknown").lower()
    ext_id = _pick(posting, "external_id")
    claimed = _pick(posting, "posted_date")
    claimed = str(claimed)[:10] if claimed else None
    salary = _pick(posting, "salary")
    desc = _pick(posting, "description") or ""
    dh = content_hash(desc)
    url = _pick(posting, "url")

    row = conn.execute(
        "SELECT external_ids, claimed_dates, sources, desc_hashes, first_seen,"
        " times_seen, last_seen FROM posting_tracking WHERE posting_key=?", (key,)
    ).fetchone()

    if row is None:
        conn.execute(
            "INSERT INTO posting_tracking (posting_key, title, company,"
            " first_seen, last_seen, times_seen, external_ids, claimed_dates,"
            " sources, desc_hashes, last_salary, last_desc_len, last_desc_text,"
            " last_url) VALUES (?,?,?,?,?,1,?,?,?,?,?,?,?,?)",
            (key, title, company, now, now,
             json.dumps([ext_id] if ext_id else []),
             json.dumps([claimed] if claimed else []),
             json.dumps([source]),
             json.dumps([dh]),
             str(salary) if salary else None, len(desc), desc[:2000], url),
        )
    else:
        ext_ids = json.loads(row[0])
        dates = json.loads(row[1])
        sources = json.loads(row[2])
        hashes = json.loads(row[3])
        first_seen = min(row[4], now)  # backfill may arrive out of order
        if ext_id and ext_id not in ext_ids:
            ext_ids.append(ext_id)
        if claimed and claimed not in dates:
            dates.append(claimed)
        if source not in sources:
            sources.append(source)
        if dh not in hashes:
            hashes.append(dh)
        conn.execute(
            "UPDATE posting_tracking SET last_seen=?, first_seen=?,"
            " times_seen=?, external_ids=?, claimed_dates=?, sources=?,"
            " desc_hashes=?, last_salary=?, last_desc_len=?, last_desc_text=?,"
            " last_url=? WHERE posting_key=?",
            (max(now, row[6]), first_seen, row[5] + 1,
             json.dumps(ext_ids), json.dumps(dates), json.dumps(sources),
             json.dumps(hashes), str(salary) if salary else None, len(desc),
             desc[:2000], url, key),
        )
    conn.commit()
    return key


# ---------------------------------------------------------------------------
# Signals — each returns (fired: bool, evidence: str)
# ---------------------------------------------------------------------------

def _days_between(a, b):
    try:
        return (datetime.fromisoformat(b) - datetime.fromisoformat(a)).days
    except (ValueError, TypeError):
        return 0


def sig_date_refresh(t):
    """GH-001: any claimed posted-date is younger than our own first_seen.
    We watched it exist before it claims to have been posted."""
    dates = json.loads(t["claimed_dates"])
    stale = [d for d in dates if d and d > t["first_seen"]]
    if stale:
        return True, (f"claims posted {max(stale)} but first observed "
                      f"{t['first_seen']} — posted-date refreshed")
    return False, ""


def sig_repost(t):
    """GH-002: same role identity seen under multiple external IDs."""
    n = len(json.loads(t["external_ids"]))
    if n >= 2:
        return True, f"{n} distinct posting IDs for the same role"
    return False, ""


def sig_days_open(t, today):
    days = _days_between(t["first_seen"], today)
    if days > 60:
        return "GH-003c", f"open {days} days (60+)"
    if days > 45:
        return "GH-003b", f"open {days} days (46-60)"
    if days > 30:
        return "GH-003a", f"open {days} days (31-45)"
    return None, ""


def sig_aggregator_only(t):
    """GH-004: seen on aggregators, never on a directly-watched company board."""
    sources = set(json.loads(t["sources"]))
    if sources & AGGREGATOR_SOURCES and not sources & WATCHLIST_SOURCES:
        return True, f"aggregator-only presence ({', '.join(sorted(sources))})"
    return False, ""


def sig_evergreen_language(t):
    text = (t["last_desc_text"] or "") + " " + (t["title"] or "")
    for p in EVERGREEN_PATTERNS:
        m = re.search(p, text, re.I)
        if m:
            return True, f"evergreen language: '{m.group(0)}'"
    return False, ""


def sig_no_salary(t):
    if not t["last_salary"]:
        return True, "no salary information"
    return False, ""


def sig_vague_description(t):
    if (t["last_desc_len"] or 0) < MIN_DESC_CHARS and t["last_desc_len"] is not None:
        return True, f"description only {t['last_desc_len']} chars"
    return False, ""


def sig_requirements_mismatch(t):
    """GH-008: entry-level framing paired with senior-level demands —
    e.g. an 'entry level' role requiring CISSP, a cert that itself
    requires five years of experience."""
    text = (t["title"] or "") + " " + (t["last_desc_text"] or "")
    entry = None
    for p in ENTRY_LEVEL_PATTERNS:
        m = re.search(p, text, re.I)
        if m:
            entry = m.group(0)
            break
    if not entry:
        return False, ""
    cert = re.search(ADVANCED_CERT_PATTERN, text)
    if cert:
        return True, f"'{entry}' role requiring {cert.group(0)}"
    years = re.search(SENIOR_YEARS_PATTERN, text)
    if years:
        return True, f"'{entry}' role requiring {years.group(0).strip()}"
    return False, ""


def tag_staffing(t):
    for p in STAFFING_PATTERNS:
        if re.search(p, t["company"] or "", re.I):
            return True
    return False


def tag_perm(t):
    text = t["last_desc_text"] or ""
    hits = [p for p in PERM_PATTERNS if re.search(p, text, re.I)]
    return len(hits) >= 2


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def score_posting(t, today=None, retitle=None):
    today = today or datetime.now(timezone.utc).date().isoformat()
    reasons, score, tags = [], 0, []

    def add(rule, evidence):
        nonlocal score
        score += WEIGHTS[rule]
        reasons.append({"rule": rule, "weight": WEIGHTS[rule], "evidence": evidence})

    fired, ev = sig_date_refresh(t)
    if fired:
        add("GH-001", ev)
    fired, ev = sig_repost(t)
    if fired:
        add("GH-002", ev)
        extra = len(json.loads(t["external_ids"])) - 2
        if extra > 0:
            add("GH-002b", f"{extra} additional repost cycle(s)")
    rule, ev = sig_days_open(t, today)
    if rule:
        add(rule, ev)
    fired, ev = sig_aggregator_only(t)
    if fired:
        add("GH-004", ev)
    fired, ev = sig_evergreen_language(t)
    if fired:
        add("GH-005", ev)
    fired, ev = sig_no_salary(t)
    if fired:
        add("GH-006", ev)
    fired, ev = sig_vague_description(t)
    if fired:
        add("GH-007", ev)
    fired, ev = sig_requirements_mismatch(t)
    if fired:
        add("GH-008", ev)
    if retitle:
        add("GH-009", retitle)

    # Hard floor: multiple distinct claimed posted-dates = active date gaming.
    if len(json.loads(t["claimed_dates"])) >= 2 and score < DATE_GAMING_FLOOR:
        score = DATE_GAMING_FLOOR
        reasons.append({"rule": "FLOOR", "weight": 0,
                        "evidence": "2+ distinct claimed posted-dates"})

    score = min(score, 100)

    if tag_staffing(t) and sig_evergreen_language(t)[0]:
        tags.append("evergreen")
    if tag_perm(t):
        tags.append("compliance")

    verdict = next(v for cutoff, v in VERDICT_BANDS if score >= cutoff)
    # Evergreen and compliance postings aren't deceptive in the same way;
    # cap the named verdict so the tag carries the meaning.
    if tags and verdict in ("ghost", "likely_ghost"):
        verdict = "watch"
    return score, verdict, tags, reasons


def score_all(conn, today=None):
    rows = conn.execute("SELECT * FROM posting_tracking").fetchall()
    cols = [d[0] for d in conn.execute("SELECT * FROM posting_tracking LIMIT 1").description]
    dicts = [dict(zip(cols, r)) for r in rows]

    # Cross-key retitle detection (GH-009): the same substantial description
    # under different role identities at one company evades GH-002 by design.
    # Short/empty descriptions are excluded to avoid hash collisions on noise.
    EMPTY = content_hash("")
    by_hash = {}
    for t in dicts:
        if (t["last_desc_len"] or 0) < 200:
            continue
        for h in json.loads(t["desc_hashes"]):
            if h != EMPTY:
                by_hash.setdefault((_norm(t["company"]), h), set()).add(t["posting_key"])

    n = 0
    for t in dicts:
        retitle = None
        if (t["last_desc_len"] or 0) >= 200:
            others = set()
            for h in json.loads(t["desc_hashes"]):
                others |= by_hash.get((_norm(t["company"]), h), set())
            others.discard(t["posting_key"])
            if others:
                retitle = (f"identical description under {len(others) + 1}"
                           f" distinct role identities at this company")
        score, verdict, tags, reasons = score_posting(t, today, retitle)
        conn.execute(
            "INSERT INTO ghost_scores (posting_key, score, verdict, tags,"
            " reasons, scored_at) VALUES (?,?,?,?,?,?)"
            " ON CONFLICT(posting_key) DO UPDATE SET score=excluded.score,"
            " verdict=excluded.verdict, tags=excluded.tags,"
            " reasons=excluded.reasons, scored_at=excluded.scored_at",
            (t["posting_key"], score, verdict, json.dumps(tags),
             json.dumps(reasons),
             today or datetime.now(timezone.utc).date().isoformat()),
        )
        n += 1
    conn.commit()
    return n


# ---------------------------------------------------------------------------
# Measurement report — aggregates only; company names stay in your dashboard
# ---------------------------------------------------------------------------

def report(conn):
    total = conn.execute("SELECT COUNT(*) FROM ghost_scores").fetchone()[0]
    if not total:
        print("No scored postings yet. Run --score first.")
        return
    print(f"postings scored:        {total}")
    for cutoff, name in VERDICT_BANDS:
        n = conn.execute(
            "SELECT COUNT(*) FROM ghost_scores WHERE verdict=?", (name,)
        ).fetchone()[0]
        print(f"  {name:<13} {n:>5}  ({100 * n / total:.1f}%)")
    ghosty = conn.execute(
        "SELECT COUNT(*) FROM ghost_scores WHERE verdict IN"
        " ('ghost','likely_ghost')").fetchone()[0]
    tagged = conn.execute(
        "SELECT COUNT(*) FROM ghost_scores WHERE tags != '[]'").fetchone()[0]
    print(f"ghost rate (verdict-based): {100 * ghosty / total:.1f}%"
          f"   (excludes {tagged} evergreen/compliance-tagged)")

    print("\nby source (share of that source with a ghost verdict):")
    rows = conn.execute("SELECT posting_key, sources FROM posting_tracking").fetchall()
    src_tot, src_ghost = {}, {}
    verdicts = dict(conn.execute("SELECT posting_key, verdict FROM ghost_scores"))
    for key, srcs in rows:
        for s in json.loads(srcs):
            src_tot[s] = src_tot.get(s, 0) + 1
            if verdicts.get(key) in ("ghost", "likely_ghost"):
                src_ghost[s] = src_ghost.get(s, 0) + 1
    for s in sorted(src_tot, key=lambda x: -src_tot[x]):
        pct = 100 * src_ghost.get(s, 0) / src_tot[s]
        print(f"  {s:<12} {src_tot[s]:>5} tracked   {pct:.1f}% ghost-scored")

    print("\ntop signals fired:")
    counts = {}
    for (reasons,) in conn.execute("SELECT reasons FROM ghost_scores"):
        for r in json.loads(reasons):
            counts[r["rule"]] = counts.get(r["rule"], 0) + 1
    for rule, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  {rule:<8} {n}")


# ---------------------------------------------------------------------------
# Backfill from an existing pipeline table
# ---------------------------------------------------------------------------


#!/usr/bin/env python3
"""
collector.py -- fetches job postings from public, key-free APIs and records
each sighting into the ghostwatch database.

Sources (all official public endpoints; no login, no API keys, no scraping):
  - Greenhouse job boards   (companies listed in watchlist.json)
  - Lever job boards        (companies listed in watchlist.json)
  - Ashby job boards        (companies listed in watchlist.json)
  - RemoteOK                (public API, filtered by keywords)
  - Remotive                (public API, searched by keywords)

Postings are kept only if the title matches KEYWORDS below. Edit the list
to widen or narrow what gets tracked.
"""

import json
import re
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# What to track -- edit freely
# ---------------------------------------------------------------------------

KEYWORDS = [
    "security", "soc ", "soc,", "incident", "threat", "detection",
    "vulnerability", "cyber", "infosec", "siem", "forensic", "grc",
]

USER_AGENT = "ghostwatch/0.1 (personal job-search tool)"
TIMEOUT = 20
PAUSE_BETWEEN_CALLS = 1.0  # seconds; be a polite guest


def title_matches(title):
    t = " " + (title or "").lower() + " "
    return any(k in t for k in KEYWORDS)


# ---------------------------------------------------------------------------
# Fetch helper
# ---------------------------------------------------------------------------

def fetch_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def strip_html(text):
    import html
    text = html.unescape(text or "")
    return re.sub(r"<[^>]+>", " ", text)


def load_watchlist():
    path = Path(__file__).with_name("watchlist.json")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"greenhouse": [], "lever": [], "ashby": []}


# ---------------------------------------------------------------------------
# Per-source normalizers: each returns a list of posting dicts
# ---------------------------------------------------------------------------

def parse_greenhouse(token, data):
    out = []
    for j in data.get("jobs", []):
        out.append({
            "title": j.get("title"),
            "company": token,
            "location": (j.get("location") or {}).get("name"),
            "source": "greenhouse",
            "external_id": str(j.get("id")),
            "posted_date": j.get("updated_at"),  # best available publicly
            "salary": None,
            "description": strip_html(j.get("content", "")),
            "url": j.get("absolute_url"),
        })
    return out


def parse_lever(token, data):
    out = []
    for j in data if isinstance(data, list) else []:
        created = j.get("createdAt")
        if isinstance(created, (int, float)):
            created = datetime.fromtimestamp(
                created / 1000, tz=timezone.utc).date().isoformat()
        cats = j.get("categories") or {}
        out.append({
            "title": j.get("text"),
            "company": token,
            "location": cats.get("location"),
            "source": "lever",
            "external_id": str(j.get("id")),
            "posted_date": created,
            "salary": None,
            "description": j.get("descriptionPlain")
                           or strip_html(j.get("description", "")),
            "url": j.get("hostedUrl"),
        })
    return out


def parse_ashby(token, data):
    out = []
    for j in data.get("jobs", []):
        out.append({
            "title": j.get("title"),
            "company": token,
            "location": j.get("location"),
            "source": "ashby",
            "external_id": str(j.get("id")),
            "posted_date": j.get("publishedAt"),
            "salary": j.get("compensationTierSummary"),
            "description": j.get("descriptionPlain")
                           or strip_html(j.get("descriptionHtml", "")),
            "url": j.get("jobUrl"),
        })
    return out


def parse_remoteok(data):
    out = []
    rows = data if isinstance(data, list) else []
    for j in rows:
        if not isinstance(j, dict) or "position" not in j:
            continue  # first element is a legal notice
        title = j.get("position")
        if not title_matches(title):
            continue
        smin, smax = j.get("salary_min"), j.get("salary_max")
        salary = f"{smin}-{smax}" if smin or smax else None
        out.append({
            "title": title,
            "company": j.get("company"),
            "location": j.get("location") or "Remote",
            "source": "remoteok",
            "external_id": str(j.get("id")),
            "posted_date": j.get("date"),
            "salary": salary,
            "description": strip_html(j.get("description", "")),
            "url": j.get("url"),
        })
    return out


def parse_remotive(data):
    out = []
    for j in data.get("jobs", []):
        title = j.get("title")
        if not title_matches(title):
            continue
        out.append({
            "title": title,
            "company": j.get("company_name"),
            "location": j.get("candidate_required_location"),
            "source": "remotive",
            "external_id": str(j.get("id")),
            "posted_date": j.get("publication_date"),
            "salary": j.get("salary"),
            "description": strip_html(j.get("description", "")),
            "url": j.get("url"),
        })
    return out


# ---------------------------------------------------------------------------
# Collection run
# ---------------------------------------------------------------------------

def collect_all(conn, log=print):
    wl = load_watchlist()
    recorded = 0
    status = []

    def record_batch(postings, label, keyword_filter=True):
        nonlocal recorded
        kept = 0
        for p in postings:
            if keyword_filter and not title_matches(p.get("title")):
                continue
            if record_observation(conn, p):
                kept += 1
        recorded += kept
        status.append((label, "ok", len(postings), kept))

    for token in wl.get("greenhouse", []):
        url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"
        try:
            record_batch(parse_greenhouse(token, fetch_json(url)),
                         f"greenhouse/{token}")
        except Exception as e:
            status.append((f"greenhouse/{token}", f"FAILED: {e}", 0, 0))
        time.sleep(PAUSE_BETWEEN_CALLS)

    for token in wl.get("lever", []):
        url = f"https://api.lever.co/v0/postings/{token}?mode=json"
        try:
            record_batch(parse_lever(token, fetch_json(url)),
                         f"lever/{token}")
        except Exception as e:
            status.append((f"lever/{token}", f"FAILED: {e}", 0, 0))
        time.sleep(PAUSE_BETWEEN_CALLS)

    for token in wl.get("ashby", []):
        url = f"https://api.ashbyhq.com/posting-api/job-board/{token}?includeCompensation=true"
        try:
            record_batch(parse_ashby(token, fetch_json(url)),
                         f"ashby/{token}")
        except Exception as e:
            status.append((f"ashby/{token}", f"FAILED: {e}", 0, 0))
        time.sleep(PAUSE_BETWEEN_CALLS)

    try:
        record_batch(parse_remoteok(fetch_json("https://remoteok.com/api")),
                     "remoteok", keyword_filter=False)
    except Exception as e:
        status.append(("remoteok", f"FAILED: {e}", 0, 0))
    time.sleep(PAUSE_BETWEEN_CALLS)

    try:
        record_batch(
            parse_remotive(fetch_json(
                "https://remotive.com/api/remote-jobs?search=security")),
            "remotive", keyword_filter=False)
    except Exception as e:
        status.append(("remotive", f"FAILED: {e}", 0, 0))

    log("")
    log("source status:")
    for label, state, seen, kept in status:
        if state == "ok":
            log(f"  {label:<24} ok    {seen:>4} postings, {kept:>3} matched keywords")
        else:
            log(f"  {label:<24} {state}")
    return recorded


# ---- notebook runner ------------------------------------------------------

def load_watchlist():  # notebook version: use the WATCHLIST at the top
    return WATCHLIST


BASE_DIR = "/content/drive/MyDrive/GhostWatch"


def colab_main():
    global BASE_DIR
    import io, os, contextlib
    print("=" * 60)
    print("GHOSTWATCH -- collecting and scoring today's postings")
    print("=" * 60)
    try:
        try:
    from google.colab import drive
except Exception:
    drive = None  # noqa
        print("\nConnecting to your Google Drive (a permission box will")
        print("pop up the first time -- click 'Connect to Google Drive',")
        print("pick your account, then Allow/Continue)...\n")
        drive.mount("/content/drive")
    except ImportError:
        BASE_DIR = "./GhostWatch"
        print("(not running in Colab -- saving to a local folder instead)")
    os.makedirs(BASE_DIR, exist_ok=True)

    conn = sqlite3.connect(os.path.join(BASE_DIR, "jobs.db"))
    init_db(conn)

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        recorded = collect_all(conn)
        print(f"\nrecorded/updated {recorded} matching postings")
        scored = score_all(conn)
        print(f"scored {scored} tracked postings\n")
        report(conn)
        first = conn.execute(
            "SELECT MIN(first_seen) FROM posting_tracking").fetchone()[0]
        if first:
            today = datetime.now(timezone.utc).date().isoformat()
            try:
                days = (datetime.fromisoformat(today)
                        - datetime.fromisoformat(first)).days + 1
            except ValueError:
                days = 1
            print(f"\nday {days} of data collection.")
            if days < 14:
                print("note: the strongest rules (reposts, refreshed dates,")
                print("staleness) need time to observe -- scores firm up as")
                print("daily runs accumulate.")
    text = buf.getvalue()
    print(text)

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    with open(os.path.join(BASE_DIR, "latest_report.txt"), "w") as f:
        f.write(f"ghostwatch report -- {stamp}\n\n{text}")

    print("=" * 60)
    print("DONE. Your data is saved in Google Drive > GhostWatch.")
    print("You can close this tab. Come back and press play again")
    print("tomorrow -- ten seconds a day builds the evidence.")
    print("=" * 60)


colab_main()

