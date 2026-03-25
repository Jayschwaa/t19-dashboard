#!/usr/bin/env python3
"""
T-19 Job Dashboard
Flask app — pulls live PSA data, scores/prioritizes, supports comments + manual reprioritization
"""

from flask import Flask, jsonify, request, render_template
from psa_extract import PSAClient, BASE_URL
import json, os, re, threading, time
from datetime import datetime

app = Flask(__name__)

OVERRIDE_FILE = '/tmp/priority_overrides.json'
CACHE_FILE    = '/tmp/jobs_cache.json'
CACHE_TTL     = 1800  # 30 minutes

_lock = threading.Lock()

import traceback as _tb

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def load_overrides():
    if os.path.exists(OVERRIDE_FILE):
        with open(OVERRIDE_FILE) as f:
            return json.load(f)
    return {}

def save_overrides(data):
    with open(OVERRIDE_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def parse_date(s):
    if not s:
        return None
    for fmt in ["%m/%d/%Y %I:%M %p", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y %I:%M:%S %p"]:
        try:
            return datetime.strptime(s.strip(), fmt)
        except:
            pass
    return None

def days_open(job):
    d = job.get('detail', {}).get('dates', {})
    for k, v in d.items():
        if any(x in k.lower() for x in ['received', 'created', 'reported', 'open']):
            dt = parse_date(v)
            if dt:
                return (datetime.now() - dt).days
    dt = parse_date(job.get('date', ''))
    if dt:
        return (datetime.now() - dt).days
    return 0

def last_activity_days(job):
    notes = job.get('notes', {}).get('notes', [])
    if not notes:
        return 999
    latest = None
    for n in notes:
        dt = parse_date(n.get('created', ''))
        if dt and (latest is None or dt > latest):
            latest = dt
    if latest:
        return (datetime.now() - latest).days
    return 999

def get_revenue(job):
    fin = job.get('financial', {}).get('summary', {})
    return max(
        fin.get('revenue_estimate', 0) or 0,
        fin.get('revenue_actual', 0) or 0,
        job.get('detail', {}).get('revenuedisplay', 0) or 0
    )

def iicrc_flags(job):
    """Check notes for IICRC documentation completeness."""
    notes_text = ' '.join(
        n.get('note', '').lower()
        for n in job.get('notes', {}).get('notes', [])
    )
    flags = []
    if not any(x in notes_text for x in ['moisture', 'reading', 'gpp', 'rh', 'humidity']):
        flags.append('Missing moisture readings')
    if not any(x in notes_text for x in ['drying log', 'daily monitor', 'day 1', 'day 2', 'drying progress']):
        flags.append('No drying log/daily monitoring')
    if not any(x in notes_text for x in ['equipment', 'dehumidifier', 'air mover', 'placed']):
        flags.append('No equipment placement record')
    if not any(x in notes_text for x in ['dry standard', 'clearance', 'final read', 'dry goal', 'reached standard']):
        flags.append('No dry standard/clearance')
    if not any(x in notes_text for x in ['source', 'cause', 'loss origin', 'pipe', 'ac ', 'roof', 'toilet', 'dishwasher', 'washing machine']):
        flags.append('Source not documented')
    return flags

def ticket_flags(job):
    """Check for missing ticket fields."""
    detail = job.get('detail', {})
    flags = []
    ins = job.get('insurance_info', '') or ''
    if not ins.strip() or ins.strip() in ['&nbsp;', '-']:
        flags.append('No insurance info')
    notes_text = ' '.join(
        n.get('note', '').lower()
        for n in job.get('notes', {}).get('notes', [])
    )
    if not any(x in notes_text for x in ['claim', 'claim #', 'claim number']):
        flags.append('No claim number in notes')
    if not any(x in notes_text for x in ['adjuster', 'adj.', 'adj ']):
        flags.append('No adjuster contact noted')
    if not detail.get('revenuedisplay') and not detail.get('completeddisplay'):
        flags.append('No estimate/scope')
    phones = detail.get('phones', [])
    if not phones:
        flags.append('No phone number on file')
    return flags

def upsell_flags(job, all_job_numbers):
    """Flag missing upsell services."""
    jnum = job.get('job_number', '')
    # Extract base: territory-year-seq
    parts = jnum.split('-')
    base = '-'.join(parts[:3]) if len(parts) >= 3 else jnum
    jtype = job.get('job_type_code', '')
    flags = []

    has_con = any(n.startswith(base) and n.endswith('-CON') for n in all_job_numbers)
    has_str = any(n.startswith(base) and (n.endswith('-STR') or n.endswith('-RCN')) for n in all_job_numbers)

    if jtype in ('WTR', 'MLD') and not has_con:
        flags.append('No Contents (CON) job')
    if jtype in ('WTR', 'MLD') and not has_str:
        flags.append('No Reconstruction (STR) estimate')

    notes_text = ' '.join(
        n.get('note', '').lower()
        for n in job.get('notes', {}).get('notes', [])
    )
    if any(x in notes_text for x in ['ac ', 'a/c', 'condensate', 'duct', 'air condition']):
        if 'duct clean' not in notes_text:
            flags.append('AC source — duct cleaning not offered')

    if 'source solution' not in notes_text and 'source repair' not in notes_text:
        flags.append('Source solution not documented')

    return flags

def priority_score(job, all_job_numbers, overrides):
    jid = str(job.get('job_id', ''))
    if jid in overrides and overrides[jid].get('pinned_score') is not None:
        return overrides[jid]['pinned_score'], {}

    breakdown = {}
    score = 0
    do = days_open(job)
    pts_age = min(do * 2, 100)
    score += pts_age
    breakdown['age'] = {'pts': round(pts_age,1), 'detail': f'{do} days open'}

    rev = get_revenue(job)
    pts_rev = min(rev / 500, 60)
    score += pts_rev
    breakdown['revenue'] = {'pts': round(pts_rev,1), 'detail': f'${rev:,.0f} estimated'}

    la = last_activity_days(job)
    pts_stuck = min(la * 3, 60)
    score += pts_stuck
    breakdown['stuck'] = {'pts': round(pts_stuck,1), 'detail': f'{la if la < 999 else "999+"} days since last activity'}

    iicrc = iicrc_flags(job)
    pts_iicrc = len(iicrc) * 8
    score += pts_iicrc
    breakdown['iicrc'] = {'pts': pts_iicrc, 'detail': f'{len(iicrc)} gap(s)'}

    ticket = ticket_flags(job)
    pts_ticket = len(ticket) * 5
    score += pts_ticket
    breakdown['ticket'] = {'pts': pts_ticket, 'detail': f'{len(ticket)} gap(s)'}

    upsell = upsell_flags(job, all_job_numbers)
    pts_upsell = len(upsell) * 6
    score += pts_upsell
    breakdown['upsell'] = {'pts': pts_upsell, 'detail': f'{len(upsell)} opportunity(ies)'}

    bump = overrides.get(jid, {}).get('bump', 0)
    if bump:
        score += bump
        breakdown['manual'] = {'pts': bump, 'detail': 'Owner adjustment'}

    return round(score, 1), breakdown

# ─────────────────────────────────────────────
# PSA data fetch (with cache)
# ─────────────────────────────────────────────

def fetch_t19_jobs():
    # Check cache
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            cached = json.load(f)
        age = time.time() - cached.get('ts', 0)
        if age < CACHE_TTL:
            return cached['jobs']

    client = PSAClient()
    client.login()

    # Get all open jobs, filter to T-19 pre-invoice only
    all_jobs = client.get_jobs('Open', page_size=100)
    t19_all = [j for j in all_jobs if j.get('territory') == '19']

    # Pre-invoice filter
    EXCLUDE_STATUSES = {'complete', 'completed', 'invoiced', 'closed', 'paid', 'collections'}
    t19 = [j for j in t19_all if (j.get('status') or '').lower() not in EXCLUDE_STATUSES]

    print(f"T-19 pre-invoice jobs: {len(t19)}", flush=True)

    # Enrich using thread pool for speed
    import concurrent.futures

    def enrich(job):
        jid = job['job_id']
        try:
            c = PSAClient()
            c.login()
            job['detail'] = c.get_job_detail(jid)
            job['financial'] = c.get_financial(jid)
            job['notes'] = c.get_notes(jid, limit=20)
        except Exception as e:
            print(f"Enrich error {jid}: {e}", flush=True)
            job.setdefault('detail', {})
            job.setdefault('financial', {})
            job.setdefault('notes', {})
        return job

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        t19 = list(ex.map(enrich, t19))

    # Post-enrich filter
    EXCLUDE_ALT = {'invoiced', 'paid', 'closed', 'collections', 'write off', 'write-off', 'completed'}
    t19 = [j for j in t19 if (j.get('detail', {}).get('alt_status', '') or '').lower() not in EXCLUDE_ALT]

    with open(CACHE_FILE, 'w') as f:
        json.dump({'ts': time.time(), 'jobs': t19}, f)

    print(f"Cache written: {len(t19)} jobs", flush=True)
    return t19

# ─────────────────────────────────────────────
# API Routes
# ─────────────────────────────────────────────

@app.route('/api/jobs')
def api_jobs():
    try:
        jobs = fetch_t19_jobs()
    except Exception as e:
        err = _tb.format_exc()
        print(err)
        return jsonify({'error': str(e), 'traceback': err}), 500

    overrides = load_overrides()
    all_nums = [j.get('job_number', '') for j in jobs]

    result = []
    for job in jobs:
        jid = str(job.get('job_id', ''))
        do = days_open(job)
        rev = get_revenue(job)
        la = last_activity_days(job)
        iicrc = iicrc_flags(job)
        ticket = ticket_flags(job)
        upsell = upsell_flags(job, all_nums)
        score, breakdown = priority_score(job, all_nums, overrides)
        override_note = overrides.get(jid, {}).get('comment', '')

        detail = job.get('detail', {})
        phones = detail.get('phones', [])
        emails = detail.get('emails', [])
        address_parts = [
            detail.get('site_address1',''),
            detail.get('site_city',''),
            detail.get('site_region',''),
            detail.get('site_postalcode',''),
        ]
        address = ', '.join(p for p in address_parts if p)

        result.append({
            'job_id': jid,
            'job_number': job.get('job_number', ''),
            'client_name': job.get('client_name', ''),
            'contact_name': job.get('contact_name', ''),
            'job_type': job.get('job_type_code', ''),
            'status': job.get('status', ''),
            'alt_status': detail.get('alt_status', ''),
            'assigned_to': job.get('assigned_to', ''),
            'revenue': rev,
            'days_open': do,
            'last_activity_days': la,
            # Contact info
            'phones': phones,
            'phone': phones[0] if phones else '',
            'emails': emails,
            'address': address,
            # Flags
            'iicrc_flags': iicrc,
            'ticket_flags': ticket,
            'upsell_flags': upsell,
            # Score
            'priority_score': score,
            'score_breakdown': breakdown,
            'override_note': override_note,
            'recent_notes': [
                {'date': n.get('created', ''), 'text': n.get('note', ''), 'by': n.get('employee', '')}
                for n in (job.get('notes', {}).get('notes', []) or [])[:3]
            ],
        })

    result.sort(key=lambda x: x['priority_score'], reverse=True)
    return jsonify(result)


@app.route('/api/comment/<job_id>', methods=['POST'])
def api_comment(job_id):
    data = request.json or {}
    comment = data.get('comment', '').strip()
    bump = int(data.get('bump', 0))

    if not comment and bump == 0:
        return jsonify({'error': 'No comment or bump provided'}), 400

    overrides = load_overrides()
    if job_id not in overrides:
        overrides[job_id] = {}

    if comment:
        overrides[job_id]['comment'] = comment
        overrides[job_id]['comment_ts'] = datetime.now().isoformat()

        # Post to PSA
        try:
            client = PSAClient()
            client.login()
            client._post(
                f"{BASE_URL}/Relationship/Log/Create",
                data={
                    'linkID': job_id,
                    'linkSource': 'Job',
                    'Subject': 'Dashboard Note',
                    'NoteText': comment,
                    'VisibilityID': 1,
                    'TopicID': '',
                    'IsScheduled': 'false',
                }
            )
        except Exception as e:
            # Save override even if PSA post fails
            overrides[job_id]['psa_post_error'] = str(e)

    if bump:
        overrides[job_id]['bump'] = overrides[job_id].get('bump', 0) + bump

    save_overrides(overrides)

    # Invalidate cache
    if os.path.exists(CACHE_FILE):
        cached = json.load(open(CACHE_FILE))
        cached['ts'] = 0
        with open(CACHE_FILE, 'w') as f:
            json.dump(cached, f)

    return jsonify({'ok': True})


@app.route('/api/refresh', methods=['POST'])
def api_refresh():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            cached = json.load(f)
        cached['ts'] = 0
        with open(CACHE_FILE, 'w') as f:
            json.dump(cached, f)
    return jsonify({'ok': True})


@app.route('/')
def index():
    return render_template('dashboard.html')


if __name__ == '__main__':
    # Pre-warm cache in background thread so first request is fast
    def prewarm():
        try:
            print("Pre-warming cache...", flush=True)
            fetch_t19_jobs()
            print("Cache ready.", flush=True)
        except Exception as e:
            print(f"Pre-warm failed: {e}", flush=True)
    t = threading.Thread(target=prewarm, daemon=True)
    t.start()
    port = int(os.environ.get('PORT', 5055))
    app.run(host='0.0.0.0', port=port, debug=False)
