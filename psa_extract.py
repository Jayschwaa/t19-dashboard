#!/usr/bin/env python3
"""PSA Data Extraction Script - stdlib only (no pip dependencies)"""

import urllib.request
import urllib.parse
import http.cookiejar
import json
import re
import time
import sys
import ssl
from datetime import datetime

BASE_URL = "https://uwrg.psarcweb.com/PSAWeb"
USERNAME = "jasonuwrgs"
PASSWORD = "j1fF5q2J"
SCHEMA = 1022
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"

# Disable SSL verification
ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE


class PSAClient:
    def __init__(self):
        self.cj = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cj),
            urllib.request.HTTPSHandler(context=ssl_ctx)
        )
        self.opener.addheaders = [("User-Agent", UA)]

    def _post(self, url, data=None, follow_redirects=True):
        encoded = urllib.parse.urlencode(data).encode() if data else b""
        req = urllib.request.Request(url, data=encoded, method="POST")
        try:
            resp = self.opener.open(req)
            return resp.read().decode("utf-8", errors="replace"), resp.status, dict(resp.headers)
        except urllib.error.HTTPError as e:
            if e.code in (301, 302) and not follow_redirects:
                return "", e.code, dict(e.headers)
            body = e.read().decode("utf-8", errors="replace") if e.fp else ""
            return body, e.code, dict(e.headers)

    def _get(self, url):
        req = urllib.request.Request(url)
        resp = self.opener.open(req)
        return resp.read().decode("utf-8", errors="replace")

    def login(self):
        """Login to PSA"""
        # Step 1: POST login (handle redirect manually)
        data = urllib.parse.urlencode({
            "Username": USERNAME, "Password": PASSWORD, "Schema": SCHEMA
        }).encode()
        req = urllib.request.Request(f"{BASE_URL}/Account/Login", data=data, method="POST")
        
        # We need to catch the redirect to get the transfer URL
        handler = urllib.request.HTTPRedirectHandler()
        no_redirect_opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cj),
            urllib.request.HTTPSHandler(context=ssl_ctx)
        )
        no_redirect_opener.addheaders = [("User-Agent", UA)]
        
        try:
            resp = no_redirect_opener.open(req)
            body = resp.read().decode()
            # If we got here without redirect, check if it redirected via meta or JS
            if "Transfer?Token=" in body:
                match = re.search(r'href="([^"]*Transfer\?Token=[^"]*)"', body)
                if match:
                    transfer_url = match.group(1)
                    if not transfer_url.startswith("http"):
                        transfer_url = f"https://uwrg.psarcweb.com{transfer_url}"
            else:
                print(f"Login may have succeeded directly", file=sys.stderr)
                return True
        except urllib.error.HTTPError as e:
            if e.code == 302:
                transfer_url = e.headers.get("Location", "")
                if not transfer_url.startswith("http"):
                    transfer_url = f"https://uwrg.psarcweb.com{transfer_url}"
                # Save cookies from this response
                self.cj.extract_cookies(e, req)
            else:
                raise Exception(f"Login failed: {e.code}")

        # Step 2: POST to transfer URL
        transfer_req = urllib.request.Request(transfer_url, data=b"", method="POST")
        transfer_req.add_header("Content-Length", "0")
        try:
            resp = self.opener.open(transfer_req)
        except urllib.error.HTTPError:
            pass  # 302 redirect is expected

        print("PSA login successful.", file=sys.stderr)
        return True

    def get_jobs(self, option="Open", page_size=50):
        """Get all jobs with pagination"""
        all_jobs = []
        offset = 0
        total = None

        while total is None or offset < total:
            body, _, _ = self._post(
                f"{BASE_URL}/Job/Job/ListFilter",
                data={
                    "option": option,
                    "iDisplayStart": offset,
                    "iDisplayLength": page_size,
                    "sEcho": 1,
                    "iColumns": 11,
                    "iSortCol_0": 8,
                    "sSortDir_0": "desc",
                    "iSortingCols": 1,
                    **{f"mDataProp_{i}": f"col{i}" for i in range(10)},
                    "mDataProp_10": "id"
                }
            )
            data = json.loads(body)
            total = data["iTotalDisplayRecords"]

            for row in data["aaData"]:
                job = {
                    "job_number": row[0],
                    "client_name": row[1],
                    "contact_name": row[2],
                    "insurance_info": row[3].replace("&nbsp;", "").strip(),
                    "address": row[4],
                    "state": row[5],
                    "city": row[6],
                    "assigned_to": row[7],
                    "date": row[8],
                    "status": row[9],
                    "job_id": row[10]
                }
                parts = job["job_number"].split("-")
                if len(parts) >= 4:
                    job["territory"] = parts[0]
                    job["year"] = parts[1]
                    job["seq"] = parts[2]
                    job["job_type_code"] = parts[3].split(";")[0]
                all_jobs.append(job)

            offset += page_size
            print(f"  Fetched {len(all_jobs)}/{total} jobs...", file=sys.stderr)
            time.sleep(0.2)

        return all_jobs

    def get_job_detail(self, job_id):
        """Get detailed job info from edit page"""
        html = self._get(f"{BASE_URL}/Job/Job/Edit/{job_id}")
        detail = {"job_id": job_id}

        # Financial fields
        for field in ["CompletedDisplay", "RevenueDisplay", "Deductible"]:
            match = re.search(rf'id="Entity_{field}"[^>]*value="([^"]*)"', html)
            try:
                detail[field.lower()] = float(match.group(1)) if match and match.group(1) else 0.0
            except (ValueError, AttributeError):
                detail[field.lower()] = 0.0

        # Job type
        jtype_section = re.search(r'id="Entity_JobTypeID"[^>]*>(.*?)</select>', html, re.DOTALL)
        if jtype_section:
            selected = re.findall(r'selected[^>]*>([^<]*)', jtype_section.group(1))
            detail["job_type"] = selected[0].strip() if selected else ""

        # Alt status
        alt_section = re.search(r'id="Entity_AlternativeStatusID"[^>]*>(.*?)</select>', html, re.DOTALL)
        if alt_section:
            selected = re.findall(r'selected[^>]*value="(\d+)"[^>]*>([^<]*)', alt_section.group(1))
            detail["alt_status_id"] = selected[0][0] if selected else ""
            detail["alt_status"] = selected[0][1].strip() if selected else ""

        # Location
        loc_section = re.search(r'id="Entity_LocationID"[^>]*>(.*?)</select>', html, re.DOTALL)
        if loc_section:
            selected = re.findall(r'selected[^>]*>([^<]*)', loc_section.group(1))
            detail["location"] = selected[0].strip() if selected else ""

        # Team
        team_section = re.search(r'name="Entity\.TeamID"[^>]*>(.*?)</select>', html, re.DOTALL)
        if team_section:
            selected = re.findall(r'selected[^>]*>([^<]*)', team_section.group(1))
            detail["team"] = selected[0].strip() if selected else ""

        # Referrer
        ref_section = re.search(r'id="Entity_ReferrerID"[^>]*>(.*?)</select>', html, re.DOTALL)
        if ref_section:
            selected = re.findall(r'selected[^>]*>([^<]*)', ref_section.group(1))
            detail["referrer"] = selected[0].strip() if selected else ""

        # Lifecycle dates
        dates = re.findall(r'JobDates\[(\d+)\]\.DateTypeDescription"[^>]*value="([^"]*)"', html)
        date_vals = re.findall(r'name="JobDates\[(\d+)\]\.DateTime"[^>]*value="([^"]*)"', html)
        if not date_vals:
            date_vals = re.findall(r'id="JobDates_(\d+)__DateTime"[^>]*value="([^"]*)"', html)
        date_map = {idx: desc for idx, desc in dates}
        date_val_map = dict(date_vals)
        detail["dates"] = {}
        for idx, desc in date_map.items():
            val = date_val_map.get(idx, "")
            if val:
                detail["dates"][desc] = val

        # Phone numbers
        phones = re.findall(r'(\(\d{3}\)\s*\d{3}[-.]?\d{4}|\d{3}[-.]?\d{3}[-.]?\d{4})', html)
        detail["phones"] = list(set(p for p in phones if p != "866-992-2626"))

        # Emails
        emails = re.findall(r'[\w.+-]+@[\w-]+\.[\w.]+', html)
        detail["emails"] = list(set(
            e for e in emails
            if not any(x in e.lower() for x in ['canamsys', 'google', 'jquery', 'fabric', 'ckeditor'])
        ))

        # Address
        for field in ["Address1", "Address2", "City", "Region", "PostalCode"]:
            match = re.search(rf'id="Entity_rm_site_{field}"[^>]*value="([^"]*)"', html)
            if match and match.group(1):
                detail[f"site_{field.lower()}"] = match.group(1)

        return detail

    def get_financial(self, job_id):
        """Get financial summary for a job"""
        url = f"{BASE_URL}/Job/Financial/List?linkID={job_id}&UpdateTargetId=FinancialTab&Source=Job"
        html = self._get(url)
        financial = {"job_id": job_id}

        # Strip scripts, then parse table
        clean = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL)
        clean = re.sub(r'<[^>]+>', '\t', clean)
        tokens = [t.strip() for t in clean.split('\t') if t.strip()]

        labels = ["Material", "Labor", "Subtrade", "Equipment", "Expense",
                  "Revenue Overhead", "Cost", "Revenue", "Profit",
                  "Gross Margin", "Invoiced", "Paid", "Outstanding"]

        summary = {}
        for i, token in enumerate(tokens):
            if token in labels:
                # Collect dollar amounts after this label
                amounts = []
                for j in range(1, 6):
                    if i + j < len(tokens):
                        next_token = tokens[i + j]
                        if next_token in labels or next_token == "Totals" or next_token == "Actual" or next_token == "Estimate":
                            break
                        dollar_vals = re.findall(r'\(?\$?[\d,]+\.?\d*\)?%?', next_token)
                        for dv in dollar_vals:
                            val = dv.replace("$", "").replace(",", "").replace("(", "-").replace(")", "").replace("%", "").strip()
                            if val:
                                try:
                                    amounts.append(float(val))
                                except ValueError:
                                    pass

                key = token.lower().replace(" ", "_")
                if len(amounts) >= 1:
                    summary[f"{key}_actual"] = amounts[0]
                if len(amounts) >= 2:
                    summary[f"{key}_estimate"] = amounts[1]

        financial["summary"] = summary

        # Hidden input totals
        for name in ["TotalCost.Actual", "TotalCost.Estimate", "TotalRevenue.Actual", "TotalRevenue.Estimate"]:
            match = re.search(rf'name="{re.escape(name)}"[^>]*value="([^"]*)"', html)
            if match and match.group(1):
                try:
                    financial[name.replace(".", "_").lower()] = float(match.group(1))
                except ValueError:
                    pass

        return financial

    def get_notes(self, job_id, limit=10):
        """Get activity notes/log for a job"""
        url = f"{BASE_URL}/Relationship/Log/ListFilter?linkID={job_id}&linkSource=Job&isCustomer=False"
        body, _, _ = self._post(url, data={
            "iDisplayStart": 0,
            "iDisplayLength": limit,
            "sEcho": 1,
            "iColumns": 11,
            "iSortCol_0": 1,
            "sSortDir_0": "desc",
            "iSortingCols": 1,
            "linkID": job_id,
            "linkSource": "Job",
            "displayOption": "false",
            "mustSeeNotes": "false",
            **{f"mDataProp_{i}": f"col{i}" for i in range(10)},
            "mDataProp_10": "id"
        })
        data = json.loads(body)

        notes = []
        for row in data.get("aaData", []):
            note_text = re.sub(r'<[^>]+>', '', str(row[8])) if row[8] else ""
            recipients = re.sub(r'<[^>]+>', '', str(row[6])) if row[6] else ""
            notes.append({
                "id": row[0],
                "created": row[1],
                "scheduled": str(row[2]).replace("&nbsp;", "").strip(),
                "delivered": str(row[3]).replace("&nbsp;", "").strip(),
                "employee": row[4],
                "topic": row[5],
                "recipients": recipients,
                "subject": row[7],
                "note": note_text.strip(),
                "visibility": row[9]
            })

        return {"job_id": job_id, "total_notes": data.get("iTotalRecords", 0), "notes": notes}


def test_sample():
    """Quick test with a few jobs"""
    client = PSAClient()
    client.login()

    # Get recent open jobs
    print("\n=== FETCHING SAMPLE JOBS ===", file=sys.stderr)
    body, _, _ = client._post(
        f"{BASE_URL}/Job/Job/ListFilter",
        data={
            "option": "Open", "iDisplayStart": 0, "iDisplayLength": 5, "sEcho": 1,
            "iColumns": 11, "iSortCol_0": 8, "sSortDir_0": "desc", "iSortingCols": 1,
            **{f"mDataProp_{i}": f"col{i}" for i in range(10)}, "mDataProp_10": "id"
        }
    )
    data = json.loads(body)
    print(f"Total open: {data['iTotalRecords']}")

    for row in data["aaData"]:
        print(f"  {row[10]:>6} | {row[0]:<25} | {row[1]:<30} | {row[7]}")

    # Pick a job and enrich it
    sample = data["aaData"][3]  # A WTR job
    jid, jnum = sample[10], sample[0]
    print(f"\n=== DETAIL: {jnum} (ID: {jid}) ===")
    detail = client.get_job_detail(jid)
    print(json.dumps(detail, indent=2, default=str))

    print(f"\n=== FINANCIAL: {jnum} ===")
    fin = client.get_financial(jid)
    print(json.dumps(fin, indent=2, default=str))

    print(f"\n=== NOTES: {jnum} ===")
    notes = client.get_notes(jid, limit=5)
    print(json.dumps(notes, indent=2, default=str))


def extract_all(output_path="/tmp/psa_extract.json"):
    """Full extraction"""
    client = PSAClient()
    client.login()

    print("\n=== FETCHING ALL OPEN JOBS ===", file=sys.stderr)
    jobs = client.get_jobs("Open")
    print(f"Total: {len(jobs)}", file=sys.stderr)

    territories = {}
    type_counts = {}
    for job in jobs:
        t = job.get("territory", "??")
        territories.setdefault(t, []).append(job)
        tc = job.get("job_type_code", "??")
        type_counts[tc] = type_counts.get(tc, 0) + 1

    print(f"\nTerritories: {json.dumps({t: len(j) for t, j in territories.items()})}", file=sys.stderr)
    print(f"Types: {json.dumps(type_counts)}", file=sys.stderr)

    # Enrich recent jobs (2025-2026)
    recent = [j for j in jobs if j.get("year", "00") in ("25", "26")]
    print(f"\nEnriching {len(recent)} recent jobs...", file=sys.stderr)

    for i, job in enumerate(recent):
        jid = job["job_id"]
        print(f"  [{i+1}/{len(recent)}] {job['job_number']}", file=sys.stderr)

        try:
            job["detail"] = client.get_job_detail(jid)
        except Exception as e:
            print(f"    detail error: {e}", file=sys.stderr)
            job["detail"] = {}

        try:
            job["financial"] = client.get_financial(jid)
        except Exception as e:
            print(f"    financial error: {e}", file=sys.stderr)
            job["financial"] = {}

        try:
            job["notes"] = client.get_notes(jid, limit=5)
        except Exception as e:
            print(f"    notes error: {e}", file=sys.stderr)
            job["notes"] = {}

        time.sleep(0.3)

        if (i + 1) % 50 == 0:
            with open(output_path, "w") as f:
                json.dump({"ts": datetime.now().isoformat(), "jobs": recent[:i+1]}, f)
            print(f"    saved progress ({i+1})", file=sys.stderr)

    result = {
        "extracted_at": datetime.now().isoformat(),
        "total_open": len(jobs),
        "total_enriched": len(recent),
        "territories": {t: len(j) for t, j in territories.items()},
        "type_counts": type_counts,
        "jobs": recent
    }

    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\nDone. Saved {len(recent)} enriched jobs to {output_path}", file=sys.stderr)
    return result


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "full":
        extract_all()
    else:
        test_sample()
