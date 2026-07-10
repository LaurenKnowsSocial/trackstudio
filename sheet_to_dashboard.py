#!/usr/bin/env python3
"""
sheet_to_dashboard.py — Reads the_receipts_tracker_v3 Google Sheet and
rebuilds the POSTS array in ~/trackstudio/index.html.

One row per post in the sheet; 24h, 7d, and 30d check data are all columns
in the same row.

Run:  python3 ~/trackstudio/sheet_to_dashboard.py
      python3 ~/trackstudio/sheet_to_dashboard.py --push   (also git push)
"""

import json
import pickle
import re
import statistics
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import gspread

# ── Config ────────────────────────────────────────────────────────────────────

SHEET_ID    = "1zXggnf739i5km6HlNwBnMzDRm3qavVYuDo4NaEbZIqc"
PICKLE_PATH = Path.home() / ".credentials/google-sheets-token.pickle"
INDEX_HTML  = Path.home() / "trackstudio/index.html"

# Rolling baseline: median 7d views of the last N posts, per client/platform.
# Only recomputed for v2 (migrated) tabs, and only once there are enough posts.
CLIENTS_JSON      = Path.home() / ".claude/social-clients.json"
BASELINE_WINDOW   = 10
BASELINE_MIN_POSTS = 5
PLATFORM_KEY      = {"ig": "instagram", "tt": "tiktok"}

# Tabs to read: (tab name, client, platform, layout)
# "v2" = lean 30-col layout (7d/30d only, pillar/format/hook, carousel revisit
# ratio); "v1" = original 39-col layout. Flip a tab to v2 when it's migrated.
TABS = [
    ("CEA Instagram", "CEA", "ig", "v2"),
    ("CEA TikTok",    "CEA", "tt", "v2"),
    ("RDC Instagram", "RDC", "ig", "v1"),
    ("RDC TikTok",    "RDC", "tt", "v1"),
    ("LKS Instagram", "LKS", "ig", "v1"),
    ("LKS TikTok",    "LKS", "tt", "v1"),
]

# Column indices (0-based, matching row 4 headers)
C = {
    # Post info
    "title":        0,
    "date":         1,
    "post_type":    2,
    "video_len":    3,
    # 24h
    "v24_views":    4,
    "v24_uniq":     5,
    "v24_nonfoll":  6,
    "v24_wtsec":    7,
    "v24_wtpct":    8,
    "v24_saves":    9,
    "v24_shares":   10,
    "v24_distro":   11,
    # 7d
    "v7_views":     12,
    "v7_uniq":      13,
    "v7_nonfoll":   14,
    "v7_wtsec":     15,
    "v7_wtpct":     16,
    "v7_saves":     17,
    "v7_saverate":  18,
    "v7_shares":    19,
    "v7_sharerate": 20,
    "v7_comments":  21,
    "v7_follows":   22,
    "v7_f1k":       23,
    "v7_engrate":   24,
    "v7_distro":    25,
    "v7_outcome":   26,
    "v7_engq":      27,
    "v7_bizsig":    28,
    "v7_nextact":   29,
    # 30d
    "v30_views":    30,
    "v30_ltviews":  31,
    "v30_ltpct":    32,
    "v30_saves":    33,
    "v30_shares":   34,
    "v30_follows":  35,
    "v30_disttype": 36,
    # Notes
    "notes_why":    37,
    "notes_hook":   38,
}

# v2 layout (lean, 30 cols) — matches the migrated CEA tabs, row-4 headers
C2 = {
    # Post info
    "title":          0,
    "date":           1,
    "post_type":      2,
    "video_len":      3,
    "format":         4,
    "hook_type":      5,
    "pillar":         6,
    # 7d
    "v7_views":       7,
    "v7_uniq":        8,   # carousel only
    "v7_nonfoll":     9,
    "v7_wtpct":       10,  # video only
    "v7_revisit":     11,  # carousel only (auto)
    "v7_saves":       12,
    "v7_saverate":    13,
    "v7_shares":      14,
    "v7_sharerate":   15,
    "v7_comments":    16,
    "v7_follows":     17,
    "v7_profvisits":  18,
    "v7_linktaps":    19,
    "v7_outcome":     20,
    "v7_nextact":     21,
    # 30d
    "v30_views":      22,
    "v30_ltviews":    23,
    "v30_ltpct":      24,
    "v30_saves":      25,
    "v30_shares":     26,
    "v30_follows":    27,
    # Notes
    "notes_why":      28,
    "notes_hook":     29,
}

# ── Value parsers ─────────────────────────────────────────────────────────────

def g(row, key, cmap=None):
    """Get cell value by column key, empty string if out of range."""
    i = (cmap or C)[key]
    return row[i].strip() if i < len(row) else ""

def parse_num(v):
    """Parse integer or float; None for blank / —."""
    v = v.strip().rstrip('%').replace(',', '')
    if not v or v == '—':
        return None
    try:
        f = float(v)
        return int(f) if f == int(f) else f
    except ValueError:
        return None

def parse_pct_formatted(v):
    """Parse a value that may come as '30.0%' or '30' → float (30.0)."""
    v = v.strip()
    if not v or v == '—':
        return None
    v = v.rstrip('%').strip()
    try:
        return float(v)
    except ValueError:
        return None

def parse_pct_raw(v):
    """Parse a raw percentage that Sheets may store as decimal (0.834 = 83.4%)
    or as a plain number (82 = 82%). Threshold: if abs(val) < 2, multiply ×100."""
    v = v.strip().rstrip('%')
    if not v or v == '—':
        return None
    try:
        f = float(v)
        if f == 0:
            return 0.0
        if abs(f) < 2:
            return round(f * 100, 2)
        return f
    except ValueError:
        return None

def parse_date(v):
    """Parse mixed date formats: 3/19/2026, 3/17/26, 2/23/2026 → datetime."""
    v = v.strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(v, fmt)
        except ValueError:
            pass
    return None

def iso_date(dt):
    return dt.strftime("%Y-%m-%d")

def due_date(dt):
    return dt.strftime("%Y-%m-%d")

def has_any(*vals):
    """True if at least one value is non-empty and not '—'."""
    return any(v.strip() and v.strip() != '—' for v in vals)

# ── Row → post object ─────────────────────────────────────────────────────────

def row_to_post(row, client, platform, post_id):
    title     = g(row, "title")
    date_raw  = g(row, "date")
    post_type = g(row, "post_type") or "Video"

    if not title or not date_raw:
        return None

    post_date = parse_date(date_raw)
    if not post_date:
        print(f"  WARNING: Could not parse date {date_raw!r} for {title!r} — skipping")
        return None

    # ── Detect which windows have data ────────────────────────────────────────
    has_24h = has_any(g(row,"v24_views"), g(row,"v24_saves"), g(row,"v24_shares"))
    has_7d  = has_any(g(row,"v7_views"),  g(row,"v7_saves"))
    has_30d = has_any(g(row,"v30_views"), g(row,"v30_saves"), g(row,"v30_shares"))

    checks = {}

    # ── 24h check ─────────────────────────────────────────────────────────────
    if has_24h:
        c = {}
        if (v := parse_num(g(row,"v24_views")))   is not None: c["views"]          = v
        if (v := parse_num(g(row,"v24_uniq")))    is not None: c["uniqueViewers"]   = v
        if (v := parse_pct_raw(g(row,"v24_nonfoll"))) is not None: c["nonFollowerPct"] = v
        if (v := parse_num(g(row,"v24_wtsec")))   is not None: c["watchTimeSec"]   = v
        if (v := parse_pct_formatted(g(row,"v24_wtpct"))) is not None: c["watchTimePct"] = v
        if (v := parse_num(g(row,"v24_saves")))   is not None: c["saves"]           = v
        if (v := parse_num(g(row,"v24_shares")))  is not None: c["shares"]          = v
        if (d := g(row,"v24_distro")):                          c["distribution"]   = d
        checks["24h"] = c

    # ── 7d check ──────────────────────────────────────────────────────────────
    if has_7d:
        c = {}
        if (v := parse_num(g(row,"v7_views")))           is not None: c["views"]          = v
        if (v := parse_num(g(row,"v7_uniq")))            is not None: c["uniqueViewers"]   = v
        if (v := parse_pct_raw(g(row,"v7_nonfoll")))     is not None: c["nonFollowerPct"] = v
        if (v := parse_num(g(row,"v7_wtsec")))           is not None: c["watchTimeSec"]   = v
        if (v := parse_pct_formatted(g(row,"v7_wtpct"))) is not None: c["watchTimePct"]   = v
        if (v := parse_num(g(row,"v7_saves")))           is not None: c["saves"]           = v
        if (v := parse_pct_formatted(g(row,"v7_saverate"))) is not None: c["saveRate"]     = v
        if (v := parse_num(g(row,"v7_shares")))          is not None: c["shares"]          = v
        if (v := parse_pct_formatted(g(row,"v7_sharerate"))) is not None: c["shareRate"]   = v
        if (v := parse_num(g(row,"v7_comments")))        is not None: c["comments"]        = v
        if (v := parse_num(g(row,"v7_follows")))         is not None: c["follows"]         = v
        if (v := parse_pct_formatted(g(row,"v7_f1k")))   is not None: c["followsPer1k"]   = v
        if (v := parse_pct_formatted(g(row,"v7_engrate"))) is not None: c["engagementRate"] = v
        if (d := g(row,"v7_distro")):                                   c["distribution"]  = d
        if (d := g(row,"v7_outcome")):                                  c["outcome"]       = d
        if (d := g(row,"v7_engq")):                                     c["engagementQuality"] = d
        if (d := g(row,"v7_bizsig")):                                   c["businessSignal"] = d
        if (d := g(row,"v7_nextact")):                                  c["nextAction"]    = d
        checks["7d"] = c

    # ── 30d check ─────────────────────────────────────────────────────────────
    if has_30d:
        c = {}
        if (v := parse_num(g(row,"v30_views")))   is not None: c["views"]           = v
        if (v := parse_num(g(row,"v30_ltviews"))) is not None: c["longTailViews"]   = v
        if (v := parse_pct_formatted(g(row,"v30_ltpct"))) is not None: c["longTailPct"] = v
        if (v := parse_num(g(row,"v30_saves")))   is not None: c["saves"]           = v
        if (v := parse_num(g(row,"v30_shares")))  is not None: c["shares"]          = v
        if (v := parse_num(g(row,"v30_follows"))) is not None: c["follows"]         = v
        if (d := g(row,"v30_disttype")):                        c["distributionType"] = d
        checks["30d"] = c

    # ── dueChecks ─────────────────────────────────────────────────────────────
    due = {}
    if not has_24h:
        due["24h"] = due_date(post_date + timedelta(days=1))
    elif not has_7d:
        due["7d"]  = due_date(post_date + timedelta(days=7))
    elif not has_30d:
        due["30d"] = due_date(post_date + timedelta(days=30))

    post = {
        "id":        post_id,
        "client":    client,
        "platform":  platform,
        "title":     title,
        "type":      post_type,
        "date":      iso_date(post_date),
        "checks":    checks,
        "dueChecks": due,
        "_sort_date": post_date,   # removed before writing JS
    }

    if why := g(row, "notes_why"):
        post["whyItPerformed"] = why
    if hook := g(row, "notes_hook"):
        post["hookInsight"] = hook

    return post

# ── Row → post object (v2 lean layout) ────────────────────────────────────────

def row_to_post_v2(row, client, platform, post_id):
    g2 = lambda key: g(row, key, C2)

    title     = g2("title")
    date_raw  = g2("date")
    post_type = g2("post_type") or "Video"

    if not title or not date_raw:
        return None

    post_date = parse_date(date_raw)
    if not post_date:
        print(f"  WARNING: Could not parse date {date_raw!r} for {title!r} — skipping")
        return None

    has_7d  = has_any(g2("v7_views"),  g2("v7_saves"))
    has_30d = has_any(g2("v30_views"), g2("v30_saves"), g2("v30_shares"))

    checks = {}

    if has_7d:
        c = {}
        if (v := parse_num(g2("v7_views")))              is not None: c["views"]          = v
        if (v := parse_num(g2("v7_uniq")))               is not None: c["uniqueViewers"]  = v
        if (v := parse_pct_raw(g2("v7_nonfoll")))        is not None: c["nonFollowerPct"] = v
        if (v := parse_pct_formatted(g2("v7_wtpct")))    is not None: c["watchTimePct"]   = v
        if (v := parse_num(g2("v7_revisit")))            is not None: c["revisitRatio"]   = v
        if (v := parse_num(g2("v7_saves")))              is not None: c["saves"]          = v
        if (v := parse_pct_formatted(g2("v7_saverate"))) is not None: c["saveRate"]       = v
        if (v := parse_num(g2("v7_shares")))             is not None: c["shares"]         = v
        if (v := parse_pct_formatted(g2("v7_sharerate"))) is not None: c["shareRate"]     = v
        if (v := parse_num(g2("v7_comments")))           is not None: c["comments"]       = v
        if (v := parse_num(g2("v7_follows")))            is not None: c["follows"]        = v
        if (v := parse_num(g2("v7_profvisits")))         is not None: c["profileVisits"]  = v
        if (v := parse_num(g2("v7_linktaps")))           is not None: c["linkTaps"]       = v
        if (d := g2("v7_outcome")):                                    c["outcome"]       = d
        if (d := g2("v7_nextact")):                                    c["nextAction"]    = d
        checks["7d"] = c

    if has_30d:
        c = {}
        if (v := parse_num(g2("v30_views")))             is not None: c["views"]         = v
        if (v := parse_num(g2("v30_ltviews")))           is not None: c["longTailViews"] = v
        if (v := parse_pct_formatted(g2("v30_ltpct")))   is not None: c["longTailPct"]   = v
        if (v := parse_num(g2("v30_saves")))             is not None: c["saves"]         = v
        if (v := parse_num(g2("v30_shares")))            is not None: c["shares"]        = v
        if (v := parse_num(g2("v30_follows")))           is not None: c["follows"]       = v
        checks["30d"] = c

    # v2 has no 24h check — due dates are 7d then 30d only
    due = {}
    if not has_7d:
        due["7d"]  = due_date(post_date + timedelta(days=7))
    elif not has_30d:
        due["30d"] = due_date(post_date + timedelta(days=30))

    post = {
        "id":        post_id,
        "client":    client,
        "platform":  platform,
        "title":     title,
        "type":      post_type,
        "date":      iso_date(post_date),
        "checks":    checks,
        "dueChecks": due,
        "_sort_date": post_date,
    }

    if fmt := g2("format"):
        post["format"] = fmt
    if hook := g2("hook_type"):
        post["hookType"] = hook
    if pillar := g2("pillar"):
        post["pillar"] = pillar
    if why := g2("notes_why"):
        post["whyItPerformed"] = why
    if hk := g2("notes_hook"):
        post["hookInsight"] = hk

    return post

# ── JS serialiser (mirrors build_js.py) ──────────────────────────────────────

def js_key(k):
    if not k or k[0].isdigit() or not re.match(r'^[A-Za-z_$][A-Za-z0-9_$]*$', k):
        return f"'{k}'"
    return k

def to_js_value(v):
    if v is None:           return 'null'
    if isinstance(v, bool): return 'true' if v else 'false'
    if isinstance(v, str):  return "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"
    if isinstance(v, float): return str(v)
    return str(v)

def dict_to_js(d, indent=0):
    inner = '  ' * (indent + 1)
    pad   = '  ' * indent
    lines = []
    for k, v in d.items():
        key = js_key(k)
        val = dict_to_js(v, indent + 1) if isinstance(v, dict) else to_js_value(v)
        lines.append(f"{inner}{key}: {val}")
    return '{\n' + ',\n'.join(lines) + '\n' + pad + '}'

def build_posts_js(posts):
    entries = ['  ' + dict_to_js(p, indent=1) for p in posts]
    return 'const POSTS = [\n' + ',\n'.join(entries) + '\n];'

# ── Rolling baselines ─────────────────────────────────────────────────────────

def recompute_baselines(all_posts, v2_tabs):
    """Median 7d views of the last BASELINE_WINDOW posts per migrated
    client/platform. Writes changes back to social-clients.json and returns
    (cfg, changes)."""
    cfg = json.loads(CLIENTS_JSON.read_text())
    changes = []
    for tab_name, client, platform in v2_tabs:
        posts = [p for p in all_posts
                 if p["client"] == client and p["platform"] == platform
                 and p["checks"].get("7d", {}).get("views")]
        posts.sort(key=lambda p: p["date"], reverse=True)
        recent = [p["checks"]["7d"]["views"] for p in posts[:BASELINE_WINDOW]]
        if len(recent) < BASELINE_MIN_POSTS:
            continue
        new_bl = int(statistics.median(recent))
        plat = cfg["clients"][client]["platforms"][PLATFORM_KEY[platform]]
        if new_bl != plat["baseline_views"]:
            changes.append((tab_name, client, platform, plat["baseline_views"], new_bl))
            plat["baseline_views"] = new_bl
    if changes:
        CLIENTS_JSON.write_text(json.dumps(cfg, indent=2) + "\n")
    return cfg, changes

def patch_clients_const(content, cfg):
    """Sync the CLIENTS const in index.html with social-clients.json baselines."""
    for client, cdata in cfg["clients"].items():
        ig = cdata["platforms"]["instagram"]["baseline_views"]
        tt = cdata["platforms"]["tiktok"]["baseline_views"]
        content = re.sub(
            rf"({client}: \{{ name:'{client}', baselines:\{{ ig:)\d+(, tt:)\d+( \}} \}})",
            rf"\g<1>{ig}\g<2>{tt}\g<3>", content)
    return content

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    push = '--push' in sys.argv

    # Connect to Google Sheets
    print("Connecting to Google Sheets…")
    with open(PICKLE_PATH, "rb") as f:
        creds = pickle.load(f)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    print(f"Connected ✓  ({sh.title})\n")

    # Read all tabs
    all_posts = []
    v2_tabs = []          # (tab_name, client, platform) for baseline recompute
    worksheets = {}
    for tab_name, client, platform, layout in TABS:
        try:
            ws = sh.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"  {tab_name}: tab not found — skipping")
            continue
        worksheets[tab_name] = ws
        if layout == "v2":
            v2_tabs.append((tab_name, client, platform))

        rows = ws.get_all_values()
        data_rows = [r for r in rows[4:] if any(c.strip() for c in r)]

        parse_row = row_to_post_v2 if layout == "v2" else row_to_post
        tab_posts = []
        for row in data_rows:
            post = parse_row(row, client, platform, post_id=0)
            if post:
                tab_posts.append(post)

        print(f"  {tab_name}: {len(tab_posts)} posts")
        all_posts.extend(tab_posts)

    if not all_posts:
        print("\nNo posts parsed — aborting.")
        sys.exit(1)

    # Sort newest first, assign IDs (strip internal sort key)
    all_posts.sort(key=lambda p: p["_sort_date"], reverse=True)
    for i, p in enumerate(all_posts):
        p["id"] = i + 1
        del p["_sort_date"]

    # Summary
    print(f"\nTotal: {len(all_posts)} posts")
    by_client = {}
    for p in all_posts:
        key = f"{p['client']} {p['platform']}"
        by_client[key] = by_client.get(key, 0) + 1
    for k, v in sorted(by_client.items()):
        print(f"  {k}: {v}")

    # Rolling baselines (migrated tabs only)
    cfg, bl_changes = recompute_baselines(all_posts, v2_tabs)
    for tab_name, client, platform, old_bl, new_bl in bl_changes:
        print(f"\nBaseline updated: {client} {platform} {old_bl:,} → {new_bl:,} "
              f"(median 7d views, last {BASELINE_WINDOW} posts)")
        try:
            worksheets[tab_name].update_acell(
                "A2", f"Baseline (7d median views, auto-updated): {new_bl:,}")
        except Exception as e:
            print(f"  (could not update baseline label on {tab_name!r}: {e})")

    # Build JS
    js = build_posts_js(all_posts)

    # Validate JS before writing
    try:
        import subprocess
        result = subprocess.run(
            ["node", "-e", f"const p = eval({repr(js.replace('const POSTS = ', '').rstrip(';'))}); process.stdout.write('ok ' + p.length)"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode != 0 or not result.stdout.startswith('ok'):
            print(f"\nJS VALIDATION FAILED:\n{result.stderr}")
            sys.exit(1)
        print(f"\nJS valid ✓  ({result.stdout.split()[1]} posts)")
    except Exception as e:
        print(f"\nWARNING: Could not validate JS ({e}) — proceeding anyway")

    # Inject into index.html
    content = INDEX_HTML.read_text()
    if '// DATA_START' not in content or '// DATA_END' not in content:
        print("\nERROR: DATA_START/DATA_END markers not found in index.html")
        sys.exit(1)

    new_block   = f'// DATA_START\n{js}\n// DATA_END'
    new_content = re.sub(r'// DATA_START\n.*?// DATA_END', new_block, content, flags=re.DOTALL)
    new_content = patch_clients_const(new_content, cfg)

    if new_content == content:
        print("index.html already up to date — no changes written.")
    else:
        INDEX_HTML.write_text(new_content)
        print("index.html updated ✓")

    # Git commit (and optionally push)
    repo = INDEX_HTML.parent
    subprocess.run(["git", "-C", str(repo), "add", "index.html"], check=True)

    try:
        subprocess.run(
            ["git", "-C", str(repo), "commit", "-m",
             f"dashboard: rebuild POSTS from Google Sheet [{datetime.now().strftime('%Y-%m-%d')}]"],
            check=True
        )
    except subprocess.CalledProcessError:
        print("Nothing new to commit.")
        return

    if push:
        result = subprocess.run(["git", "-C", str(repo), "push", "origin", "main"],
                                capture_output=True, text=True)
        if result.returncode == 0:
            print("\nDashboard updated and live at https://laurenknowssocial.github.io/trackstudio")
        else:
            print(f"\nPush failed:\n{result.stderr}")
            print("Run `gh auth login` if credentials are missing, then retry with --push.")
    else:
        print("\nindex.html committed locally. Run with --push to deploy to GitHub Pages.")


if __name__ == "__main__":
    main()
