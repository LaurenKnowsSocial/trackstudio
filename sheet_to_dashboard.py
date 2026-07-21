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
PLATFORM_KEY      = {"ig": "instagram", "tt": "tiktok", "fb": "facebook"}

# Tabs to read: (tab name, client, platform, layout). All tabs use the lean
# v2+ layout since 2026-07-10 (RDC/LKS removed from the tracker; the v1 parser
# lives in git history if ever needed).
TABS = [
    ("CEA Instagram", "CEA", "ig", "v2"),
    ("CEA TikTok",    "CEA", "tt", "v2"),
    ("BTC Instagram", "BTC", "ig", "v2"),
    ("BTC Facebook",  "BTC", "fb", "v2"),
]

# v3.2 layout (lean, 30 cols A-AD) — row-4 headers. 2026-07-13: Format and
# Hook Type columns removed (never used). Watch time entered as SECONDS (I);
# % (J) and all other (auto) columns are sheet formulas.
C2 = {
    # Post info
    "title":          0,
    "date":           1,
    "post_type":      2,   # Video / Carousel / Static
    "video_len":      3,
    "pillar":         4,
    # 7d
    "v7_views":       5,
    "v7_uniq":        6,   # non-video only
    "v7_nonfoll":     7,
    "v7_wtsec":       8,   # video only (entered)
    "v7_wtpct":       9,   # video only (auto: sec / video_len)
    "v7_revisit":     10,  # non-video (auto)
    "v7_saves":       11,
    "v7_saverate":    12,
    "v7_shares":      13,
    "v7_sharerate":   14,
    "v7_comments":    15,
    "v7_follows":     16,
    "v7_profvisits":  17,
    "v7_linktaps":    18,
    "v7_outcome":     19,
    "v7_nextact":     20,
    # 30d
    "v30_views":      21,
    "v30_ltviews":    22,
    "v30_ltpct":      23,
    "v30_saves":      24,
    "v30_shares":     25,
    "v30_follows":    26,
    # Notes
    "notes_why":      27,
    "notes_hook":     28,
    "link":           29,
}

# Auto-column formulas (v3.2 letters). Reinstalled on every run because rows
# INSERTED into the sheet (Lauren adds newest posts at the top) don't inherit
# formulas — idempotent: these columns contain nothing but formulas.
FORMULA_ROWS = (5, 1000)

def auto_formulas(r):
    return {
        "J": (f'=IF(AND($C{r}="Video",$I{r}<>"",$D{r}<>""),ROUND($I{r}/$D{r}*100,1),"")'),
        "K": (f'=IF(AND($C{r}<>"",$C{r}<>"Video",$F{r}<>"",$G{r}<>""),ROUND($F{r}/$G{r},2),"")'),
        "M": (f'=IF(AND($F{r}<>"",$L{r}<>""),ROUND($L{r}/$F{r}*100,2),"")'),
        "O": (f'=IF(AND($F{r}<>"",$N{r}<>""),ROUND($N{r}/$F{r}*100,2),"")'),
        "T": (f'=IF($F{r}="","",IFERROR(LET(bl,VALUE(SUBSTITUTE(REGEXEXTRACT($A$2,"[\\d,]+$"),",","")),'
              f'ret_hi,IF($C{r}="Video",IF($J{r}<>"",$J{r}>=30,FALSE),IF($K{r}<>"",$K{r}>=1.3,FALSE)),'
              f'ret_lo,IF($C{r}="Video",IF($J{r}<>"",$J{r}<20,TRUE),IF($K{r}<>"",$K{r}<1.1,TRUE)),'
              f'save_hi,IF($M{r}<>"",$M{r}>=1,FALSE),'
              f'save_lo,IF($M{r}<>"",$M{r}<0.5,FALSE),'
              f'IF(AND($F{r}>bl*1.2,OR(ret_hi,save_hi)),"Winner",'
              f'IF(AND($F{r}<bl*0.8,ret_lo,save_lo),"Underperformed","Learning"))),"CHECK A2"))'),
        "U": (f'=IF($T{r}="","",IF(AND($T{r}="Winner",OR(N($Q{r})>0,N($S{r})>0)),"Repeat",'
              f'IF(AND($T{r}="Underperformed",$L{r}<>"",$N{r}<>"",$L{r}=0,$N{r}=0),"Drop","Iterate")))'),
        "W": (f'=IF(AND($V{r}<>"",$F{r}<>""),$V{r}-$F{r},"")'),
        "X": (f'=IF(AND($V{r}<>"",$W{r}<>""),ROUND($W{r}/$V{r}*100,2),"")'),
    }

def ensure_formulas(ws):
    first, last = FORMULA_ROWS
    data = [{"range": f"{col}{first}:{col}{last}",
             "values": [[auto_formulas(r)[col]] for r in range(first, last + 1)]}
            for col in ("J", "K", "M", "O", "T", "U", "W", "X")]
    ws.batch_update(data, value_input_option="USER_ENTERED")

# ── Value parsers ─────────────────────────────────────────────────────────────

def g(row, key, cmap=None):
    """Get cell value by column key, empty string if out of range."""
    i = (cmap or C2)[key]
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

# ── Pillars ───────────────────────────────────────────────────────────────────
# Sheet cells read like "P4 - Authority". The NUMBER is not trustworthy: the two
# CEA tabs number the same pillars differently (Proof is P3 on Instagram but P2
# on TikTok, so a number-keyed lookup mislabels 24 posts and silently drops the
# Instagram-only P5). The NAME is consistent across tabs, so resolve on the name
# and treat social-clients.json as the canonical id ↔ name map.

def norm_pillar(s):
    """Fold a pillar label for matching: case, punctuation, and the emoji
    prefixes on the BTC names ('🍽️ Use' ↔ 'Use') all become irrelevant."""
    return re.sub(r"[^a-z0-9]", "", s.lower())

def build_pillar_lut(cfg):
    """{client: {normalized name: canonical id}} from social-clients.json."""
    lut = {}
    for key, cdata in cfg["clients"].items():
        names = {}
        for p in cdata.get("pillars", []):
            for label in [p["name"], *p.get("aliases", [])]:
                names[norm_pillar(label)] = p["id"]
        lut[key] = names
    return lut

def resolve_pillar(cell, client, pillar_lut, unmatched):
    """'P4 - Authority' → canonical id. Falls back to the literal P# when the
    name is unknown, and records it so main() can report it rather than
    silently mislabelling the post."""
    cell = cell.strip()
    m = re.match(r"(P\d+)\s*[-–—]\s*(.+)$", cell)
    if not m:
        m2 = re.match(r"(P\d+)$", cell)
        if m2:
            unmatched.append((client, cell, "no name in cell"))
            return m2.group(1)
        unmatched.append((client, cell, "unparseable"))
        return cell
    literal_id, name = m.group(1), m.group(2)
    canonical = pillar_lut.get(client, {}).get(norm_pillar(name))
    if canonical:
        return canonical
    unmatched.append((client, cell, f"name not in social-clients.json"))
    return literal_id

# ── Row → post object (v2 lean layout) ────────────────────────────────────────

def row_to_post_v2(row, client, platform, post_id, pillar_lut, unmatched):
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
        if (v := parse_pct_formatted(g2("v7_nonfoll")))  is not None: c["nonFollowerPct"] = v
        if (v := parse_num(g2("v7_wtsec")))              is not None: c["watchTimeSec"]   = v
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

    # Pillar cells hold human labels like "P1 - Demystify" (2026-07-14 dropdown);
    # the dashboard keys on the canonical P# id — resolved by NAME, see above.
    if pillar := g2("pillar"):
        post["pillar"] = resolve_pillar(pillar, client, pillar_lut, unmatched)
    if link := g2("link"):
        post["link"] = link
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
    """Regenerate the whole CLIENTS const in index.html from
    social-clients.json — names, per-platform baselines, and pillar maps.
    The json is the only hand-edited copy."""
    js_plat = {v: k for k, v in PLATFORM_KEY.items()}
    entries = []
    for key, cdata in cfg["clients"].items():
        bl = ", ".join(f"{js_plat[p]}:{d['baseline_views']}"
                       for p, d in cdata["platforms"].items())
        pillars = ", ".join(
            f"{p['id']}:'" + p["name"].replace("\\", "\\\\").replace("'", "\\'") + "'"
            for p in cdata.get("pillars", []))
        name = cdata["name"].replace("'", "\\'")
        entries.append(f"  {key}: {{ name:'{name}', baselines:{{ {bl} }}, "
                       f"pillars:{{ {pillars} }} }}")
    block = "const CLIENTS = {\n" + ",\n".join(entries) + "\n};"
    return re.sub(r"const CLIENTS = \{.*?\};", block, content, flags=re.DOTALL)

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

    # Canonical pillar names, used to resolve the Sheet's per-tab numbering
    pillar_lut = build_pillar_lut(json.loads(CLIENTS_JSON.read_text()))
    unmatched_pillars = []

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

        # Self-heal: rows Lauren inserts by hand don't inherit formulas —
        # reinstall the auto columns before reading values.
        ensure_formulas(ws)

        rows = ws.get_all_values()
        data_rows = [r for r in rows[4:] if any(c.strip() for c in r)]

        tab_posts = []
        for row in data_rows:
            post = row_to_post_v2(row, client, platform, post_id=0,
                                  pillar_lut=pillar_lut,
                                  unmatched=unmatched_pillars)
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

    # Pillar resolution: show what the labels became, and flag anything the
    # name lookup could not place (a new dropdown option, a typo, a rename).
    by_pillar = {}
    for p in all_posts:
        if p.get("pillar"):
            key = f"{p['client']} {p['pillar']}"
            by_pillar[key] = by_pillar.get(key, 0) + 1
    if by_pillar:
        print("\nPillars:")
        for k, v in sorted(by_pillar.items()):
            print(f"  {k}: {v}")
    if unmatched_pillars:
        seen = sorted({(c, cell, why) for c, cell, why in unmatched_pillars})
        print(f"\n⚠  {len(unmatched_pillars)} pillar cell(s) not resolved by name "
              f"— falling back to the literal number:")
        for c, cell, why in seen:
            print(f"     {c}: {cell!r} ({why})")
        print("   Add the name (or an alias) to social-clients.json to fix.")

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
