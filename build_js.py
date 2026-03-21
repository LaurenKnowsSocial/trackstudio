#!/usr/bin/env python3
"""
build_js.py — Parses all SocialTracking reports.md files and rebuilds the
POSTS array in ~/trackstudio/index.html between // DATA_START and // DATA_END.

Run:  python3 ~/trackstudio/build_js.py
"""

import json
import re
import sys
from pathlib import Path
from datetime import datetime, timedelta

# ── Config ────────────────────────────────────────────────────────────────────

REPORT_FILES = [
    (Path.home() / "Documents/LaurenKnowsSocial/SocialTracking/CEA/reports.md",        "ig"),
    (Path.home() / "Documents/LaurenKnowsSocial/SocialTracking/CEA/reports-tiktok.md", "tt"),
    (Path.home() / "Documents/LaurenKnowsSocial/SocialTracking/RDC/reports.md",        "ig"),
    (Path.home() / "Documents/LaurenKnowsSocial/SocialTracking/RDC/reports-tiktok.md", "tt"),
    (Path.home() / "Documents/LaurenKnowsSocial/SocialTracking/LKS/reports.md",        "ig"),
    (Path.home() / "Documents/LaurenKnowsSocial/SocialTracking/LKS/reports-tiktok.md", "tt"),
]

INDEX_HTML = Path.home() / "trackstudio/index.html"

# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_num(val):
    """Return int/float or None for — / empty."""
    val = val.strip().rstrip('%')
    if not val or val == '—':
        return None
    try:
        f = float(val)
        return int(f) if f == int(f) else f
    except ValueError:
        return None

def parse_pct(val):
    """Strip %, return float or None."""
    val = val.strip().rstrip('%')
    if not val or val == '—':
        return None
    try:
        return float(val)
    except ValueError:
        return None

def parse_date(s):
    """Parse 'January 2, 2026' or 'Jan 2, 2026' → datetime."""
    s = s.strip()
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None

def iso_date(dt):
    """Format datetime as ISO 8601 string JS Date() can parse: '2026-01-02'."""
    return dt.strftime("%Y-%m-%d")

def due_date(dt):
    """Format a due date as ISO for consistency."""
    return dt.strftime("%Y-%m-%d")

def infer_type(title):
    """Infer post type from title keywords."""
    if 'carousel' in title.lower():
        return 'Carousel'
    return 'Video'

# ── Section parser ────────────────────────────────────────────────────────────

def parse_section(lines):
    """Extract metrics/tags/notes from the lines of one check section."""
    data = {}
    notes_lines = []
    in_notes = False

    for line in lines:
        ls = line.strip()

        if ls == 'NOTES':
            in_notes = True
            continue
        if in_notes:
            notes_lines.append(ls)
            continue
        if ls in ('RAW METRICS', 'CALCULATED RATES', 'TAGS'):
            continue

        def gv(prefix):
            if ls.startswith(prefix + ':'):
                return ls[len(prefix) + 1:].strip()
            return None

        # Raw metrics
        if (v := gv('Views'))               is not None: data['views']          = parse_num(v);  continue
        if (v := gv('Unique Viewers'))      is not None: data['uniqueViewers']   = parse_num(v);  continue
        if (v := gv('% From Non-Followers/FYP')) is not None: data['nonFollowerPct'] = parse_pct(v);  continue
        if (v := gv('Avg Watch Time (sec)')) is not None:
            data['watchTimeSec'] = None if (not v or v == '—') else (float(v) if v.replace('.','').isdigit() else None)
            continue
        if (v := gv('Avg Watch Time %'))    is not None: data['watchTimePct']   = parse_pct(v);  continue
        if (v := gv('Saves'))               is not None: data['saves']           = parse_num(v);  continue
        if (v := gv('Shares'))              is not None: data['shares']          = parse_num(v);  continue
        if (v := gv('Comments'))            is not None: data['comments']        = parse_num(v);  continue
        if (v := gv('Follows Attributed'))  is not None: data['follows']         = parse_num(v);  continue

        # Calculated rates
        if (v := gv('Save Rate'))           is not None: data['saveRate']        = parse_pct(v);  continue
        if (v := gv('Share Rate'))          is not None: data['shareRate']       = parse_pct(v);  continue
        if (v := gv('Engagement Rate'))     is not None: data['engagementRate']  = parse_pct(v);  continue
        if (v := gv('Follows per 1K'))      is not None: data['followsPer1k']    = parse_pct(v);  continue
        if (v := gv('Long-Tail Views'))     is not None: data['longTailViews']   = parse_num(v);  continue
        if (v := gv('Long-Tail %'))         is not None: data['longTailPct']     = parse_pct(v);  continue

        # Tags
        if (v := gv('Distribution'))        is not None:
            if v and v != '—': data['distribution'] = v;  continue
        if (v := gv('Outcome'))             is not None:
            if v and v != '—': data['outcome'] = v;  continue
        if (v := gv('Engagement Quality'))  is not None:
            if v and v != '—': data['engagementQuality'] = v;  continue
        if (v := gv('Business Signal'))     is not None:
            if v and v != '—': data['businessSignal'] = v;  continue
        if (v := gv('Next Action'))         is not None:
            if v and v != '—': data['nextAction'] = v;  continue
        if (v := gv('30d Distribution Type')) is not None:
            if v and v != '—': data['distributionType'] = v;  continue

    notes = '\n'.join(notes_lines).strip()
    if notes:
        data['notes'] = notes

    return data

# ── File parser ───────────────────────────────────────────────────────────────

def parse_file(filepath, default_platform):
    path = Path(filepath)
    if not path.exists():
        return []

    sections = re.split(r'\n---\n', path.read_text())
    entries = []

    for section in sections:
        lines = section.strip().split('\n')

        # Find header line
        header_line = header_idx = None
        for i, line in enumerate(lines):
            if line.startswith('## '):
                header_line = line[3:].strip()
                header_idx = i
                break
        if not header_line:
            continue

        # Parse "CLIENT — PLATFORM — TITLE"
        parts = header_line.split(' \u2014 ', 2)
        if len(parts) < 3:
            parts = [p.strip() for p in header_line.split('\u2014', 2)]
        if len(parts) < 3:
            print(f"WARNING: Cannot parse header: {header_line}", file=sys.stderr)
            continue

        client, platform_str, title = parts[0].strip(), parts[1].strip(), parts[2].strip()
        platform = {'Instagram': 'ig', 'TikTok': 'tt'}.get(platform_str, default_platform)

        check_type = post_date = explicit_type = None
        for line in lines[header_idx + 1:]:
            ls = line.strip()
            if ls.startswith('Check:'):
                cs = ls[6:].strip()
                check_type = '24h' if '24h' in cs else '7d' if '7d' in cs else '30d' if '30d' in cs else None
            elif ls.startswith('Post date:'):
                post_date = parse_date(ls[10:].strip())
            elif ls.startswith('Type:'):
                v = ls[5:].strip()
                if v and v != '—':
                    explicit_type = v

        if not check_type or not post_date:
            continue

        data = parse_section(lines[header_idx + 1:])
        entries.append({
            'client': client,
            'platform': platform,
            'title': title,
            'post_date': post_date,
            'check_type': check_type,
            'explicit_type': explicit_type,
            'data': data,
        })

    return entries

# ── JS serialiser ─────────────────────────────────────────────────────────────

def js_key(k):
    """Quote keys that start with a digit or are otherwise not valid identifiers."""
    if not k or k[0].isdigit() or not re.match(r'^[A-Za-z_$][A-Za-z0-9_$]*$', k):
        return f"'{k}'"
    return k

def to_js_value(v):
    if v is None:              return 'null'
    if isinstance(v, bool):    return 'true' if v else 'false'
    if isinstance(v, str):     return "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"
    if isinstance(v, float):   return str(v)
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

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # Step 1: Parse all files
    all_entries = []
    for filepath, default_platform in REPORT_FILES:
        all_entries.extend(parse_file(filepath, default_platform))

    # Step 2: Group by (client, platform, title, post_date)
    groups = {}
    for e in all_entries:
        key = (e['client'], e['platform'], e['title'], e['post_date'].date())
        if key not in groups:
            groups[key] = {
                'client':        e['client'],
                'platform':      e['platform'],
                'title':         e['title'],
                'post_date':     e['post_date'],
                'explicit_type': e['explicit_type'],
                'checks':        {},
            }
        elif e['explicit_type'] and not groups[key]['explicit_type']:
            groups[key]['explicit_type'] = e['explicit_type']
        groups[key]['checks'][e['check_type']] = e['data']

    # Step 3: Build post objects
    posts = []
    for group in groups.values():
        post_date = group['post_date']
        checks    = group['checks']

        due = {}
        if '24h' not in checks:
            due['24h'] = due_date(post_date + timedelta(days=1))
        elif '7d' not in checks:
            due['7d']  = due_date(post_date + timedelta(days=7))
        elif '30d' not in checks:
            due['30d'] = due_date(post_date + timedelta(days=30))

        posts.append((post_date, {
            'id':        0,                        # renumbered after sort
            'client':    group['client'],
            'platform':  group['platform'],
            'title':     group['title'],
            'type':      group['explicit_type'] or infer_type(group['title']),
            'date':      iso_date(post_date),           # ← ISO 8601
            'checks':    checks,
            'dueChecks': due,
        }))

    # Step 4: Sort newest first, assign IDs
    posts.sort(key=lambda x: x[0], reverse=True)
    posts = [p for _, p in posts]
    for i, p in enumerate(posts):
        p['id'] = i + 1

    # Step 5: Print summary
    by_client = {}
    for p in posts:
        by_client[p['client']] = by_client.get(p['client'], 0) + 1
    print(f"Parsed {len(posts)} posts total")
    for client, count in sorted(by_client.items()):
        print(f"  {client}: {count}")

    missing = [p for p in posts if not p.get('client') or not p.get('title')]
    if missing:
        print(f"\nERROR: {len(missing)} posts missing client/title — aborting", file=sys.stderr)
        sys.exit(1)
    print("No issues found.")

    # Step 6: Inject into index.html
    content = INDEX_HTML.read_text()
    js = build_posts_js(posts)
    new_block = f'// DATA_START\n{js}\n// DATA_END'

    if '// DATA_START' not in content or '// DATA_END' not in content:
        print("ERROR: DATA_START/DATA_END markers not found in index.html", file=sys.stderr)
        sys.exit(1)

    new_content = re.sub(r'// DATA_START\n.*?// DATA_END', new_block, content, flags=re.DOTALL)

    if new_content == content:
        print("index.html already up to date — no changes written.")
    else:
        INDEX_HTML.write_text(new_content)
        print(f"index.html updated ({len(posts)} posts injected).")

if __name__ == '__main__':
    main()
