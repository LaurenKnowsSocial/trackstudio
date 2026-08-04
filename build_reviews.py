#!/usr/bin/env python3
"""build_reviews.py — renders reviews/*.md into the REVIEWS block of index.html.

Each review is a markdown file named <CLIENT>-<YYYY-MM>-<audience>.md with an HTML
comment metadata block near the top:

    <!-- meta
    client: BTC
    period: 2026-07
    audience: client | internal
    title: July 2026
    published: 2026-08-04
    summary: one line for the index card
    -->

SAFETY: this repo is PUBLIC and GitHub Pages serves it to anyone with the URL. The
access keys in index.html are obfuscation, not access control — everything shipped into
the page is readable by anyone who opens it. So only `audience: client` reviews are
published. Internal write-ups stay out of this repo entirely (see .gitignore) and live
with the client's other internal deliverables.

Run:  python3 ~/trackstudio/build_reviews.py
      python3 ~/trackstudio/build_reviews.py --push
"""

import html
import json
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).parent
REVIEWS_DIR = HERE / "reviews"
INDEX_HTML = HERE / "index.html"
START = "// REVIEWS_START"
END = "// REVIEWS_END"

META_RE = re.compile(r"<!--\s*meta\s*(.*?)-->", re.S)
# Markdown image paths are relative to the .md file (reviews/img/…), but index.html is
# served from the repo root, so rewrite them on the way into the page.
WEB_PREFIX = "reviews/"
INLINE_RE = re.compile(r"(\*\*.+?\*\*|(?<!\*)\*[^*]+?\*(?!\*)|`[^`]+?`|!\[[^\]]*\]\([^)]+\)|\[[^\]]+\]\([^)]+\))")
IMG_RE = re.compile(r"!\[([^\]]*)\]\(([^)]+)\)")
LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")


def parse_meta(text):
    m = META_RE.search(text)
    if not m:
        raise ValueError("no <!-- meta --> block")
    meta = {}
    for line in m.group(1).strip().split("\n"):
        if ":" in line:
            k, v = line.split(":", 1)
            meta[k.strip()] = v.strip()
    return meta, META_RE.sub("", text, count=1)


def webpath(src):
    """Resolve a markdown-relative image path against the site root."""
    if src.startswith(("http://", "https://", "/", WEB_PREFIX)):
        return src
    return WEB_PREFIX + src.lstrip("./")


def inline(s):
    """Render inline markdown to HTML. Escapes first so post copy can't inject tags."""
    out = []
    for piece in INLINE_RE.split(s):
        if not piece:
            continue
        if piece.startswith("**") and piece.endswith("**"):
            out.append(f"<strong>{html.escape(piece[2:-2])}</strong>")
        elif piece.startswith("`") and piece.endswith("`"):
            out.append(f"<code>{html.escape(piece[1:-1])}</code>")
        elif piece.startswith("!["):
            m = IMG_RE.fullmatch(piece)
            out.append(
                f'<img src="{html.escape(webpath(m.group(2)))}" alt="{html.escape(m.group(1))}" loading="lazy">'
            )
        elif piece.startswith("["):
            m = LINK_RE.fullmatch(piece)
            out.append(
                f'<a href="{html.escape(m.group(2))}" target="_blank" rel="noopener">{html.escape(m.group(1))}</a>'
            )
        elif piece.startswith("*") and piece.endswith("*"):
            out.append(f"<em>{html.escape(piece[1:-1])}</em>")
        else:
            out.append(html.escape(piece))
    return "".join(out)


def split_row(line):
    return [c.strip() for c in line.strip().strip("|").split("|")]


def to_html(md):
    lines = md.split("\n")
    out, i = [], 0
    para, quote, bullets = [], [], []

    def flush():
        if para:
            out.append(f"<p>{inline(' '.join(para))}</p>")
            para.clear()
        if quote:
            out.append(f"<blockquote>{inline(' '.join(quote))}</blockquote>")
            quote.clear()
        if bullets:
            items = "".join(f"<li>{inline(b)}</li>" for b in bullets)
            out.append(f"<ul>{items}</ul>")
            bullets.clear()

    while i < len(lines):
        s = lines[i].strip()

        if s.startswith("|") and i + 1 < len(lines) and set(lines[i + 1].strip()) <= set("|-: "):
            flush()
            head = split_row(s)
            i += 2
            body = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                body.append(split_row(lines[i]))
                i += 1
            th = "".join(f"<th>{inline(c)}</th>" for c in head)
            trs = "".join(
                "<tr>" + "".join(f"<td>{inline(c)}</td>" for c in r[: len(head)]) + "</tr>"
                for r in body
            )
            out.append(f'<div class="rv-tw"><table><thead><tr>{th}</tr></thead><tbody>{trs}</tbody></table></div>')
            continue

        if not s:
            flush()
        elif s == "---":
            flush()
            out.append("<hr>")
        elif s.startswith("#"):
            flush()
            lvl = len(s) - len(s.lstrip("#"))
            out.append(f"<h{min(lvl + 1, 6)}>{inline(s.lstrip('#').strip())}</h{min(lvl + 1, 6)}>")
        elif s.startswith("> "):
            if para:
                flush()
            quote.append(s[2:])
        elif s.startswith("- "):
            if para or quote:
                flush()
            bullets.append(s[2:])
        elif IMG_RE.fullmatch(s):
            flush()
            m = IMG_RE.fullmatch(s)
            cap = f'<figcaption>{inline(m.group(1))}</figcaption>' if m.group(1) else ""
            out.append(
                f'<figure><img src="{html.escape(webpath(m.group(2)))}" alt="{html.escape(m.group(1))}" loading="lazy">{cap}</figure>'
            )
        else:
            if quote or bullets:
                flush()
            para.append(s)
        i += 1

    flush()
    return "".join(out)


def main():
    if not REVIEWS_DIR.exists():
        sys.exit(f"no reviews dir at {REVIEWS_DIR}")

    merged = {}
    for p in sorted(REVIEWS_DIR.glob("*.md")):
        meta, body = parse_meta(p.read_text())
        for k in ("client", "period", "audience", "title", "published"):
            if k not in meta:
                sys.exit(f"{p.name}: missing '{k}' in meta block")
        key = f"{meta['client']}-{meta['period']}"
        e = merged.setdefault(
            key,
            {
                "id": key,
                "client": meta["client"],
                "period": meta["period"],
                "title": meta["title"],
                "published": meta["published"],
                "summary": "",
                "body": "",
            },
        )
        if meta["audience"] != "client":
            sys.exit(
                f"{p.name}: audience is '{meta['audience']}'. This repo is public — only "
                f"client-facing reviews may be published. Move it out of reviews/."
            )
        e["body"] = to_html(body)
        e["summary"] = meta.get("summary", "")
        print(f"  {p.name}: {len(body.split()):,} words")

    entries = sorted(merged.values(), key=lambda e: e["period"], reverse=True)

    js = json.dumps(entries, ensure_ascii=False, indent=1)
    src = INDEX_HTML.read_text()
    if START not in src or END not in src:
        sys.exit("index.html is missing the REVIEWS_START/REVIEWS_END markers")
    pre, rest = src.split(START, 1)
    _, post = rest.split(END, 1)
    INDEX_HTML.write_text(f"{pre}{START}\nconst REVIEWS = {js};\n{END}{post}")

    print(f"\nwrote {len(entries)} review(s) into {INDEX_HTML.name}")
    for e in entries:
        print(f"  {e['id']}  {e['title']}")

    if "--push" in sys.argv:
        subprocess.run(["git", "-C", str(HERE), "add", "index.html", "reviews"], check=True)
        subprocess.run(
            ["git", "-C", str(HERE), "commit", "-m", f"dashboard: reviews archive ({len(entries)} entries)"],
            check=True,
        )
        subprocess.run(["git", "-C", str(HERE), "push"], check=True)
        print("pushed")


if __name__ == "__main__":
    main()
