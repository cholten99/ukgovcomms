#!/usr/bin/env python3
"""
Generate a repository overview Markdown file.

It will:
- Describe known top-level folders (unknowns are skipped entirely)
- Summarise scripts in tools/ (description + CLI flags if found)
- Summarise templates/ (HTML <title> if present)
- Parse Flask routes from app.py
- Note key config files (wsgi.py, requirements.txt, GitHub workflows)

Usage:
  python3 tools/generate_repo_docs.py --root . --out exports/repo_overview.md --log-level INFO
"""

import os
import re
import ast
import sys
import argparse
import datetime
from pathlib import Path

# ---------- helpers ----------
def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""

def ensure_parent(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def first_line(s: str, maxlen=300) -> str:
    s = (s or "").strip().splitlines()[0] if s else ""
    return (s[: maxlen - 1] + "…") if len(s) > maxlen else s

def ksort_unique(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return sorted(out)

# ---------- folder descriptions (only these will be listed) ----------
KNOWN_DIR_DESCRIPTIONS = {
    "tools": "Command-line utilities: crawlers, renderers, exporters, health/check scripts.",
    "tools/legacy": "Older one-off scripts kept for reference.",
    "templates": "Flask/Jinja2 HTML templates used to render pages.",
    "static": "Static assets (CSS, JS, images).",
    "assets": "Generated visualisations and wordclouds (per-source and global).",
    "exports": "Exported CSV/ZIP bundles and generated documents.",
    "logs": "Runtime logs from crawlers/renderers.",
    "charts": "Legacy chart output (mostly superseded by assets/).",
    "tmp": "Temporary scratch space.",
    ".github/workflows": "GitHub Actions workflows (deploy, docs, etc.).",
}

# ---------- tools/*.py parsing ----------
ARGPARSE_DESC_RE = re.compile(
    r"ArgumentParser\([^)]*?description\s*=\s*(?P<q>['\"])(?P<desc>.*?)(?P=q)",
    re.S,
)
ADD_ARGUMENT_FLAG_RE = re.compile(
    r"\.add_argument\(\s*(?P<q>['\"])(?P<flag>--?[A-Za-z0-9][-\w]*)\1"
)

def summarize_tool_script(path: Path):
    """Return (description, flags[]) from a tools/*.py file."""
    src = read_text(path)
    desc = None
    m = ARGPARSE_DESC_RE.search(src)
    if m:
        desc = m.group("desc").strip()

    flags = [m.group("flag") for m in ADD_ARGUMENT_FLAG_RE.finditer(src)]
    flags = [f for f in ksort_unique(flags) if f not in ("-h", "--help")]

    if desc is None:
        try:
            mod = ast.parse(src)
            desc = ast.get_docstring(mod)
        except Exception:
            pass
    return (first_line(desc or ""), flags)

# ---------- templates/*.html parsing ----------
TITLE_RE = re.compile(r"<title>(?P<title>.*?)</title>", re.I | re.S)

def summarize_template(path: Path):
    html = read_text(path)
    m = TITLE_RE.search(html)
    if m:
        title = re.sub(r"\s+", " ", m.group("title")).strip()
    else:
        m2 = re.search(r"<h1[^>]*>(?P<h>.*?)</h1>", html, re.I | re.S)
        title = re.sub(r"\s+", " ", m2.group("h")).strip() if m2 else ""
    return title

# ---------- app.py Flask routes parsing ----------
HTTP_DECOS = {"route": None, "get": ["GET"], "post": ["POST"], "put": ["PUT"], "delete": ["DELETE"], "patch": ["PATCH"]}

def summarize_flask_routes(app_py: Path):
    """Return list of dicts: {rule, methods[], func, doc}."""
    src = read_text(app_py)
    if not src:
        return []
    try:
        tree = ast.parse(src)
    except Exception:
        return []

    routes = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            doc = ast.get_docstring(node) or ""
            for deco in node.decorator_list:
                if isinstance(deco, ast.Call) and isinstance(deco.func, ast.Attribute):
                    attr = deco.func.attr  # 'route', 'get', etc.
                    base_ok = isinstance(deco.func.value, ast.Name) and deco.func.value.id == "app"
                    if not base_ok or attr not in HTTP_DECOS:
                        continue

                    rule = None
                    if deco.args:
                        a0 = deco.args[0]
                        if isinstance(a0, ast.Constant) and isinstance(a0.value, str):
                            rule = a0.value

                    methods = None
                    for kw in deco.keywords or []:
                        if kw.arg == "methods":
                            if isinstance(kw.value, (ast.List, ast.Tuple)):
                                vals = []
                                for elt in kw.value.elts:
                                    if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                                        vals.append(elt.value.upper())
                                methods = vals or None
                    if methods is None:
                        methods = HTTP_DECOS[attr] or ["GET"]

                    routes.append({
                        "rule": rule or "(dynamic)",
                        "methods": methods,
                        "func": node.name,
                        "doc": first_line(doc, 200),
                    })
    routes.sort(key=lambda r: r["rule"])
    return routes

# ---------- render markdown ----------
def render_markdown(root: Path, out: Path, log):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = []
    lines.append(f"# UKGovComms – Repository Overview")
    lines.append(f"_Generated: {now}_")
    lines.append("")
    lines.append("This document was generated automatically by `tools/generate_repo_docs.py`.")
    lines.append("")

    # Top-level folders — ONLY list known/accurately described ones
    lines.append("## Top-level folders")
    for rel, desc in KNOWN_DIR_DESCRIPTIONS.items():
        p = (root / rel)
        if p.exists() and p.is_dir():
            lines.append(f"- **{rel}/** — {desc}")
    lines.append("")

    # tools/ scripts
    tools_dir = root / "tools"
    if tools_dir.is_dir():
        lines.append("## Command-line tools (`tools/`)")
        tool_files = sorted([p for p in tools_dir.glob("*.py") if p.is_file()])
        if tool_files:
            lines.append("| Script | Description | Notable flags |")
            lines.append("|---|---|---|")
            for f in tool_files:
                desc, flags = summarize_tool_script(f)
                flag_str = " ".join(f"`{x}`" for x in flags) if flags else "—"
                lines.append(f"| `{f.name}` | {desc or '—'} | {flag_str} |")
        else:
            lines.append("_No scripts found in tools/_")
        # legacy tools
        legacy_dir = tools_dir / "legacy"
        if legacy_dir.is_dir():
            leg = sorted([p for p in legacy_dir.glob("*.py")])
            if leg:
                lines.append("")
                lines.append("### Legacy (`tools/legacy/`)")
                for f in leg:
                    desc, flags = summarize_tool_script(f)
                    lines.append(f"- `{f.name}` — {desc or '—'}")
        lines.append("")

    # templates/
    t_dir = root / "templates"
    if t_dir.is_dir():
        lines.append("## Templates (`templates/`)")
        t_files = sorted([p for p in t_dir.glob("*.html")])
        if t_files:
            lines.append("| Template | Title / summary |")
            lines.append("|---|---|")
            for f in t_files:
                title = summarize_template(f)
                lines.append(f"| `{f.name}` | {title or '—'} |")
        else:
            lines.append("_No templates found._")
        lines.append("")

    # app.py routes
    app_py = root / "app.py"
    if app_py.is_file():
        lines.append("## Flask routes (`app.py`)")
        routes = summarize_flask_routes(app_py)
        if routes:
            lines.append("| Route | Methods | View | Docstring |")
            lines.append("|---|---|---|---|")
            for r in routes:
                methods = ", ".join(r["methods"])
                doc = r["doc"] or "—"
                lines.append(f"| `{r['rule']}` | {methods} | `{r['func']}` | {doc} |")
        else:
            lines.append("_No routes found (or could not parse)._")
        lines.append("")

    # Notable files
    lines.append("## Notable files")
    notable = []
    for rel in ("wsgi.py", "requirements.txt"):
        p = root / rel
        if p.is_file():
            notable.append(rel)
    gh_dir = root / ".github" / "workflows"
    if gh_dir.is_dir():
        for f in sorted(gh_dir.glob("*.yml")):
            notable.append(f".github/workflows/{f.name}")
    if notable:
        for n in notable:
            lines.append(f"- `{n}`")
    else:
        lines.append("_None detected._")

    ensure_parent(out)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log(f"Wrote: {out}")

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="Generate a Markdown overview of the repository.")
    ap.add_argument("--root", default=".", help="Repository root (default: .)")
    ap.add_argument("--out", default="exports/repo_overview.md", help="Output Markdown path")
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR"])
    args = ap.parse_args()

    def log(msg):
        if args.log_level in ("DEBUG","INFO"):
            print(msg)

    root = Path(args.root).resolve()
    out = Path(args.out)
    if not root.is_dir():
        print(f"ERROR: root not found: {root}", file=sys.stderr)
        sys.exit(1)

    render_markdown(root, out, log)

if __name__ == "__main__":
    main()

