#!/usr/bin/env python3
# concat_docs.py
"""
Fügt alle Markdown-Dateien aus dem docs-Verzeichnis zu einer einzelnen Datei zusammen.
- Respektiert die Reihenfolge in mkdocs.yml (nav).
- Ignoriert doppelte Einträge / Anker (#...).
- Hängt übrige .md-Dateien (nicht in nav) am Ende an.
- Optional: Headings demoten (um mehrfaches H1 zu vermeiden).

Beispiel:
    python concat_docs.py -o Combined.md
    python concat_docs.py -o Combined.md --demote --exclude "reference/**" --exclude "site/**"
"""

from __future__ import annotations
import argparse
import fnmatch
import os
from pathlib import Path
import re
import sys

try:
    import yaml  # PyYAML
except ImportError:
    yaml = None

DOCS_DIR_DEFAULT = "docs"
MKDOCS_YML = "mkdocs.yml"


def load_nav_order(project_root: Path) -> list[Path]:
    """Liest mkdocs.yml und extrahiert eine geordnete Liste der Markdown-Pfade (ohne Anker)."""
    yml_path = project_root / MKDOCS_YML
    ordered: list[Path] = []
    if yaml is None or not yml_path.exists():
        return ordered  # keine Order-Info -> leere Liste
    data = yaml.safe_load(yml_path.read_text(encoding="utf-8"))
    nav = data.get("nav") if isinstance(data, dict) else None
    if not isinstance(nav, list):
        return ordered

    def normalize_nav_item(item) -> list[str]:
        # item kann dict ({"Title": "path.md" | ["subitems"]}) oder string sein
        out: list[str] = []
        if isinstance(item, str):
            out.append(item)
        elif isinstance(item, dict):
            for _, v in item.items():
                if isinstance(v, str):
                    out.append(v)
                elif isinstance(v, list):
                    for sub in v:
                        out.extend(normalize_nav_item(sub))
        return out

    paths = []
    for entry in nav:
        paths.extend(normalize_nav_item(entry))

    seen = set()
    for p in paths:
        # Nur Dateien unter docs berücksichtigen; Anker entfernen
        p_no_anchor = p.split("#", 1)[0]
        if not p_no_anchor.lower().endswith(".md"):
            continue
        # mkdocs erlaubt relative Pfade; wir interpretieren sie relativ zu docs/
        # Falls der Pfad bereits "docs/..." enthält, normalisieren wir trotzdem
        if p_no_anchor.startswith(DOCS_DIR_DEFAULT + "/"):
            rel = Path(p_no_anchor).relative_to(DOCS_DIR_DEFAULT)
        else:
            rel = Path(p_no_anchor)
        if rel.as_posix() not in seen:
            seen.add(rel.as_posix())
            ordered.append(rel)
    return ordered


def collect_md_files(docs_dir: Path) -> list[Path]:
    return sorted([p.relative_to(docs_dir) for p in docs_dir.rglob("*.md")])


def apply_excludes(paths: list[Path], patterns: list[str]) -> list[Path]:
    if not patterns:
        return paths
    kept = []
    for p in paths:
        posix = p.as_posix()
        if any(fnmatch.fnmatch(posix, pat) for pat in patterns):
            continue
        kept.append(p)
    return kept


def demote_headings(text: str, levels: int = 1) -> str:
    """
    Erhöht die Anzahl der '#' um 'levels' für alle ATX-Headings (Markdown #).
    Lässt Codeblöcke unberührt.
    """
    if levels <= 0:
        return text

    lines = text.splitlines()
    in_code = False
    fence_re = re.compile(r"^(```|~~~)")
    heading_re = re.compile(r"^(#{1,6})\s+")
    for i, line in enumerate(lines):
        if fence_re.match(line.strip()):
            in_code = not in_code
            continue
        if in_code:
            continue
        m = heading_re.match(line)
        if m:
            hashes = m.group(1)
            new_level = min(len(hashes) + levels, 6)
            lines[i] = "#" * new_level + line[m.end(1) :]
    return "\n".join(lines)


def read_file(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="utf-8", errors="replace")


def main():
    parser = argparse.ArgumentParser(
        description="Concatenate Markdown files from docs/ into a single file."
    )
    parser.add_argument(
        "-d",
        "--docs-dir",
        default=DOCS_DIR_DEFAULT,
        help="Pfad zum docs-Verzeichnis (Default: docs)",
    )
    parser.add_argument("-o", "--output", required=True, help="Ausgabedatei (z. B. Combined.md)")
    parser.add_argument(
        "--demote",
        action="store_true",
        help="Headings ab der zweiten Datei um eine Ebene demoten (# -> ##, usw.)",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Glob-Pattern zum Ausschließen (z. B. 'reference/**'). Mehrfach nutzbar.",
    )
    parser.add_argument(
        "--no-nav",
        action="store_true",
        help="mkdocs.yml ignorieren und alphabetisch alle .md zusammenfügen",
    )
    args = parser.parse_args()

    project_root = Path(".").resolve()
    docs_dir = (project_root / args.docs_dir).resolve()
    if not docs_dir.exists():
        print(f"Fehler: docs-Verzeichnis nicht gefunden: {docs_dir}", file=sys.stderr)
        sys.exit(1)

    # 1) Reihenfolge aus mkdocs.yml (falls nicht deaktiviert / vorhanden)
    nav_order = load_nav_order(project_root) if not args.no_nav else []
    all_md = collect_md_files(docs_dir)
    all_md = apply_excludes(all_md, args.exclude)

    # 2) Liste zusammenstellen: zuerst nav, dann Rest (ohne Duplikate)
    ordered: list[Path] = []
    seen = set()
    for rel in nav_order:
        if rel in all_md and rel.as_posix() not in seen:
            ordered.append(rel)
            seen.add(rel.as_posix())
    for rel in all_md:
        if rel.as_posix() not in seen:
            ordered.append(rel)
            seen.add(rel.as_posix())

    if not ordered:
        print("Keine Markdown-Dateien gefunden.", file=sys.stderr)
        sys.exit(2)

    out_path = Path(args.output).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    parts = []
    for i, rel in enumerate(ordered, start=1):
        src = docs_dir / rel
        content = read_file(src)
        if i > 1 and args.demote:
            content = demote_headings(content, levels=1)

        header = f"\n\n<!-- >>> FILE: {rel.as_posix()} >>> -->\n\n"
        parts.append(header + content.strip() + "\n")

    out_text = f"# Combined Documentation\n\n" + "\n".join(parts)
    out_path.write_text(out_text, encoding="utf-8")
    print(f"✔️  {len(ordered)} Dateien zusammengeführt → {out_path}")


if __name__ == "__main__":
    main()
