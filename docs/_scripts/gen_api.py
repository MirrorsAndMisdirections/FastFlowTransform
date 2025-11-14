# docs/_scripts/gen_api.py
from __future__ import annotations
from pathlib import Path
import mkdocs_gen_files
from collections import defaultdict

# -------------------------------------------------------------------
# Configuration
# If you already know the package name, set it here; None => auto-detect.
PACKAGE: str | None = "fastflowtransform"  # adjust as needed or set to None
SRC_DIR = Path("src")
# -------------------------------------------------------------------


def detect_package() -> tuple[str, Path]:
    """
    Returns (package_name, package_root_path).
    Checks src layout first (src/<pkg>/__init__.py), then flat layout (<pkg>/__init__.py).
    """
    candidates: list[tuple[str, str, Path]] = []

    # src layout
    if SRC_DIR.exists():
        for p in SRC_DIR.iterdir():
            if p.is_dir() and (p / "__init__.py").exists():
                candidates.append(("src", p.name, p))

    # Flat layout (at the repository root)
    root = Path(".")
    ignore = {
        "docs",
        "site",
        "build",
        "dist",
        "tests",
        ".git",
        "venv",
        ".venv",
        "src",
        ".mypy_cache",
        ".pytest_cache",
    }
    for p in root.iterdir():
        if p.is_dir() and (p / "__init__.py").exists() and p.name not in ignore:
            candidates.append(("flat", p.name, p))

    if PACKAGE:
        for _, name, path in candidates:
            if name == PACKAGE:
                return name, path
        raise AssertionError(
            f'Package "{PACKAGE}" not found. Expected e.g. src/{PACKAGE}/ or {PACKAGE}/ with __init__.py'
        )

    unique = {(name, str(path)) for _, name, path in candidates}
    if not unique:
        raise AssertionError(
            "No package found. Create src/<package>/__init__.py or set PACKAGE in the script."
        )
    if len(unique) > 1:
        formatted = "\n".join(f"- {name} @ {path}" for name, path in sorted(unique))
        raise AssertionError(
            f"Multiple possible packages found:\n{formatted}\nSet PACKAGE explicitly in the script."
        )
    name, path_str = next(iter(unique))
    return name, Path(path_str)


package, pkg_root = detect_package()
print(f"[gen_api] Detected package: {package}  | Path: {pkg_root}")

generated_files: list[tuple[str, str]] = []  # (module, doc_file)

for path in sorted(pkg_root.rglob("*.py")):
    # Filter out unwanted cache directories
    if any(part in {"__pycache__", ".pytest_cache"} for part in path.parts):
        continue

    rel = path.with_suffix("").relative_to(pkg_root)
    parts = list(rel.parts)

    # Determine module name
    if path.name == "__init__.py":
        module = package + ("" if not parts[:-1] else "." + ".".join(parts[:-1]))
    else:
        module = package + "." + ".".join(parts)

    # Target path (mkdocs_gen_files.open creates directories)
    doc_file = f"reference/{module.replace('.', '/')}.md"
    generated_files.append((module, doc_file))

    with mkdocs_gen_files.open(doc_file, "w") as f:
        f.write(f"# {module}\n\n")
        f.write(f"::: {module}\n")
        f.write("    options:\n")
        f.write("      show_signature: true\n")
        f.write("      filters:\n")
        f.write('        - "!^_"\n')

# Generate index page (paths relative to the reference/ directory)
index_path = "reference/index.md"
with mkdocs_gen_files.open(index_path, "w") as f:
    f.write("# API Reference\n\n")
    f.write("> Auto-generated per module\n\n")
    for module, doc_file in generated_files:
        rel = Path(doc_file).relative_to("reference").as_posix()
        # IMPORTANT: link without 'reference/' prefix
        f.write(f"- [{module}]({rel})\n")

# -------------------------------------------------------------------
# Generate reference/SUMMARY.md for mkdocs-literate-nav
# -------------------------------------------------------------------
# This builds a *structured* nav. If you prefer a flat list, you can
# just copy the loop used for index.md above instead.
reference_root = Path("reference")

# Build a tree: level-1 = top-level package (fastflowtransform),
# level-2 = immediate subpackage/module, then files underneath.
tree: dict[str, list[tuple[str, str]]] = defaultdict(list)
for module, doc_file in generated_files:
    rel = Path(doc_file).relative_to("reference")
    parts = module.split(".")
    # Expect modules like "fastflowtransform", "fastflowtransform.api.http", ...
    group = parts[1] if len(parts) > 1 else parts[0]  # e.g. "api", "config", "executors"
    tree[group].append((module, rel.as_posix()))

# Sort groups and entries for stable nav
for k in list(tree.keys()):
    tree[k].sort(key=lambda x: x[0])

with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as f:
    f.write("# API Reference\n\n")
    # Link to the overview page
    f.write("- [Overview](index.md)\n")
    # Grouped subsections
    for group in sorted(tree.keys()):
        items = tree[group]
        if not items:
            # Safety: never emit an empty section (would break literate-nav)
            continue
        if len(items) == 1:
            # For single-item groups, emit a direct link (no section header)
            module, rel = items[0]
            f.write(f"- [{module}]({rel})\n")
            continue
        # Section header (no link)
        f.write(f"- {group}\n")
        for module, rel in items:
            # Nested items MUST be indented by 4 spaces for literate-nav
            f.write(f"    - [{module}]({rel})\n")
