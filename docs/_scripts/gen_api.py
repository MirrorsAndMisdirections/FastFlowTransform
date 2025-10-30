# docs/_scripts/gen_api.py
from __future__ import annotations
from pathlib import Path
import mkdocs_gen_files

# -------------------------------------------------------------------
# Konfiguration
# Falls du den Namen kennst, trag ihn hier ein; None => Auto-Detect.
PACKAGE: str | None = "fastflowtransform"  # <- bei Bedarf anpassen oder auf None setzen
SRC_DIR = Path("src")
# -------------------------------------------------------------------


def detect_package() -> tuple[str, Path]:
    """
    Liefert (package_name, package_root_path).
    Prüft zuerst src-Layout (src/<pkg>/__init__.py), dann Flat-Layout (<pkg>/__init__.py).
    """
    candidates: list[tuple[str, str, Path]] = []

    # src-Layout
    if SRC_DIR.exists():
        for p in SRC_DIR.iterdir():
            if p.is_dir() and (p / "__init__.py").exists():
                candidates.append(("src", p.name, p))

    # Flat-Layout (im Repo-Root)
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
            f'Paket "{PACKAGE}" nicht gefunden. Erwartet z. B. src/{PACKAGE}/ oder {PACKAGE}/ mit __init__.py'
        )

    unique = {(name, str(path)) for _, name, path in candidates}
    if not unique:
        raise AssertionError(
            "Kein Paket gefunden. Lege src/<paket>/__init__.py an oder setze PACKAGE im Skript."
        )
    if len(unique) > 1:
        formatted = "\n".join(f"- {name} @ {path}" for name, path in sorted(unique))
        raise AssertionError(
            f"Mehrere mögliche Pakete gefunden:\n{formatted}\nSetze PACKAGE explizit im Skript."
        )
    name, path_str = next(iter(unique))
    return name, Path(path_str)


package, pkg_root = detect_package()
print(f"[gen_api] Paket erkannt: {package}  | Pfad: {pkg_root}")

generated_files: list[tuple[str, str]] = []  # (module, doc_file)

for path in sorted(pkg_root.rglob("*.py")):
    # Rausfiltern
    if any(part in {"__pycache__", ".pytest_cache"} for part in path.parts):
        continue

    rel = path.with_suffix("").relative_to(pkg_root)
    parts = list(rel.parts)

    # Modulname bestimmen
    if path.name == "__init__.py":
        module = package + ("" if not parts[:-1] else "." + ".".join(parts[:-1]))
    else:
        module = package + "." + ".".join(parts)

    # Zielpfad (mkdocs_gen_files.open legt Ordner an)
    doc_file = f"reference/{module.replace('.', '/')}.md"
    generated_files.append((module, doc_file))

    with mkdocs_gen_files.open(doc_file, "w") as f:
        f.write(f"# {module}\n\n")
        f.write(f"::: {module}\n")
        f.write("    options:\n")
        f.write("      show_signature: true\n")
        f.write("      filters:\n")
        f.write('        - "!^_"\n')

# Index-Seite erzeugen
index_path = "reference/index.md"
with mkdocs_gen_files.open(index_path, "w") as f:
    f.write("# API Reference\n\n")
    f.write("> Auto-generated per module\n\n")
    for module, doc_file in generated_files:
        rel = Path(doc_file).relative_to("reference").as_posix()
        f.write(f"- [{module}]({rel})\n")
