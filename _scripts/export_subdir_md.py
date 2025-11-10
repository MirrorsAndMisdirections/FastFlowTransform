#!/usr/bin/env python3
import argparse
import subprocess
from pathlib import Path


def get_git_root() -> Path:
    """Return the root directory of the current Git repository."""
    try:
        out = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
        return Path(out)
    except subprocess.CalledProcessError:
        raise SystemExit("Error: This script must be run inside a Git repository.")


def get_git_files(git_root: Path) -> list[Path]:
    """
    Return all files that are not ignored by Git
    (tracked + untracked, but excluding standard ignored files).
    """
    try:
        out = subprocess.check_output(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
            text=True,
            cwd=git_root,
        )
    except subprocess.CalledProcessError as e:
        raise SystemExit(f"Error while running 'git ls-files': {e}")
    paths = [git_root / line.strip() for line in out.splitlines() if line.strip()]
    return paths


def is_under_dir(path: Path, directory: Path) -> bool:
    """Return True if 'path' is located under 'directory'."""
    try:
        path.relative_to(directory)
        return True
    except ValueError:
        return False


def is_binary_file(path: Path, chunk_size: int = 2048) -> bool:
    """
    Simple heuristic to check if a file is binary.

    Reads the first 'chunk_size' bytes and checks for NUL bytes or
    decoding errors when interpreting as UTF-8.
    """
    try:
        with path.open("rb") as f:
            chunk = f.read(chunk_size)
        # NUL byte or decode error => treat as binary
        if b"\0" in chunk:
            return True
        try:
            chunk.decode("utf-8")
        except UnicodeDecodeError:
            return True
        return False
    except OSError:
        # If file cannot be read for some reason, treat it as binary
        return True


def build_tree_structure(files: list[Path], base_dir: Path) -> str:
    """
    Build a textual tree structure relative to 'base_dir'.

    'files' should be the list of all files under 'base_dir'.
    """
    # Work with paths relative to base_dir
    rel_paths = [f.relative_to(base_dir) for f in files]
    # Nested dict-based tree representation
    tree = {}

    for rel in rel_paths:
        parts = rel.parts
        current = tree
        for part in parts[:-1]:
            current = current.setdefault(part + "/", {})
        # Store files under special key
        current.setdefault("__files__", []).append(parts[-1])

    lines = []
    root_name = base_dir.name + "/"
    lines.append(root_name)

    def walk(node: dict, prefix: str = "  "):
        """Recursively traverse the tree and build the text representation."""
        # Files in the current directory
        files_here = sorted(node.get("__files__", []))
        for fname in files_here:
            lines.append(f"{prefix}{fname}")
        # Subdirectories
        for key in sorted(k for k in node.keys() if k != "__files__"):
            lines.append(f"{prefix}{key}")
            walk(node[key], prefix + "  ")

    walk(tree)
    return "\n".join(lines)


def normalize_ext_list(exts: list[str]) -> set[str]:
    """
    Normalize a list of file extensions:

    - ensure each starts with a dot (.)
    - convert all to lowercase
    """
    norm = set()
    for e in exts:
        e = e.strip()
        if not e:
            continue
        if not e.startswith("."):
            e = "." + e
        norm.add(e.lower())
    return norm


def main():
    parser = argparse.ArgumentParser(
        description="Concatenate the contents of all non-ignored files in a subdirectory into a Markdown file."
    )
    parser.add_argument(
        "subdir", help="Subdirectory inside the Git repository (relative or absolute)."
    )
    parser.add_argument(
        "-o",
        "--output",
        default="combined.md",
        help="Path to the output Markdown file (default: combined.md)",
    )
    parser.add_argument(
        "--exclude-ext",
        nargs="*",
        default=[],
        help="File extensions to exclude, e.g. --exclude-ext .html .css js",
    )
    args = parser.parse_args()

    git_root = get_git_root()
    subdir_path = Path(args.subdir).resolve()

    # Ensure that the given subdirectory is inside the Git repository
    if not is_under_dir(subdir_path, git_root):
        raise SystemExit(
            f"Error: The given subdirectory is not inside the Git repository: {subdir_path}"
        )

    if not subdir_path.is_dir():
        raise SystemExit(f"Error: {subdir_path} is not a directory.")

    all_git_files = get_git_files(git_root)

    # Filter to files under the given subdirectory
    files_in_subdir = [f for f in all_git_files if is_under_dir(f, subdir_path) and f.is_file()]

    # Normalize and apply excluded extensions
    excluded_exts = normalize_ext_list(args.exclude_ext)
    if excluded_exts:
        files_in_subdir = [f for f in files_in_subdir if f.suffix.lower() not in excluded_exts]

    files_in_subdir = sorted(files_in_subdir)

    if not files_in_subdir:
        raise SystemExit(
            "No matching files found in the subdirectory (or all are excluded/ignored)."
        )

    # Build directory tree for Markdown
    tree_md = build_tree_structure(files_in_subdir, subdir_path)

    output_path = Path(args.output).resolve()

    skipped_binary = []

    with output_path.open("w", encoding="utf-8") as out:
        # Title
        out.write(f"# Export from `{subdir_path.relative_to(git_root)}`\n\n")

        # Directory structure
        out.write("## Directory structure\n\n")
        out.write("```text\n")
        out.write(tree_md)
        out.write("\n```\n\n")

        # Files
        out.write("## Files\n\n")

        for file_path in files_in_subdir:
            rel = file_path.relative_to(git_root)
            if is_binary_file(file_path):
                skipped_binary.append(rel)
                continue

            out.write(f"### `{rel}`\n\n")
            out.write("```text\n")
            try:
                content = file_path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                skipped_binary.append(rel)
                out.write("[File could not be read as UTF-8]\n")
                out.write("```\n\n")
                continue
            out.write(content)
            if not content.endswith("\n"):
                out.write("\n")
            out.write("```\n\n")

        if skipped_binary:
            out.write("## Skipped files (binary or not readable)\n\n")
            for rel in skipped_binary:
                out.write(f"- `{rel}`\n")

    print(f"Done! Output written to: {output_path}")


if __name__ == "__main__":
    main()
