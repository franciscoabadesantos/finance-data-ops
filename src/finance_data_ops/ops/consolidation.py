"""Project file aggregation utilities."""

from __future__ import annotations

import argparse
import os
from collections import defaultdict
from collections.abc import Iterable


def _excluded_dirs() -> set[str]:
    return {
        "__pycache__",
        ".git",
        ".idea",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".venv",
        ".venv-finance",
        "env",
        "venv",
        "virtualenv",
        "node_modules",
        "data_cache",
    }


def collect_project_files(
    root_dir: str,
    mode: str = "all",
    extensions: Iterable[str] | None = None,
    include_experiments: bool = True,
    include_results: bool = True,
    output_dir: str | None = None,
) -> tuple[list[tuple[str, str]], set[str]]:
    """Walk through a directory and gather matching files."""
    if mode not in {"all", "no-tests", "tests-only"}:
        raise ValueError("mode must be one of {'all', 'no-tests', 'tests-only'}")

    excluded_dirs = _excluded_dirs()
    if not include_experiments:
        excluded_dirs.add("experiments")
    if not include_results:
        excluded_dirs.add("results")
        excluded_dirs.add("logs")

    allowed_extensions = tuple(extensions) if extensions else None
    project_files: list[tuple[str, str]] = []
    discovered_dirs: set[str] = set()

    output_dir_abs = os.path.abspath(output_dir) if output_dir else None

    for dirpath, dirnames, filenames in os.walk(root_dir):
        filtered_dirnames: list[str] = []
        for dirname in dirnames:
            if dirname in excluded_dirs or dirname == "site-packages":
                continue
            if output_dir_abs:
                candidate = os.path.abspath(os.path.join(dirpath, dirname))
                if candidate == output_dir_abs or candidate.startswith(output_dir_abs + os.sep):
                    continue
            filtered_dirnames.append(dirname)
        dirnames[:] = filtered_dirnames

        rel_dir = os.path.relpath(dirpath, root_dir)
        in_tests_dir = rel_dir == "tests" or rel_dir.startswith(f"tests{os.sep}")
        in_github = rel_dir == ".github" or rel_dir.startswith(f".github{os.sep}")

        if mode == "no-tests" and in_tests_dir:
            dirnames[:] = []
            continue

        if mode == "tests-only":
            if rel_dir == ".":
                dirnames[:] = [d for d in dirnames if d in {"tests", ".github"}]
            elif not (in_tests_dir or in_github):
                dirnames[:] = []
                continue

        discovered_dirs.add(rel_dir)

        for filename in filenames:
            if allowed_extensions is None:
                include_file = True
            else:
                include_file = True if in_github else filename.endswith(allowed_extensions)

            if not include_file:
                continue

            file_path = os.path.join(dirpath, filename)
            relative_path = os.path.relpath(file_path, root_dir)

            if mode == "no-tests" and in_tests_dir:
                continue
            if mode == "tests-only" and not (in_tests_dir or in_github):
                continue

            project_files.append((file_path, relative_path))

    project_files.sort(key=lambda pair: pair[1])
    return project_files, discovered_dirs


def folder_output_name(relative_folder: str) -> str:
    if relative_folder in {".", ""}:
        return "folder__root.txt"
    safe = relative_folder.replace(os.sep, "__").replace("/", "__").replace("\\", "__")
    return f"folder__{safe}.txt"


def top_level_group(relative_path: str) -> str:
    normalized = relative_path.replace("\\", os.sep).replace("/", os.sep)
    if os.sep not in normalized:
        return "."
    parts = normalized.split(os.sep)
    head = parts[0]

    if head == "scripts":
        return "scripts"

    if head == "src":
        return os.path.dirname(normalized) or "src"

    return head


def build_project_tree(root_dir: str, excluded_dirs: set[str], output_dir: str | None = None) -> dict[str, object]:
    tree: dict[str, object] = {}
    output_dir_abs = os.path.abspath(output_dir) if output_dir else None

    for dirpath, dirnames, filenames in os.walk(root_dir):
        filtered_dirnames: list[str] = []
        for dirname in dirnames:
            if dirname in excluded_dirs or dirname == "site-packages":
                continue
            if output_dir_abs:
                candidate = os.path.abspath(os.path.join(dirpath, dirname))
                if candidate == output_dir_abs or candidate.startswith(output_dir_abs + os.sep):
                    continue
            filtered_dirnames.append(dirname)
        dirnames[:] = filtered_dirnames

        rel_dir = os.path.relpath(dirpath, root_dir)
        parts = [] if rel_dir == "." else rel_dir.split(os.sep)

        node: dict[str, object] = tree
        for part in parts:
            node = node.setdefault(f"{part}/", {})  # type: ignore[assignment]

        for directory in dirnames:
            node.setdefault(f"{directory}/", {})
        for filename in sorted(filenames):
            node[filename] = None

    return tree


def write_tree(node: dict[str, object], outfile, indent: str = "") -> None:
    for name in sorted(node):
        outfile.write(f"{indent}- {name}\n")
        child = node[name]
        if isinstance(child, dict):
            write_tree(child, outfile, indent + "  ")


def consolidate_project_by_folder(
    root_dir: str,
    output_dir: str,
    mode: str = "all",
    extensions: Iterable[str] | None = None,
    include_experiments: bool = True,
    include_results: bool = True,
) -> None:
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    excluded_dirs = _excluded_dirs()
    if not include_experiments:
        excluded_dirs.add("experiments")
    if not include_results:
        excluded_dirs.add("results")
        excluded_dirs.add("logs")

    project_files, _ = collect_project_files(
        root_dir=root_dir,
        mode=mode,
        extensions=extensions,
        include_experiments=include_experiments,
        include_results=include_results,
        output_dir=output_dir,
    )
    files_by_folder: defaultdict[str, list[tuple[str, str]]] = defaultdict(list)
    for file_path, relative_path in project_files:
        folder = top_level_group(relative_path)
        files_by_folder[folder].append((file_path, relative_path))

    written_files = 0
    for folder in sorted(files_by_folder):
        output_name = folder_output_name(folder)
        output_path = os.path.join(output_dir, output_name)
        files_in_folder = files_by_folder[folder]

        with open(output_path, "w", encoding="utf-8") as outfile:
            outfile.write(f"# Top-level folder: {folder}\n")
            outfile.write(f"# Source files aggregated: {len(files_in_folder)}\n\n")
            for file_path, relative_path in files_in_folder:
                source_folder = os.path.dirname(relative_path) or "."
                outfile.write(f"# {'-' * 20}\n")
                outfile.write(f"# File: {relative_path}\n")
                outfile.write(f"# Source folder: {source_folder}\n")
                outfile.write(f"# {'-' * 20}\n\n")
                try:
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as infile:
                        outfile.write(infile.read())
                        outfile.write("\n\n")
                except Exception as exc:  # pragma: no cover - defensive
                    outfile.write(f"# Error reading file {relative_path}: {exc}\n\n")
        written_files += 1

    organization_path = os.path.join(output_dir, "project_organization.txt")
    project_tree = build_project_tree(root_dir, excluded_dirs, output_dir=output_dir)
    with open(organization_path, "w", encoding="utf-8") as outfile:
        outfile.write("# Project organization\n")
        outfile.write(f"# Root: {os.path.abspath(root_dir)}\n\n")
        write_tree(project_tree, outfile)

    summary_bits = [f"mode='{mode}'"]
    if extensions:
        summary_bits.append(f"extensions={','.join(extensions)}")
    else:
        summary_bits.append("extensions=ALL")
    if not include_experiments:
        summary_bits.append("exclude experiments")
    if not include_results:
        summary_bits.append("exclude aux logs")
    summary = ", ".join(summary_bits)
    print(
        f"Created {written_files} folder aggregation files in '{output_dir}' "
        f"plus 'project_organization.txt' ({summary})."
    )


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(
        description="Create top-level-folder aggregation text files for the project."
    )
    parser.add_argument(
        "--root",
        default=os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
        help="Root directory to search from (default: project root)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory for aggregation outputs (default: <root>/aggregation).",
    )
    parser.add_argument(
        "--mode",
        choices=["all", "no-tests", "tests-only"],
        default="all",
        help="Which files to include: all files, exclude tests, or only tests.",
    )
    parser.add_argument(
        "--ext",
        nargs="*",
        help="Optional list of file extensions to include (e.g., --ext .py .toml .yml). Default: include all files.",
    )
    parser.add_argument(
        "--exclude-experiments",
        action="store_true",
        help="Exclude files under experiments/ (included by default).",
    )
    parser.add_argument(
        "--exclude-results",
        action="store_true",
        help="Exclude files under results/ (included by default).",
    )
    parser.add_argument(
        "--output",
        dest="output_dir_compat",
        default=None,
        help="Deprecated alias for --output-dir.",
    )
    args = parser.parse_args(argv)
    if args.output_dir is None and args.output_dir_compat is not None:
        args.output_dir = args.output_dir_compat
    if args.output_dir is None:
        args.output_dir = os.path.join(os.path.abspath(args.root), "aggregation")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    consolidate_project_by_folder(
        args.root,
        args.output_dir,
        mode=args.mode,
        extensions=args.ext,
        include_experiments=not args.exclude_experiments,
        include_results=not args.exclude_results,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
