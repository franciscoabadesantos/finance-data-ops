from __future__ import annotations

from finance_data_ops.ops import consolidation


def test_consolidation_writes_expected_outputs(tmp_path):
    root = tmp_path / "project"
    output = tmp_path / "aggregation"
    (root / "src").mkdir(parents=True, exist_ok=True)
    (root / "tests").mkdir(parents=True, exist_ok=True)
    (root / "src" / "app.py").write_text("print('ok')\n", encoding="utf-8")
    (root / "tests" / "test_app.py").write_text("def test_ok():\n    assert True\n", encoding="utf-8")

    consolidation.consolidate_project_by_folder(
        str(root),
        str(output),
        mode="all",
        extensions=[".py"],
        include_experiments=True,
        include_results=True,
    )

    assert (output / "project_organization.txt").exists()
    folder_files = list(output.glob("folder__*.txt"))
    assert folder_files
    assert any("src" in path.name for path in folder_files)


def test_consolidation_main_cli_smoke(tmp_path):
    root = tmp_path / "project"
    output = tmp_path / "aggregation"
    (root / "src").mkdir(parents=True, exist_ok=True)
    (root / "src" / "app.py").write_text("print('ok')\n", encoding="utf-8")

    exit_code = consolidation.main(
        [
            "--root",
            str(root),
            "--output-dir",
            str(output),
            "--mode",
            "no-tests",
            "--ext",
            ".py",
        ]
    )

    assert exit_code == 0
    assert (output / "project_organization.txt").exists()


def test_consolidation_groups_nested_scripts_into_single_top_level_file(tmp_path):
    root = tmp_path / "project"
    output = tmp_path / "aggregation"
    (root / "scripts" / "dev").mkdir(parents=True, exist_ok=True)
    (root / "scripts" / "ops").mkdir(parents=True, exist_ok=True)
    (root / "scripts" / "dev" / "a.py").write_text("print('a')\n", encoding="utf-8")
    (root / "scripts" / "ops" / "b.py").write_text("print('b')\n", encoding="utf-8")

    consolidation.consolidate_project_by_folder(
        str(root),
        str(output),
        mode="all",
        extensions=[".py"],
        include_experiments=True,
        include_results=True,
    )

    scripts_output = output / "folder__scripts.txt"
    assert scripts_output.exists()
    assert not (output / "folder__scripts__dev.txt").exists()
    assert not (output / "folder__scripts__ops.txt").exists()

    content = scripts_output.read_text(encoding="utf-8")
    assert "# File: scripts/dev/a.py" in content
    assert "# Source folder: scripts/dev" in content
    assert "# File: scripts/ops/b.py" in content
    assert "# Source folder: scripts/ops" in content


def test_consolidation_groups_root_files_into_root_bucket(tmp_path):
    root = tmp_path / "project"
    output = tmp_path / "aggregation"
    root.mkdir(parents=True, exist_ok=True)
    (root / "README.md").write_text("# readme\n", encoding="utf-8")
    (root / "pyproject.toml").write_text("[project]\nname='x'\n", encoding="utf-8")

    consolidation.consolidate_project_by_folder(
        str(root),
        str(output),
        mode="all",
        extensions=None,
        include_experiments=True,
        include_results=True,
    )

    root_output = output / "folder__root.txt"
    assert root_output.exists()
    assert not (output / "folder__README.md.txt").exists()
    assert not (output / "folder__pyproject.toml.txt").exists()


def test_consolidation_preserves_src_subfolder_split(tmp_path):
    root = tmp_path / "project"
    output = tmp_path / "aggregation"
    (root / "src" / "alpha").mkdir(parents=True, exist_ok=True)
    (root / "src" / "beta").mkdir(parents=True, exist_ok=True)
    (root / "src" / "alpha" / "a.py").write_text("print('a')\n", encoding="utf-8")
    (root / "src" / "beta" / "b.py").write_text("print('b')\n", encoding="utf-8")

    consolidation.consolidate_project_by_folder(
        str(root),
        str(output),
        mode="all",
        extensions=[".py"],
        include_experiments=True,
        include_results=True,
    )

    assert (output / "folder__src__alpha.txt").exists()
    assert (output / "folder__src__beta.txt").exists()
    assert not (output / "folder__src.txt").exists()
