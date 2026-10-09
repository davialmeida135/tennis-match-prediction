"""Capture code and dependency identity without copying datasets or environment secrets."""

import platform
import shutil
import subprocess
from importlib.metadata import version
from pathlib import Path
from zipfile import ZipFile

from pydantic import JsonValue

from tennis_match_prediction.paths import PROJECT_ROOT


def capture_provenance(directory: Path) -> dict[str, JsonValue]:
    packages = ("mlflow", "scikit-learn", "numpy", "pandas", "scipy", "cloudpickle")
    versions = {name: version(name) for name in packages}
    (directory / "requirements.txt").write_text(
        "\n".join(f"{name}=={value}" for name, value in versions.items()) + "\n", encoding="utf-8"
    )
    revision = None
    dirty = None
    if shutil.which("git") is not None:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            revision = result.stdout.strip()
            status = subprocess.run(
                ["git", "status", "--porcelain"],
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
                check=True,
            )
            dirty = bool(status.stdout.strip())
            diff = subprocess.run(
                ["git", "diff", "HEAD", "--", "src", "pyproject.toml", "uv.lock"],
                cwd=PROJECT_ROOT,
                capture_output=True,
                check=True,
            )
            (directory / "code.diff").write_bytes(diff.stdout)
    # Includes new, untracked modules too; the git diff alone cannot reproduce them.
    with ZipFile(directory / "source.zip", "w") as archive:
        for path in sorted((PROJECT_ROOT / "src").rglob("*.py")):
            archive.write(path, path.relative_to(PROJECT_ROOT))
        for name in ("pyproject.toml", "uv.lock"):
            path = PROJECT_ROOT / name
            if path.is_file():
                archive.write(path, name)
    return {
        "git_revision": revision,
        "git_dirty": dirty,
        "python": platform.python_version(),
        "packages": versions,
    }
