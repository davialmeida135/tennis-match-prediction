"""File publication shared by ingestion, workflows and application services."""

import json
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import pandas as pd

from tennis_match_prediction import paths


@contextmanager
def atomic_output(output: Path) -> Iterator[Path]:
    """Publish through a unique, closed temporary file, tolerating brief Windows locks."""
    with NamedTemporaryFile(
        dir=output.parent, prefix=f".{output.name}.", suffix=".tmp", delete=False
    ) as handle:
        temporary = Path(handle.name)
    try:
        yield temporary
        for attempt in range(5):
            try:
                temporary.replace(output)
                break
            except PermissionError as error:
                # Windows scanners can briefly deny replacement with ACCESS_DENIED
                # (5), not only the sharing/lock violations (32/33).
                if getattr(error, "winerror", None) not in {5, 32, 33}:
                    raise
                if attempt == 4:
                    raise PermissionError(
                        f"Cannot replace {output}: Windows denied access "
                        "or reports that a file is in use. "
                        "Check file permissions and applications holding it open, then retry."
                    ) from error
                time.sleep(0.25 * 2**attempt)
    finally:
        temporary.unlink(missing_ok=True)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    """Publish strict JSON atomically and clean up temporary files on failure."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with atomic_output(path) as temporary, temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, allow_nan=False)


def write_curated_csv(
    frame: pd.DataFrame, file_name: str, *, output_dir: Path | None = None
) -> Path:
    """Write `frame` under data/curated as `file_name` and return the path."""
    path = (paths.CURATED_DIR if output_dir is None else output_dir) / file_name
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path
