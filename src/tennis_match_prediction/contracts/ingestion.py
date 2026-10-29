"""Metadata for downloaded historical season files."""

from pathlib import Path

from pydantic import ConfigDict

from tennis_match_prediction.contracts.base import ContractModel


class DownloadedSeason(ContractModel):
    """Metadata recorded for one downloaded source file."""

    model_config = ConfigDict(frozen=True)

    year: int
    path: Path
    sha256: str
    changed: bool
