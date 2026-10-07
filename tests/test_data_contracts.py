"""Boundary regressions for frames and durable checkpoints."""

import json

import numpy as np
import pandas as pd
import pytest

from pipelines.historical_matches.feature_state import FeatureState
from pipelines.shared.contracts import FEATURE_COLUMNS, NORMALIZED_MATCHES, TRAINING_MATCHES


@pytest.mark.parametrize("bad_value", [np.nan, np.inf, -np.inf, "1"])
def test_training_rejects_invalid_features(bad_value):
    frame = pd.DataFrame({column: [bad_value] for column in FEATURE_COLUMNS})
    frame["winner"] = 1
    frame["match_date"] = "2026-01-01"
    with pytest.raises(ValueError, match="contract failed"):
        TRAINING_MATCHES.validate_frame(frame)


@pytest.mark.parametrize("target", [None, 2, "1"])
def test_training_rejects_invalid_target(target):
    frame = pd.DataFrame({column: [0.0] for column in FEATURE_COLUMNS})
    frame["winner"] = target
    frame["match_date"] = "2026-01-01"
    with pytest.raises(ValueError, match="binary target"):
        TRAINING_MATCHES.validate_frame(frame)


def test_normalized_dates_and_ids_are_validated_without_coercion():
    frame = pd.DataFrame({column: ["x", "x"] for column in NORMALIZED_MATCHES.required})
    frame["tourney_date"] = pd.to_datetime(["2026-01-02", "2026-01-01"])
    frame["winner_id"] = ["a", " "]
    errors = NORMALIZED_MATCHES.errors(frame)
    assert any("chronological" in error for error in errors)
    assert any("winner_id" in error for error in errors)
    assert frame.loc[1, "winner_id"] == " "


def test_checkpoint_rejects_extra_fields(tmp_path):
    path = tmp_path / "checkpoint.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "last_result_date": None,
                "players": {},
                "unexpected": True,
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="unexpected"):
        FeatureState.load(path)


def test_empty_checkpoint_round_trip(tmp_path):
    path = tmp_path / "checkpoint.json"
    FeatureState().save(path)
    loaded = FeatureState.load(path)
    assert loaded.players == {}
    assert loaded.last_result_date is None
