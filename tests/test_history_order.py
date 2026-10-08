"""Regression tests for source-date/match-number history without inferred days."""

import pickle

import pandas as pd
import pytest

from tennis_match_prediction.contracts import (
    INITIAL_ELO,
    PLAYER_COMPARISON_FEATURE_COLUMNS,
    FutureMatchRequest,
    PlayerHistory,
)
from tennis_match_prediction.ml.predict import features_for, predict
from tennis_match_prediction.transforms.elo import calcular_elo
from tennis_match_prediction.transforms.head_to_head import (
    calcular_h2h,
)
from tennis_match_prediction.transforms.history import prepare_history
from tennis_match_prediction.transforms.player_history import (
    attach_temporal_features,
)
from tennis_match_prediction.transforms.winrate import (
    calcular_winrate_superficie,
    calcular_winrate_superficie_ultimas_n,
    calcular_winrate_total,
    calcular_winrate_ultimas_n,
)


def _matches(numbers: list[object]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "tourney_date": [20260105] * len(numbers),
            "tourney_id": ["2026-1"] * len(numbers),
            "match_num": numbers,
            "winner_id": ["a"] * len(numbers),
            "loser_id": ["b"] * len(numbers),
            "winner_name": ["Alice"] * len(numbers),
            "loser_name": ["Bob"] * len(numbers),
            "surface": ["Hard"] * len(numbers),
            "w_svpt": [100] * len(numbers),
            "w_ace": [10] * len(numbers),
            "l_svpt": [100] * len(numbers),
            "l_ace": [0] * len(numbers),
        }
    )


def test_same_source_date_uses_numeric_match_order_for_every_feature() -> None:
    prepared = prepare_history(_matches(["10", "2", "1"]))
    _, features, _ = attach_temporal_features(prepared)
    elo = calcular_elo(prepared)
    h2h = calcular_h2h(prepared)
    assert features["match_num"].tolist() == [1, 2, 10]
    assert features["overall_elo_diff"].tolist() == pytest.approx(elo["elo_diff"].tolist())
    assert features["form_10_diff"].tolist() == [0.0, 1.0, 1.0]
    assert features["surface_form_10_diff"].tolist() == [0.0, 1.0, 1.0]
    assert features["ace_rate_diff"].tolist() == pytest.approx([0.0, 0.1, 0.1])
    assert h2h["h2h"].tolist() == [0, 1, 2]
    for transform, suffix in (
        (calcular_winrate_total, ""),
        (calcular_winrate_ultimas_n, "_last_50"),
        (calcular_winrate_superficie, "_surface"),
        (calcular_winrate_superficie_ultimas_n, "_surface_last_50"),
    ):
        winrates = transform(prepared)
        assert winrates[f"winner_winrate{suffix}"].tolist() == [0.0, 1.0, 1.0]
        assert winrates[f"loser_winrate{suffix}"].tolist() == [0.0, 0.0, 0.0]


def test_later_match_in_same_tournament_cannot_change_prior_features() -> None:
    _, prefix, _ = attach_temporal_features(_matches([1, 2]))
    future = _matches([3]).assign(
        winner_id="b", loser_id="a", winner_name="Bob", loser_name="Alice"
    )
    _, full, _ = attach_temporal_features(pd.concat([_matches([1, 2]), future], ignore_index=True))
    pd.testing.assert_frame_equal(prefix, full.iloc[:2].reset_index(drop=True))


@pytest.mark.parametrize("numbers", [[1, None], [1, 1]])
def test_ambiguous_numbers_share_prior_history_and_apply_results_afterward(numbers) -> None:
    source = _matches(numbers)
    _, features, history = attach_temporal_features(source)
    assert features[list(PLAYER_COMPARISON_FEATURE_COLUMNS)].eq(0.0).all().all()
    assert calcular_elo(source)["elo_diff"].eq(0.0).all()
    assert calcular_h2h(source)["h2h"].eq(0).all()
    assert calcular_winrate_total(source)["winner_winrate"].eq(0.0).all()
    assert history.players["a"].wins == 2
    assert history.players["b"].losses == 2
    # Both results must use the same pre-batch rating and completed-match count.
    expected_gain = 2 * (250 / (5**0.4)) * 0.5
    assert history.players["a"].rating == pytest.approx(INITIAL_ELO + expected_gain)
    assert history.players["a"].surface_ratings["Hard"] == pytest.approx(
        INITIAL_ELO + expected_gain
    )
    assert history.players["b"].surface_ratings["Hard"] == pytest.approx(
        INITIAL_ELO - expected_gain
    )
    future = source.iloc[[0]].assign(tourney_date=20260112, match_num=1)
    _, full, _ = attach_temporal_features(pd.concat([source, future], ignore_index=True))
    assert full.iloc[-1]["form_10_diff"] == 1.0
    assert full.iloc[-1]["overall_elo_diff"] > 0.0


def test_surface_elo_updates_only_the_played_surface() -> None:
    source = _matches([1, 2, 3]).assign(surface=["Hard", "Clay", "Hard"])
    _, _, after_hard = attach_temporal_features(source.iloc[:1])
    _, comparisons, after_clay = attach_temporal_features(source.iloc[:2])
    _, _, final = attach_temporal_features(source)

    # Prior Hard results influence overall Elo, but a first Clay match starts equal.
    assert comparisons.iloc[1]["overall_elo_diff"] > 0.0
    assert comparisons.iloc[1]["surface_elo_diff"] == 0.0
    for player_id in ("a", "b"):
        assert (
            after_clay.players[player_id].surface_ratings["Hard"]
            == (after_hard.players[player_id].surface_ratings["Hard"])
        )
        assert (
            final.players[player_id].surface_ratings["Clay"]
            == (after_clay.players[player_id].surface_ratings["Clay"])
        )
        assert (
            final.players[player_id].surface_ratings["Hard"]
            != (after_clay.players[player_id].surface_ratings["Hard"])
        )


def test_tournament_id_cannot_order_same_day_results_for_a_shared_player() -> None:
    source = _matches([1, 2]).assign(tourney_id=["event-a", "event-b"])
    _, features, _ = attach_temporal_features(source)
    assert features[list(PLAYER_COMPARISON_FEATURE_COLUMNS)].eq(0.0).all().all()
    assert calcular_elo(source)["elo_diff"].eq(0.0).all()
    assert calcular_h2h(source)["h2h"].eq(0).all()
    assert calcular_winrate_total(source)["winner_winrate"].eq(0.0).all()


def test_next_period_prediction_matches_historical_features() -> None:
    source = _matches([1, 2])
    _, _, history = attach_temporal_features(source)
    request = FutureMatchRequest(
        player0_name="Bob", player1_name="Alice", match_date="2026-01-12", surface="Hard"
    )
    future = _matches([1]).assign(tourney_date=20260112, tourney_id="2026-2")
    _, features, _ = attach_temporal_features(pd.concat([source, future], ignore_index=True))
    assert (
        features_for(history, request)
        == features.iloc[-1][list(PLAYER_COMPARISON_FEATURE_COLUMNS)].to_dict()
    )


def test_old_history_format_is_rejected() -> None:
    with pytest.raises(ValueError, match="last_source_date"):
        PlayerHistory.model_validate({"last_result_date": None, "players": {}})


def test_old_model_format_is_rejected(tmp_path) -> None:
    model_path = tmp_path / "model.pkl"
    model_path.write_bytes(
        pickle.dumps({"model": None, "feature_columns": PLAYER_COMPARISON_FEATURE_COLUMNS})
    )
    history_path = tmp_path / "history.parquet"
    pd.DataFrame(
        {"history": [PlayerHistory(last_source_date=None, players={}).model_dump_json()]}
    ).to_parquet(history_path)
    request = FutureMatchRequest(
        player0_name="Bob", player1_name="Alice", match_date="2026-01-12", surface="Hard"
    )
    with pytest.raises(ValueError, match="retrained"):
        predict(request, model_path, history_path)
