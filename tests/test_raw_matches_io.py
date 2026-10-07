import pandas as pd


def test_mixed_seed_values_can_be_snapshotted_as_parquet(tmp_path) -> None:
    source = tmp_path / "matches.csv"
    source.write_text("winner_seed,loser_seed\n6,\n,6.0\nWC,7\n", encoding="utf-8")

    frame = pd.read_csv(source, dtype={"winner_seed": "string", "loser_seed": "string"})
    snapshot = tmp_path / "matches.parquet"
    frame.to_parquet(snapshot, index=False)
    restored = pd.read_parquet(snapshot)

    assert restored["winner_seed"].tolist() == ["6", pd.NA, "WC"]
    assert restored["loser_seed"].tolist() == [pd.NA, "6.0", "7"]
