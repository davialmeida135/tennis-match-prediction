"""Pure pandas/polars transformations.

Functions transform DataFrames or player snapshots: no IO, no Dagster
imports, no side effects. That keeps the business logic unit-testable and makes
the Dagster assets thin orchestration layers.

Modules
-------
normalization   surface cleaning, source-date parsing and unusable-row filtering.
match_preparation  shared cleaning sequence, chronological ordering and history batches.
elo             overall and surface Elo updates for player history.
player_comparison  pre-match model features calculated from player snapshots.
temporal_features  pre-match comparisons and accumulated player state.
curation        removal of leaky per-match statistics.
training_rows   player positions, signed comparisons and the binary training target.
"""
