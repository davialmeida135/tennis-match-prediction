"""Pure pandas/polars transformations.

Functions transform DataFrames or player snapshots: no IO, no Dagster
imports, no side effects. That keeps the business logic unit-testable and makes
the Dagster assets thin orchestration layers.

Modules
-------
normalization   dates, chronological ordering, seed/entry-method parsing.
imputation      filling null height/age/rank/surface values.
winrate         rolling win-rate features (overall and per surface).
history         common date parsing, filtering and chronological ordering.
elo             pre-match ratings and shared Elo update formula.
player_comparison  pre-match model features calculated from player snapshots.
player_history  chronological player snapshots and match comparisons.
curation    column encoding and removal of leaky per-match statistics.
training_rows   player positions, signed comparisons and the binary training target.
"""
