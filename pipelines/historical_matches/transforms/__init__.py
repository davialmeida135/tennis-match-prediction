"""Pure pandas/polars transformations.

Every function here takes a DataFrame and returns a DataFrame: no IO, no Dagster
imports, no side effects. That keeps the business logic unit-testable and makes
the Dagster assets thin orchestration layers.

Modules
-------
normalization   dates, chronological ordering, seed/entry-method parsing.
imputation      filling null height/age/rank/surface values.
winrate         rolling win-rate features (overall and per surface).
player_stats    head-to-head history and Elo ratings.
finalization    column encoding and removal of leaky per-match statistics.
anonymization   winner/loser -> player0/player1 shuffling with a binary target.
history_utils   lookback helpers shared by the feature engineering modules.
"""
