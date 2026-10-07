"""Configurable model inputs, independent of the generated dataset schema."""

# Remove or reorder entries to configure the default training experiment.
TRAINING_FEATURE_COLUMNS: tuple[str, ...] = (
    "overall_elo_diff",
    "surface_elo_diff",
    "rank_log_advantage",
    "points_log_diff",
    "form_10_diff",
    "surface_form_10_diff",
    "minutes_7d_diff",
    "matches_14d_diff",
    "age_diff",
    "h2h_log_odds",
    "experience_log_diff",
    "ace_rate_diff",
    "double_fault_rate_diff",
    "service_points_won_diff",
)
