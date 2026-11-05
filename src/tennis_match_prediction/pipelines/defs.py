"""Application Dagster entry point, composing history, training and daily updates."""

from dagster import Definitions

from tennis_match_prediction.pipelines.daily_update import DailyUpdate
from tennis_match_prediction.pipelines.historical_matches import defs as history_module
from tennis_match_prediction.pipelines.model_training import defs as training_module

# Import modules so autodiscovery sees only the combined Definitions object.
defs = Definitions.merge(history_module.defs, training_module.defs, DailyUpdate().build_defs())
