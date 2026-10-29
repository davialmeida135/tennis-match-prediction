"""Application Dagster entry point, composing history, training and daily updates."""

from dagster import Definitions

from tennis_match_prediction.pipelines.daily_update import DailyUpdate
from tennis_match_prediction.pipelines.historical_matches.defs import defs as history_defs
from tennis_match_prediction.pipelines.model_training.defs import defs as training_defs

defs = Definitions.merge(history_defs, training_defs, DailyUpdate().build_defs())
