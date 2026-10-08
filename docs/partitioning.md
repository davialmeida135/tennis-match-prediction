# Dagster partitioning assessment

Decision: keep the historical feature graph unpartitioned for now. Explicit
training dates are an experiment selection over a complete feature dataset;
they do not require Dagster partitions.

## Where partitions would help

Annual source ingestion is the first useful candidate: the source publishes
annual files, and `download_seasons(years=...)` already supports selected years.
The raw asset now owns ingestion via `RawMatchesCsv`; an annual partitioned
ingestion asset would expose per-season materialization,
retries and backfills. Normalization could also be done independently per season,
then consolidated before chronological feature generation. The raw asset still
consolidates all locally available seasons, so annual partitions would require
separate season outputs and a consolidation dependency, not just adding a
decorator argument to the existing consolidated raw asset.

## Why temporal features cannot be partitioned independently

`attach_temporal_features` starts with an empty player history and rebuilds it
from all supplied rows. Splitting that input by year would reset Elo, H2H,
experience and rolling history each January. The initial warmup would be repeated
instead of carrying the previous years' state forward.

Partitioning these assets needs either all preceding input partitions on every
run, or a validated checkpoint from the preceding partition. The first approach
still replays history; the second needs ordered execution, reproducible state,
deduplication and replay of later partitions after historical corrections. The
checkpoint must include any other causal references, such as imputation state.
Partitions alone do not provide incremental correctness.

## Storage changes required first

`ParquetDataFrameIOManager.snapshot_path` currently ignores partition keys and
writes one file per asset. Partitioned materializations would overwrite each
other. CSV exports likewise use fixed paths. Before enabling partitions, storage
must use partition-specific paths, support loading multiple upstream partitions,
and publish an explicit consolidated dataset/history for training and prediction.

## Recommended sequence

1. Keep full chronological feature generation and date-based training splits.
2. Measure ingestion/rebuild time and establish a need for selective reprocessing.
3. Add annual ingestion/normalization partitions and partition-aware persistence,
   retaining one consolidated downstream feature computation initially.
4. Only partition stateful features after testing that incremental execution and
   full replay produce identical features and final state, including corrections.

Reference: [Dagster partition mappings](https://dagster.io/docs/api/dagster/partitions)
and [I/O managers](https://dagster.io/docs/api/dagster/io-managers).
