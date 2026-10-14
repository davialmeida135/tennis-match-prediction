# Tennis match prediction — project diagram

```mermaid
flowchart TB
    source["TennisMyLife annual ATP CSVs"] --> refresh["Dagster raw_atp_matches<br/>Reuse / consolidate / refresh when needed"]
    refresh --> raw[("all_atp_matches.csv + raw snapshot")]
    raw --> normalize["Dagster: normalize and order source dates / numeric match numbers"]
    normalize --> comparisons["12 pre-match comparisons<br/>Shared prior state for ambiguous order"]
    comparisons --> history[("player_history.parquet<br/>last_source_date + player states")]
    comparisons --> extras["Win rates → Elo<br/>Same history batches"]
    extras --> curate["Impute → encode context → remove outcome statistics"]
    curate --> rows["Assign player0/player1 and target"]
    rows --> training[("training_atp_matches.csv<br/>tourney_date + features + target")]
    training --> train["Shared experiment runner<br/>Logistic regression / random forest<br/>Warmup excluded from fitting<br/>Validation by default; explicit final test"]
    train --> model[("Versioned model + metadata<br/>Unique experiment directory")]
    train --> tracking["MLflow tracking server<br/>Compose: PostgreSQL + artifact volume"]
    history --> predict["tennis_match_prediction.ml.predict<br/>Shared comparison formulas"]
    model --> predict
    request["Player names + future date + surface"] --> predict
    predict --> output["Win probabilities + history_source_date + model path"]
```

The CLI trains from the Dagster-produced training CSV. Prediction reads the model
and the Dagster player-history snapshot. Comparison and curated CSV exports are
independent inspection branches.

`tourney_date` is preserved as a source date, whose granularity varies between
seasons and tournaments. Match numbers provide sequence within a tournament,
including rounds sharing a source date. Ambiguous groups share one prior state.
The pipeline does not infer match days or compute calendar workload windows.
See the history-order section in `README.md` for the ordering policy and limits.

Model artifacts require the `source_date_match_num` history order. Regenerate
snapshots, exports and models after this schema change; old formats are rejected.
