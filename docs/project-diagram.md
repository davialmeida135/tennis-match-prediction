# Tennis match prediction — project diagram

Current implementation, verified against the CLI entry points and Dagster asset definitions.
Paths shown below use the default `data/` root; `TENNIS_DATA_DIR` can override it.

```mermaid
flowchart TB
    source["TennisMyLife<br/>Annual ATP results"] --> refresh["ml.refresh_history<br/>Download, deduplicate, consolidate"]
    refresh --> raw[("data/raw/historical_matches/<br/>all_atp_matches.csv")]

    subgraph cli["Model training and prediction · CLI"]
        train["ml.train<br/>Read raw history"] --> engine["Shared temporal feature engine<br/>Clean and order matches<br/>14 pre-match difference features<br/>Assign player positions and target"]
        engine --> split["Chronological split<br/>70% train · 15% validation · 15% test"]
        split --> fit["StandardScaler + LogisticRegression<br/>Evaluate accuracy, Brier score, log loss"]
        fit --> model[("data/models/<br/>match_winner.pkl + metrics JSON")]
        fit --> tracking["Local MLflow<br/>Metrics and model artifacts"]
        engine --> state[("data/state/feature_state.json<br/>Player history + result cutoff")]
        request["Two player names<br/>Future date + surface"] --> predict["ml.predict<br/>Generate features from saved history<br/>Run model"]
        model --> predict
        state --> predict
        predict --> output["Win probability for each player<br/>History cutoff + model path"]
    end

    subgraph dagster["Dagster · materialize_historical_dataset"]
        d0["raw_atp_matches"] --> d1["normalized_atp_matches"]
        d1 --> d2["player_comparison_atp_matches<br/>Shared temporal feature engine"]
        d2 --> d3["winrate_featured_atp_matches"]
        d3 --> d4["h2h_featured_atp_matches"]
        d4 --> d5["elo_featured_atp_matches"]
        d5 --> d6["imputed_atp_matches<br/>Earlier observations only"]
        d6 --> d7["curated_atp_matches<br/>Encode context, remove match statistics"]
        d7 --> d8["training_atp_matches<br/>Assign player0/player1 + binary target"]
        d2 --> comparison[("player_comparison_atp_matches.csv")]
        d7 --> curated[("curated_atp_matches.csv")]
        d8 --> training[("training_atp_matches.csv")]
    end

    raw --> train
    raw --> d0
    d2 -->|"save checkpoint"| state
    storage["Parquet IO manager<br/>data/staging/: one snapshot per asset"] -.-> dagster
    checks["Pydantic / DataFrame contracts<br/>Dagster asset checks"] -.-> dagster

    classDef input fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e;
    classDef persisted fill:#fef3c7,stroke:#d97706,color:#78350f;
    classDef result fill:#dcfce7,stroke:#16a34a,color:#14532d;
    class source,request input;
    class raw,model,state,comparison,curated,training persisted;
    class output,tracking result;
```

The CLI trains directly from raw history. Dagster publishes a wider dataset for inspection and experiments; its CSV exports are independent branches and are not inputs to the current `ml.train` command.

Both workflows use `feature_state.py` and shared comparison formulas. Features are computed before applying each match result. Prediction requires known players and a date after the saved history cutoff. Training and the Dagster comparison asset both write the default state checkpoint.

## Source references

- `ml/refresh_history.py` and `pipelines/historical_matches/tennis_my_life.py`: ingestion.
- `ml/train.py` and `ml/predict.py`: model fitting, evaluation, artifacts, and inference.
- `pipelines/historical_matches/feature_state.py`: chronological feature generation and persisted history.
- `pipelines/historical_matches/assets.py` and `defs.py`: asset dependencies, exports, job, checks, and Parquet persistence.
- `pipelines/shared/contracts.py` and `paths.py`: feature contracts and default storage paths.
