"""Small, quota-bounded experiment against Live Tennis API, isolated from ingestion."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import requests
from dotenv import load_dotenv

from tennis_match_prediction.providers.live_tennis import BASE_URL

SAMPLE_FILTERS = {"tour": "atp", "draw": "singles", "is_qualifying": "false", "limit": 100}


def analyze_samples(
    fixtures: dict[str, Any],
    upcoming: dict[str, Any],
    *,
    observed_at: datetime,
    history_players: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Describe gaps and identity differences without mapping provider IDs to ATP IDs."""
    if observed_at.utcoffset() is None:
        raise ValueError("observed_at must include a timezone")
    fixture_rows = _rows(fixtures)
    match_rows = _rows(upcoming)
    matches = {row["id"]: row for row in match_rows}
    issues = []
    for row in match_rows:
        reasons = []
        if row.get("status") != "upcoming":
            reasons.append("not_upcoming")
        if row.get("surface") not in {"hard", "clay", "grass", "carpet"}:
            reasons.append("unknown_surface")
        start = row.get("scheduled_time")
        if start is None:
            reasons.append("missing_start_time")
        else:
            try:
                parsed = datetime.fromisoformat(start)
                if parsed.utcoffset() is None:
                    reasons.append("start_time_without_timezone")
                elif parsed <= observed_at:
                    reasons.append("start_time_not_future")
            except (TypeError, ValueError):
                reasons.append("invalid_start_time")
        players = row.get("players", {})
        ids = [players.get(side, {}).get("id") for side in ("p1", "p2")]
        if any(type(player_id) is not int for player_id in ids) or ids[0] == ids[1]:
            reasons.append("invalid_participants")
        if reasons:
            issues.append({"match_id": row["id"], "reasons": reasons})

    swapped = []
    conflicting = []
    for fixture in fixture_rows:
        # Observed live payloads expose a fixture ID and a DIFFERENT canonical match_id.
        match = matches.get(fixture.get("match_id"))
        if match is None:
            continue
        first = fixture.get("player1_id")
        second = fixture.get("player2_id")
        players = match.get("players", {})
        p1 = players.get("p1", {}).get("id")
        p2 = players.get("p2", {}).get("id")
        if None in (first, second, p1, p2):
            conflicting.append(fixture["id"])
        elif (first, second) == (p2, p1) and p1 != p2:
            swapped.append(fixture["id"])
        elif (first, second) != (p1, p2):
            conflicting.append(fixture["id"])

    candidates = []
    if history_players is not None:
        names: dict[str, list[str]] = {}
        for player_id, player in history_players.items():
            names.setdefault(player["name"].casefold(), []).append(player_id)
        seen = set()
        for row in match_rows:
            for player in row.get("players", {}).values():
                if player.get("id") is None or player["id"] in seen:
                    continue
                seen.add(player["id"])
                candidates.append(
                    {
                        "provider_player_id": player["id"],
                        "name": player.get("name"),
                        "history_candidate_ids": names.get(
                            (player.get("name") or "").casefold(), []
                        ),
                        "identity_verified": False,
                    }
                )

    return {
        "fixtures": {
            "count": len(fixture_rows),
            "status_counts": dict(Counter(row.get("status") for row in fixture_rows)),
            "fixture_ids_different_from_match_ids": sum(
                row.get("match_id") is not None and row["id"] != row["match_id"]
                for row in fixture_rows
            ),
            "swapped_player_order_fixture_ids": swapped,
            "conflicting_participants_fixture_ids": conflicting,
            "complete_page_set": fixtures.get("meta", {}).get("has_more") is False,
        },
        "upcoming": {
            "count": len(match_rows),
            "status_counts": dict(Counter(row.get("status") for row in match_rows)),
            "surface_counts": dict(Counter(row.get("surface") for row in match_rows)),
            "tournaments": sorted(
                {row["tournament"] for row in match_rows if row.get("tournament")}
            ),
            "issues": issues,
            "rows_passing_basic_checks": len(match_rows) - len(issues),
            "complete_page_set": upcoming.get("meta", {}).get("has_more") is False,
        },
        "history_name_candidates": candidates,
        "prediction_ready": False,
        "note": (
            "Names are suggestions only; identity mapping and start-state confirmation "
            "remain required."
        ),
    }


def _rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("data")
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("Expected a JSON object with a data array of objects")
    ids = [row.get("id") for row in rows]
    if any(type(row_id) is not int for row_id in ids) or len(set(ids)) != len(ids):
        raise ValueError("Expected unique integer row IDs")
    return rows


def run_probe(
    api_key: str, *, output_dir: Path, history_path: Path | None = None
) -> dict[str, Any]:
    """Make four requests, with no retries or automatic pagination, and save local evidence."""
    if not api_key.strip():
        raise ValueError("Configure LIVE_TENNIS_API_KEY in the local .env or environment")
    attempt = output_dir / uuid4().hex
    attempt.mkdir(parents=True)
    report: dict[str, Any] = {
        "started_at": datetime.now(UTC).isoformat(),
        "status": "running",
        "filters": SAMPLE_FILTERS,
        "requests": [],
        "report_path": str((attempt / "report.json").resolve()),
    }
    payloads = {}
    try:
        for endpoint in ("health", "usage", "fixtures", "matches"):
            params = dict(SAMPLE_FILTERS) if endpoint in {"fixtures", "matches"} else {}
            if endpoint == "matches":
                params["status"] = "upcoming"
            try:
                response = requests.get(
                    f"{BASE_URL}/{endpoint}",
                    headers={"X-API-Key": api_key},
                    params=params,
                    timeout=30,
                    allow_redirects=False,
                )
            except requests.RequestException:
                raise RuntimeError(f"Network request failed for /{endpoint}") from None
            report["requests"].append({"endpoint": endpoint, "status": response.status_code})
            if response.status_code != 200:
                # Never echo server error bodies or request headers containing credentials.
                raise RuntimeError(f"/{endpoint} returned HTTP {response.status_code}")
            payloads[endpoint] = response.json()
            snapshot = attempt / f"{endpoint}.json"
            snapshot.write_bytes(response.content)
        players = None
        if history_path is not None:
            import pandas as pd

            raw_history = pd.read_parquet(history_path)
            players = json.loads(raw_history.loc[0, "history"])["players"]
            report["history_sha256"] = hashlib.sha256(history_path.read_bytes()).hexdigest()
        report["analysis"] = analyze_samples(
            payloads["fixtures"],
            payloads["matches"],
            observed_at=datetime.now(UTC),
            history_players=players,
        )
        usage = payloads["usage"]
        report["plan"] = {"tier": usage.get("tier"), "limits": usage.get("limits")}
        report["status"] = "success"
    except (RuntimeError, ValueError, OSError) as error:
        report["status"] = "failed"
        # Error messages here come from our validation, not HTTP request representations.
        report["error"] = str(error)
        raise RuntimeError(f"Probe failed; see {report['report_path']}") from None
    finally:
        report["finished_at"] = datetime.now(UTC).isoformat()
        (attempt / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def main() -> None:
    """Run the experiment without building datasets or generating predictions."""
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=root / "data/tmp/live-tennis-api")
    parser.add_argument("--history-path", type=Path)
    args = parser.parse_args()
    load_dotenv(root / ".env")
    try:
        report = run_probe(
            os.getenv("LIVE_TENNIS_API_KEY", ""),
            output_dir=args.output_dir,
            history_path=args.history_path,
        )
    except (RuntimeError, ValueError, OSError) as error:
        parser.exit(1, f"{error}\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
