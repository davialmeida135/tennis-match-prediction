"""Fetch upcoming ATP singles and preserve reproducible batches for the personal app."""

import hashlib
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Lock
from typing import Any

import requests

from tennis_match_prediction.providers.live_tennis import BASE_URL
from tennis_match_prediction.storage.json_io import write_json
from tennis_match_prediction.web.contracts import MatchupRequest
from tennis_match_prediction.web.service import PlayerService, search_key


class UpcomingService:
    """Serialize refreshes and cache the source for one minute to conserve the free quota."""

    def __init__(self, players: PlayerService, directory: Path, api_key: str | None) -> None:
        self.players = players
        self.directory = directory
        self.api_key = api_key
        self.lock = Lock()
        self._source: dict[str, Any] | None = None
        self._fetched_at: datetime | None = None

    def _fetch(self, now: datetime) -> dict[str, Any]:
        if (
            self._source is not None
            and self._fetched_at is not None
            and (now - self._fetched_at).total_seconds() < 60
        ):
            return self._source
        if not self.api_key:
            raise ValueError(
                "Configure LIVE_TENNIS_API_KEY on the server to load upcoming matches."
            )
        rows = []
        ids = set()
        for page in range(5):
            try:
                response = requests.get(
                    f"{BASE_URL}/matches",
                    headers={"X-API-Key": self.api_key},
                    params={
                        "status": "upcoming",
                        "tour": "atp",
                        "draw": "singles",
                        "is_qualifying": "false",
                        "limit": 100,
                        "offset": page * 100,
                    },
                    timeout=30,
                    allow_redirects=False,
                )
            except requests.RequestException:
                raise RuntimeError("Cannot reach the match provider. Try again later.") from None
            if response.status_code != 200:
                raise RuntimeError(f"Match provider returned HTTP {response.status_code}.")
            try:
                payload = response.json()
            except ValueError:
                raise RuntimeError("Match provider returned invalid JSON.") from None
            if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
                raise RuntimeError("Match provider returned an invalid match list.")
            for row in payload["data"]:
                if not isinstance(row, dict) or type(row.get("id")) is not int:
                    raise RuntimeError("Match provider returned an invalid match ID.")
                if row["id"] in ids:
                    raise RuntimeError("Match provider repeated an ID across pages. Retry later.")
                ids.add(row["id"])
                rows.append(row)
            meta = payload.get("meta")
            more = meta.get("has_more") if isinstance(meta, dict) else None
            if more is False:
                self._source = {"data": rows}
                self._fetched_at = datetime.now(UTC)
                return self._source
            if more is not True or not payload["data"]:
                raise RuntimeError("Match provider did not confirm complete pagination.")
        raise RuntimeError(
            "The match list exceeds the five-page refresh budget. No batch published."
        )

    def refresh(self, model_id: str) -> dict[str, Any]:
        if model_id not in self.players.models:
            raise ValueError("Select an available prediction model.")
        with self.lock:
            source = self._fetch(datetime.now(UTC))
            now = datetime.now(UTC)
            names: dict[str, list[str]] = {}
            for player_id, player in self.players.history.players.items():
                names.setdefault(search_key(player.name), []).append(player_id)
            predictions = []
            skipped = []
            for match in source["data"]:
                reason, start, player_ids = self._eligible(match, now, names)
                if reason:
                    skipped.append({"match_id": match["id"], "reason": reason})
                    continue
                try:
                    prediction = self.players.predict(
                        MatchupRequest(
                            player0_id=player_ids[0],
                            player1_id=player_ids[1],
                            match_date=start.date(),
                            surface=match["surface"].title(),
                            model_id=model_id,
                        )
                    )
                except ValueError:
                    skipped.append({"match_id": match["id"], "reason": "incompatible_history"})
                    continue
                prediction["match"] = {
                    "id": match["id"],
                    "tournament": match.get("tournament"),
                    "round": match.get("round_code") or match.get("round"),
                    "surface": match["surface"].title(),
                    "scheduled_time": start.isoformat(),
                    "provider_updated_at": match.get("updated_at"),
                    "provider_player0_id": match["players"]["p1"]["id"],
                    "provider_player1_id": match["players"]["p2"]["id"],
                }
                predictions.append(prediction)
            predictions.sort(key=lambda row: (row["match"]["scheduled_time"], row["match"]["id"]))
            # Exclude timestamps so repeating the same batch is idempotent.
            identity = {
                "source": source,
                "model_id": model_id,
                "model_version": self.players.model_version(model_id),
                "history": self.players.history_version,
                "predictions": [
                    {k: v for k, v in row.items() if k != "created_at"} for row in predictions
                ],
                "skipped": skipped,
            }
            batch_id = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
            self.directory.mkdir(parents=True, exist_ok=True)
            destination = self.directory / batch_id
            batch = {
                "id": batch_id,
                "created_at": now.isoformat(),
                "source_fetched_at": self._fetched_at.isoformat(),
                "source": "Live Tennis API",
                "model_id": model_id,
                "model_version": identity["model_version"],
                "model_name": self.players.models[model_id][1].model_name,
                "run_id": self.players.models[model_id][1].run_id,
                "history_source_date": self.players.history.last_source_date.isoformat(),
                "history_version": self.players.history_version,
                "predictions": predictions,
                "skipped": skipped,
                "skip_counts": dict(Counter(row["reason"] for row in skipped)),
                "source_match_count": len(source["data"]),
            }
            if not destination.exists():
                with TemporaryDirectory(dir=self.directory, prefix=".batch-") as temporary:
                    stage = Path(temporary) / "artifacts"
                    self.players.save_prediction_artifacts(model_id, stage)
                    write_json(stage / "source.json", source)
                    write_json(stage / "batch.json", batch)
                    stage.replace(destination)
            else:
                batch = json.loads((destination / "batch.json").read_text(encoding="utf-8"))
            write_json(self.directory / f"latest-{model_id}.json", {"id": batch_id})
            return self.latest(model_id)

    @staticmethod
    def _eligible(
        match: dict[str, Any], now: datetime, names: dict[str, list[str]]
    ) -> tuple[str | None, datetime | None, list[str] | None]:
        state = " ".join(
            str(match.get(k) or "").lower() for k in ("status", "event_status", "reason")
        )
        if any(word in state for word in ("postpon", "delay", "cancel", "suspend", "interrupt")):
            return "postponed_or_cancelled", None, None
        if match.get("status") != "upcoming" or match.get("live_at") or match.get("score"):
            return "already_started_or_not_upcoming", None, None
        if str(match.get("event_status") or "").strip().casefold() not in {
            "",
            "scheduled",
            "not started",
            "upcoming",
        }:
            return "already_started_or_not_upcoming", None, None
        if (
            match.get("tour") != "atp"
            or match.get("draw") != "singles"
            or match.get("is_qualifying") is not False
        ):
            return "outside_atp_singles", None, None
        try:
            start = datetime.fromisoformat(match.get("scheduled_time"))
        except (ValueError, TypeError):
            return "missing_or_invalid_time", None, None
        if start.utcoffset() is None or start <= now:
            return "not_a_future_time", None, None
        if not isinstance(match.get("surface"), str) or match["surface"] not in {
            "hard",
            "clay",
            "grass",
            "carpet",
        }:
            return "unknown_surface", None, None
        participants = match.get("players") or {}
        if not isinstance(participants, dict):
            return "player_not_found", None, None
        resolved = []
        provider_ids = []
        for side in ("p1", "p2"):
            player = participants.get(side) or {}
            if not isinstance(player, dict) or not isinstance(player.get("name"), str):
                return "player_not_found", None, None
            if type(player.get("id")) is not int:
                return "player_not_found", None, None
            matches = names.get(search_key(player.get("name") or ""), [])
            if len(matches) != 1:
                return "player_not_found_or_ambiguous", None, None
            resolved.append(matches[0])
            provider_ids.append(player["id"])
        if resolved[0] == resolved[1] or provider_ids[0] == provider_ids[1]:
            return "invalid_participants", None, None
        return None, start.astimezone(UTC), resolved

    def latest(self, model_id: str | None) -> dict[str, Any]:
        batch = None
        if model_id in self.players.models:
            pointer = self.directory / f"latest-{model_id}.json"
            if pointer.is_file():
                batch_id = json.loads(pointer.read_text(encoding="utf-8"))["id"]
                batch = self.read_batch(batch_id)
        if batch is not None:
            # Never show a prediction after its scheduled start, or for another history snapshot.
            now = datetime.now(UTC)
            batch = dict(batch)
            batch["predictions"] = [
                row
                for row in batch["predictions"]
                if datetime.fromisoformat(row["match"]["scheduled_time"]) > now
            ]
            if batch["history_version"] != self.players.history_version or batch[
                "model_version"
            ] != self.players.model_version(model_id):
                batch = None
        return {"configured": bool(self.api_key), "batch": batch}

    def read_batch(self, batch_id: str) -> dict[str, Any]:
        if len(batch_id) != 64 or any(c not in "0123456789abcdef" for c in batch_id):
            raise FileNotFoundError("Unknown prediction batch")
        path = self.directory / batch_id / "batch.json"
        if not path.is_file():
            raise FileNotFoundError("Unknown prediction batch")
        return json.loads(path.read_text(encoding="utf-8"))
