"""Render forecast evaluation reports for human readers."""

import json
from html import escape
from typing import Any


def _cell(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, (dict, list)):
        value = json.dumps(value, ensure_ascii=False)
    return escape(str(value)).replace("|", "&#124;").replace("\r", " ").replace("\n", " ")


def _prediction_rows(decisions: list[dict[str, Any]]) -> list[str]:
    lines = [
        "",
        "## Previsões e resultados",
        "",
        "Cada linha representa uma previsão salva, incluindo pendências, exclusões e "
        "previsões substituídas. Somente as linhas avaliadas entram nas métricas. "
        "Em 50%/50%, o segundo jogador é selecionado para o cálculo de acurácia.",
        "",
        "| Partida / previsão | Modelo / versão | Jogadores e probabilidades | "
        "Favorito | Vencedor observado | Placar do provedor | Avaliação |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in decisions:
        forecast = row.get("forecast") or {}
        result = row.get("result") or {}
        names = [row.get(f"player{index}_name", f"Player {index + 1}") for index in (0, 1)]
        probabilities = [
            f"{value:.1%}" if type(value) in (int, float) else "—"
            for index in (0, 1)
            for value in [forecast.get(f"player{index}_win_probability")]
        ]
        predicted = row.get("predicted_player_index")
        winner = row.get("winner_player_index")
        correct = row.get("prediction_correct")
        status = (
            "Correta"
            if correct is True
            else "Incorreta"
            if correct is False
            else row.get("reason") or "Detalhes indisponíveis"
        )
        cells = [
            f"{row['match_id']} / {row.get('created_at') or '—'}",
            f"{row.get('model_name')} / {row['model_version'][:12]}",
            f"{names[0]} ({probabilities[0]}) vs {names[1]} ({probabilities[1]})",
            names[predicted] if predicted is not None else None,
            names[winner] if winner is not None else result.get("status"),
            result.get("score"),
            status,
        ]
        lines.append(f"| {' | '.join(_cell(value) for value in cells)} |")
    if not decisions:
        lines.extend(["", "Ainda não há previsões salvas neste relatório."])
    for row in decisions:
        record = {
            "batch_id": row["batch_id"],
            "match_id": row["match_id"],
            "forecast": row.get("forecast"),
            "result": row.get("result"),
            "result_observed_at": row.get("result_observed_at"),
            "reason": row.get("reason"),
        }
        lines.extend(
            [
                "",
                "<details>",
                f"<summary>Partida {_cell(row['match_id'])} — lote "
                f"{_cell(row['batch_id'])}: previsão e resultado completos</summary>",
                "",
                f"<pre>{escape(json.dumps(record, ensure_ascii=False, indent=2))}</pre>",
                "</details>",
            ]
        )
    return lines


def markdown_report(report: dict[str, Any]) -> str:
    lines = [
        "# Desempenho das previsões salvas",
        "",
        f"Gerado em UTC: {report['generated_at']}",
        "",
        f"Lotes: {report['batch_count']}. Partidas previstas: {report['forecast_match_count']}. "
        f"Partidas avaliadas: {report['evaluated_match_count']}.",
        "",
        "Última previsão elegível por partida e versão exata do modelo. "
        "Somente partidas concluídas normalmente; abandonos, walkovers e cancelamentos excluídos.",
        "A previsão deve anteceder os horários agendados original/final e live_at. "
        "live_at é uma observação do provedor, pode ser regravado e não comprova o início oficial.",
        "O histórico deve ter data de fonte anterior ao dia UTC do corte. "
        "Cobertura avaliada inclui pendências no denominador; "
        "modelos podem ter amostras diferentes.",
        "",
        "| Modelo / versão | Avaliadas / previstas | Accuracy | Brier | Log loss |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for model in report["models"]:
        values = [
            f"{model[name]:.4f}" if model[name] is not None else "—"
            for name in ("accuracy", "brier", "log_loss")
        ]
        label = str(model["model_name"]).replace("|", " ").replace("\n", " ")
        lines.append(
            f"| {label} / {model['model_version'][:12]} | "
            f"{model['count']} / {model['forecast_matches']} | {' | '.join(values)} |"
        )
    if report["evaluated_count"] == 0:
        lines.extend(["", "Ainda não há partidas elegíveis para calcular desempenho."])
    lines.extend(_prediction_rows(report.get("decisions", [])))
    lines.extend(["", "## Motivos de exclusão ou pendência", ""])
    lines.extend(
        f"- {reason}: {count}" for reason, count in sorted(report["reason_counts"].items())
    )
    lines.extend(
        [
            "",
            "Referência neutra (50%): Brier 0,25; log loss 0,6931.",
            "O JSON contém decisões por previsão, versões de resultado, defasagem do histórico, "
            "métricas por superfície e frequências de calibração. "
            "Amostras pequenas não estabelecem calibração.",
            "",
            "Contrato do provedor: https://docs.livetennisapi.com/live-scores.html",
            "",
        ]
    )
    return "\n".join(lines)
