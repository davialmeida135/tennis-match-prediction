"""Render forecast evaluation reports for human readers."""

from typing import Any


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
