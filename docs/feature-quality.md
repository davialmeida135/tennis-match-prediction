# Qualidade das features

Relatório da fonte local antes de imputação. Não é uma avaliação de desempenho do modelo.

Partidas na fonte: 200,444. Elegíveis: 198,865. Excluídas: 1,579 (walkovers e IDs ausentes).

SHA-256: `4a118344b0cca6986c0dea3f853dc72cdc12753e636114c9b63ec080b8d97563`.

Os números completos por temporada, incluindo ausências, valores inválidos e zeros válidos, estão em [feature-quality-coverage.json](feature-quality-coverage.json). A temporada mais recente pode estar incompleta.

| Campo | Cobertura total | 2024 | 2025 | 2026 |
| --- | ---: | ---: | ---: | ---: |
| winner_rank | 88.3% | 99.2% | 99.6% | 100.0% |
| loser_rank | 86.7% | 98.7% | 98.0% | 99.7% |
| winner_rank_points | 78.6% | 99.2% | 99.6% | 99.9% |
| loser_rank_points | 77.0% | 98.7% | 98.0% | 99.7% |
| winner_age | 98.6% | 99.9% | 99.9% | 99.7% |
| loser_age | 96.0% | 100.0% | 99.9% | 99.6% |
| w_ace_rate | 52.0% | 98.7% | 94.1% | 100.0% |
| l_ace_rate | 52.0% | 98.7% | 94.1% | 100.0% |
| w_double_fault_rate | 52.0% | 98.7% | 94.1% | 100.0% |
| l_double_fault_rate | 52.0% | 98.7% | 94.1% | 100.0% |
| w_service_points_won_rate | 52.0% | 98.7% | 94.1% | 100.0% |
| l_service_points_won_rate | 52.0% | 98.7% | 94.1% | 100.0% |

## Política implementada

- Ranking deve ser positivo; pontos e idade podem ser zero. Valores ausentes, negativos ou não finitos não substituem a última observação válida. Esses atributos continuam sendo os últimos valores conhecidos na sequência da fonte; a cobertura da fonte não mede sua defasagem no estado.
- Cada taxa de saque acumula seu próprio denominador. Só entram partidas com pontos de saque positivos e todos os componentes daquela taxa observados, finitos, não negativos e com soma menor ou igual aos pontos de saque. Zero aces ou duplas faltas é uma observação válida. Ausência de um componente de pontos ganhos invalida essa taxa naquela partida.
- Quando qualquer jogador não tem uma observação válida, a diferença correspondente fica neutra (zero). O modelo usa apenas as 12 comparações, sem indicadores de ausência.
- Forma geral mantém as últimas 50 partidas e usa as últimas 10. Forma por superfície mantém as últimas 10 daquela superfície em uma janela independente. Elo, H2H e estatísticas continuam respeitando os grupos temporais congelados.

## Reprodução

```powershell
uv run python -m tennis_match_prediction.ml.feature_quality --output docs/feature-quality-coverage.json
```

O JSON registra o hash da entrada e os critérios de elegibilidade são aplicados antes da medição. A execução não altera os dados de origem.
