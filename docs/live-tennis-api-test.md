# Teste da Live Tennis API

Branch: `codex/test-live-tennis-api`. Consulta real em 27/10/2026,
às 17:44 UTC (14:44 em America/Sao_Paulo).

## Resultado

A chave gratuita funcionou. O teste reproduzível fez quatro chamadas HTTP,
sem retries ou paginação automática: `/health`, `/usage`, `/fixtures` e
`/matches?status=upcoming`. Todas retornaram HTTP 200. O acesso sem chave a
`/fixtures` foi testado separadamente e retornou HTTP 401.

`/usage` confirmou plano `free`, 100 consultas/dia e 30/minuto.
As consultas de partidas usaram `tour=atp`, `draw=singles`,
`is_qualifying=false` e `limit=100`. As duas respostas indicaram `has_more=false`;
portanto, não houve truncamento declarado para esses filtros naquele instante.

| Verificação | Observado |
| --- | --- |
| Fixtures retornados | 26 |
| Status dos fixtures | 10 scheduled, 10 live, 3 finished, 3 cancelled |
| Partidas em `/matches?status=upcoming` | 11 |
| Torneios futuros observados | Basel e Vienna |
| Superfície dos 11 confrontos futuros | hard |
| Horários futuros com fuso explícito | 11 de 11 |
| Identificação dos participantes válida nas verificações básicas | 11 de 11 |
| Jogadores distintos no conjunto futuro | 22 |
| Correspondência única por nome com nosso histórico | 21 de 22, ainda não verificada |

## Diferenças importantes entre fixtures e matches

O endpoint chamado de fixtures não retornou apenas partidas futuras na amostra.
Não interpretar a presença nessa lista como confirmação de que um jogo ainda
não começou. Para o primeiro coletor, priorizar a lista `status=upcoming` e exigir
também um horário futuro; um horário previsto sozinho não comprova o estado atual.

Nos 26 fixtures, `id` diferiu de `match_id`. Exemplo observado:
`fixture.id=44633` aponta para `match_id=209696`. Uma chamada adicional a
`/matches/209696` confirmou o confronto Dimitrov/Cobolli. Usar espaços de identidade
separados e a ligação explícita `match_id`, sem tratar o ID do fixture como ID da
partida. Essa observação difere da descrição de um espaço único de IDs no guia;
o coletor deve seguir o payload validado e registrar mudanças futuras.

Em sete fixtures associados à lista futura, a ordem de `player1_id/player2_id`
estava invertida em relação a `players.p1/p2` da partida. Não transportar índices
de jogador entre endpoints: associar participantes e probabilidades pelos IDs.
Não houve conflito do par de participantes nos confrontos associados nesta amostra.

Os nomes nos fixtures podem vir abreviados (por exemplo, `F. Cobolli`); na lista de
matches vieram nomes completos. Os IDs do provedor são numéricos e não devem ser
comparados diretamente aos IDs ATP usados no nosso histórico.

## Identidade e adiamentos ainda pendentes

O nome `Daniel Merida Aguilar`, ID de provedor `214`, não teve correspondência
exata por nome com o histórico atual. Os outros 21 nomes produziram um candidato
único cada; o teste não transforma esses candidatos em mapeamentos confirmados.
Mesmo um nome único precisa de evidência adicional de identidade antes de prever.

Foram observados três fixtures `cancelled`, todos com `reason=cancelled`.
Não foi observada uma transição real de adiamento/reagendamento. Uma única coleta
não valida estabilidade de IDs, propagação de horários alterados, pontualidade dos
status nem cobertura contínua de torneios. O diário de transições do provedor é
documentado como BASIC; o plano gratuito exige nossos próprios snapshots para
comparar revisões, sem inferir cancelamento de uma simples ausência.

O resultado é favorável para continuar o piloto, mas não habilita predições
automáticas. O relatório marca `prediction_ready=false` explicitamente. O teste
não altera datasets, histórico dos jogadores, modelo ou aplicação web.

## Repetir o teste

Configure `LIVE_TENNIS_API_KEY` apenas no `.env` local ignorado pelo Git, ou na
variável de ambiente correspondente. O exemplo versionado contém somente o campo
vazio. Não passar a chave como argumento de linha de comando.

```powershell
uv run python -m tennis_match_prediction.diagnostics.live_tennis_probe --history-path data/curated/player_history.parquet
uv run pytest tests/test_live_tennis_probe.py -q
```

Cada execução usa um diretório novo sob `data/tmp/live-tennis-api/`, com respostas
JSON e `report.json`. Não há paginação automática: se `has_more=true`, o relatório
marca o conjunto incompleto. O teste compara nomes apenas como sugestões e lista
problemas de horário, superfície, participantes e ordenação.

Relatório da execução real:
`data/tmp/live-tennis-api/1f54153d2003495eb02b610d91594efe/report.json`.
O diretório é ignorado pelo Git e as evidências ficam locais. A chave não é
incluída nos relatórios. Onze testes locais passaram, incluindo troca de ordem,
IDs diferentes, datas inválidas/ausentes/passadas, nomes ambíguos, paginação
incompleta e falha de autenticação sem repetição da chamada.

Referências do provedor:
[partidas e fixtures](https://docs.livetennisapi.com/live-scores.html),
[autenticação e cota](https://docs.livetennisapi.com/auth-quota-and-health.html),
[OpenAPI](https://docs.livetennisapi.com/openapi.yaml).
