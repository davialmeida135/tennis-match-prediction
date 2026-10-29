# Rotina diária e desempenho prospectivo

```powershell
uv run python -m tennis_match_prediction.workflows.daily_update
```

O comando atualiza a temporada corrente e a anterior, preserva as demais temporadas
locais, coleta os resultados de torneios em andamento e reconstrói o histórico
somente se houver alteração nos dados, implementação/dependências ou artefatos.
Não treina nem promove modelos. Uma primeira instalação deve baixar todo o
histórico com o comando de refresh existente antes de usar a rotina diária.
Use `--years 2024 2025 2026` para reconsultar temporadas específicas.

A etapa de desempenho roda mesmo se a atualização do histórico falhar; o comando
termina com código diferente de zero quando qualquer etapa falha. Os relatórios
operacionais ficam em `data/maintenance/<id>.json` e `latest.json`. Os arquivos de
lock impedem concorrência dentro de cada etapa. Um encerramento abrupto pode
deixar o lock: confirmar que não existe execução ativa antes de removê-lo.

Para execução local sem acesso às fontes:

```powershell
uv run python -m tennis_match_prediction.workflows.daily_update --local-only --offline-results
```

`--csv-path`, `--curated-dir`, `--predictions-dir` e `--performance-dir` permitem
isolar entradas e saídas. `--annual-only` desativa a integração do arquivo parcial.
O modo local utiliza o CSV consolidado existente, sem tentar atualizá-lo.

## Agendamento

As definições existentes registram o job `daily_update` e o schedule
`daily_history_and_performance`, diariamente às **06:00 em America/Sao_Paulo**,
com estado padrão RUNNING. É necessário carregar essas definições em uma instância
com o daemon Dagster em funcionamento. Uma configuração persistida de schedule
parado na instância prevalece sobre o padrão; verificar o estado na interface.
Não há criação de serviço de sistema ou alteração do daemon pelo comando.

O op `update_history_and_performance` aceita os mesmos diretórios, seleção de anos,
`local_only`, `include_ongoing` e `max_requests` no Launchpad. Treinamento e refresh
das previsões futuras continuam independentes; previsões só são criadas pelo
refresh já existente da aplicação.

## Publicação do histórico

Cada tentativa preserva o CSV de entrada congelado, snapshots intermediários,
checks e hashes em `data/curated/.refreshes/<id>/`. O download anual é validado
antes da substituição de cada arquivo. Uma falha durante uma coleta de várias
temporadas pode atualizar parte do cache bruto; não publica histórico derivado.
A próxima execução reconsulta os anos selecionados.

O arquivo em andamento é uma coleção parcial de resultados. A identidade usada
na reconciliação é `(tourney_id, match_num)`, com conferência do par de jogadores
independente do vencedor. Chaves ausentes/duplicadas no parcial ou participantes
conflitantes entre fontes interrompem a atualização. Linhas anuais sem identidade
completa ou com identidade ambígua são preservadas sob a política temporal existente,
sem entrar nessa associação. Sobreposição do parcial com uma dessas identidades
interrompe a atualização para revisão. Arquivos parciais brutos ficam em `ongoing_snapshots/<sha256>.csv`;
`ongoing_results.csv` retém a última observação de cada chave, inclusive quando ela
desaparece do parcial. O anual tem precedência sobre o parcial para correções e
migração. A ausência no parcial não confirma exclusão. Exclusões anuais de uma
partida anteriormente observada no parcial exigem reconciliação explícita futura.

Após os checks, os exports compatíveis são substituídos e `latest_refresh.json`
aponta atomicamente para o conjunto de snapshots validado. A aplicação resolve
esse ponteiro, verifica o hash e carrega o novo histórico na próxima chamada da
API. Se falhar, mantém o snapshot anterior e informa `history_reload_error` em
`/api/status`. Requisições já em andamento mantêm sua versão congelada. Modelos
novos ainda exigem reinício da aplicação. Não apagar snapshots referenciados pelo
manifesto ou preservados junto das previsões.

Falhas de publicação tratadas tentam restaurar os exports anteriores. A troca dos
três arquivos de compatibilidade não é uma transação: um encerramento abrupto
durante a troca pode deixá-los misturados. O leitor web evita esse problema usando
o ponteiro e os snapshots imutáveis; consumidores diretos de CSV devem rodar após
o término bem-sucedido da atualização.

## Associação dos resultados e política de avaliação

```powershell
uv run python -m tennis_match_prediction.forecast_evaluation
uv run python -m tennis_match_prediction.forecast_evaluation --offline
```

Somente lotes imutáveis `data/predictions/*/batch.json` da Live Tennis API entram
na avaliação. Predições manuais exportadas não têm identidade de partida e ficam
fora deste fluxo. Consultas usam `/matches/{id}` do mesmo provedor, conferindo
ID da partida e os dois participantes. O campo `winner` é posição 1/2 na resposta;
o resultado é traduzido para o jogador previsto pelos IDs, aceitando ordem invertida.
Não se faz associação por nome, ID de fixture ou data aproximada.

Há no máximo 20 chamadas por execução, sem retries automáticos. `--max-requests N`
reduz esse limite. Só partidas cujo horário original já passou são consultadas;
primeiro as sem observação, depois as observadas há mais tempo. Resultados finais
também são reconsultados para capturar correções. Execuções concorrentes são
bloqueadas. A cota é compartilhada com outras chamadas da aplicação; HTTP 403/429,
falhas de rede e payloads inválidos preservam a publicação anterior. IDs 404/410
ficam pendentes; não se segue merge de ID sem mapeamento verificado.

A regra registrada em cada relatório é:

- Uma previsão por ID de partida e hash exato de modelo: a última elegível.
- Somente `status=completed` **e** `outcome=completed`, com vencedor explícito.
- Excluir abandonos, walkovers, defaults, cancelamentos e desfechos desconhecidos.
- Exigir timestamps com fuso: coleta da programação anterior ou igual à geração,
  ambas anteriores ao menor entre horário agendado original, horário final e
  `live_at`. Exigir observação do resultado posterior a esses horários.
- Exigir data de fonte do histórico anterior ao dia UTC do corte. Isso mantém o
  limite conservador atual; não habilita avaliação histórica com estado final.

`live_at` é a observação do provedor de que o jogo entrou em andamento; pode ser
regravado e não comprova o início oficial. O relatório explicita essa limitação.
Uma política mais forte exige evidência adicional de início e revisões temporais.

Cada execução preserva `results.json`, `report.json` e `report.md` em
`data/performance/runs/<id>/`. Só após gravar o conjunto completo substitui
`latest.json`. Resultados corrigidos produzem outro relatório sem alterar lotes,
probabilidades ou relatórios anteriores. O modo offline usa as últimas observações
publicadas e não chama a API. Observações não reconsultadas permanecem identificadas
pelo timestamp, hash e versão do resultado; não se apresentam como recém-validadas.

## Leitura do relatório

Na aplicação, abrir **Performance** (`/performance`). O relatório inclui accuracy,
Brier, log loss, quantidade avaliada por versão, cobertura e motivos de pendência.
O JSON (`GET /api/performance`) inclui decisões por previsão, histórico/modelo,
referência do resultado, métricas por superfície, defasagem média do histórico e
frequências descritivas em dez faixas de probabilidade. O Markdown pode ser baixado
em `GET /api/performance/download`. Os endpoints só leem a última publicação;
não consultam o provedor e não executam a rotina diária.

Cobertura de predição = partidas distintas previstas / partidas distintas vistas
nos lotes, incluindo as ignoradas. Fração avaliada por modelo = partidas avaliadas /
partidas previstas daquela versão; partidas ainda futuras ou pendentes permanecem
no denominador. Versões diferentes podem ter amostras diferentes; as métricas não
constituem uma comparação controlada entre modelos. Sem partidas elegíveis, as
métricas são nulas, nunca zero. Uma previsão neutra de 50% tem Brier 0,25 e log loss
aproximadamente 0,6931. Amostras pequenas não estabelecem calibração.

Contrato consultado e exemplo real de resultado validado em 27/10/2026:
[Live Tennis API — partidas e resultados](https://docs.livetennisapi.com/live-scores.html).

## Validação realizada em 27/10/2026

A rotina completa terminou com sucesso sobre 200.987 linhas brutas, em 702,97 s,
sem checks reprovados. O histórico publicado avançou da data de fonte 18/10 para
27/10/2026. Um servidor iniciado antes da publicação passou a usar o novo hash
automaticamente, sem reinício. Uma predição manual para 28/10, com o modelo
arquivado, gerou as 12 features e probabilidades cuja soma é 1; essa verificação
não foi inserida no conjunto de previsões prospectivas.

Relatório operacional local:
`data/maintenance/65e5c95effb340dd8c8ee23cab7866eb.json`.
Relatório de desempenho:
`data/performance/runs/fffeab63976b465983b0dd80346be580/report.md`.
Há dois lotes, dez partidas distintas previstas e nenhuma partida elegível para
métricas neste instante. Duas partidas já foram reconsultadas, ainda sem conclusão
elegível. As versões exatas dos resultados e motivos ficam no JSON do relatório.

A suíte completa passou com 226 testes e duas integrações opcionais ignoradas;
as áreas afetadas foram verificadas novamente após os ajustes finais. Ruff,
Pylint e carregamento das definições passaram. A página foi conferida no Chrome
em desktop e viewport móvel de 390 px, sem overflow da página; a tabela tem
rolagem horizontal própria.
