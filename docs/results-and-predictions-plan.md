# Plano: resultados novos e predições de partidas futuras

Status: atualização com reconstrução completa implementada; demais entregas propostas.

Primeira implementação: `refresh_history` agora executa o grafo histórico completo,
valida checks antes de publicar, preserva snapshots por tentativa e registra tempos
por etapa/total. O fluxo usa os arquivos anuais existentes, sem treinamento automático.
Publicação é atômica por arquivo; cache de origem e conjunto de exports não formam
uma transação única. Reconciliação de arquivos em andamento e programação continuam
pendentes. Ver comandos e limites operacionais no README.

### Primeira medição com dados reais

Atualização da temporada de 2026 com reconstrução de todas as temporadas locais,
concluída em 19/10/2026 (America/Sao_Paulo). Foram processadas 200.888 linhas brutas
e 199.313 partidas normalizadas; todos os checks passaram e os três artefatos foram
publicados. O modelo não foi retreinado.

| Etapa | Tempo |
| --- | ---: |
| Execução completa, incluindo publicação | 533,16 s |
| Histórico dos jogadores e comparações | 360,52 s |
| Elo | 114,11 s |
| Normalização | 24,13 s |
| Coleta e consolidação bruta | 13,15 s |

Relatório local: `data/curated/.refreshes/f552cf30ccdb43adb309ff7666af7104/report.json`.
Esta é uma medição operacional inicial, com parte da execução concorrente à suíte
de testes, não um benchmark controlado. Manter a reconstrução completa e comparar
os relatórios seguintes antes de implementar estados incrementais.

## Objetivo e escopo inicial

Fechar o ciclo de coletar resultados, atualizar o dataset e o histórico dos jogadores,
buscar confrontos futuros, registrar predições antes das partidas e avaliar os resultados.
O primeiro recorte mantém o ATP simples já atendido pelo projeto. Challenger, WTA,
qualifying e duplas ficam para uma ampliação explícita de cobertura.

Priorizar o TennisMyLife, já integrado. Uma fonte adicional para programação só será
escolhida se a fonte atual não fornecer os dados necessários. Não pressupor API paga.

## Ponto de partida verificado no código

- `RawMatchesCsv` consulta o catálogo do TennisMyLife e aceita arquivos anuais
  `YYYY.csv`. A seleção atual exclui arquivos de torneios em andamento.
- Já existem seleção de temporadas, hashes, manifesto, validação de colunas,
  publicação atômica de CSVs e consolidação com remoção de duplicatas.
- `refresh` é `True` por padrão no código, apesar da descrição de cache no README.
- A chave de deduplicação inclui vencedor e perdedor. Ela não é suficiente para
  identificar a mesma partida quando houver correção de participantes ou resultado.
- O job `materialize_historical_dataset` reconstrói features, histórico e exports;
  `train_model` é um job separado. Não há schedules registrados em `defs.py`.
- A predição manual já reutiliza as fórmulas do treino, aceita IDs no serviço web
  e exporta entradas, features, probabilidades e hashes de modelo/histórico.
- O serviço web carrega um histórico fixo na inicialização. Atualizar o arquivo
  ainda exige reiniciar a aplicação para que ela use o novo estado.
- `tourney_date` é data da fonte, com granularidade variável. A verificação
  `prediction_date > last_source_date` não comprova que a previsão antecede a partida.

## Entrega 1 — Atualização confiável dos resultados

1. Validar amostras de uma temporada anual e de `ongoing_tourneys.csv`: cobertura,
   schema, status de conclusão, IDs, datas, duplicatas e passagem para o anual.
   A existência desse arquivo está documentada; seu conteúdo ainda precisa ser validado.
2. Atualizar a temporada corrente e reconsultar uma janela de temporadas recentes
   para capturar correções. Permitir atualização explícita de temporadas antigas.
   Consolidar também os anos anteriores locais para preservar o aquecimento.
3. Tratar o arquivo em andamento como observações de resultados recentes, nunca
   como um arquivo anual completo. Seu desaparecimento não significa excluir partidas.
   Definir precedência e reconciliação com os anuais após inspecionar exemplos reais.
4. Definir uma identidade canônica independente do vencedor e da data agendada.
   Validar unicidade de temporada/torneio/número da partida antes de adotar essa
   combinação; persistir mapeamentos e encaminhar colisões para revisão.
5. Guardar snapshots brutos imutáveis e metadados: fonte, coleta em UTC, hash,
   versão do schema, contagens e alterações. Validar o arquivo completo antes de
   publicar uma atualização; uma falha parcial mantém a versão publicada anterior.
6. Detectar inclusões, alterações e exclusões em snapshots comparáveis. Uma linha
   ausente em arquivo parcial não é uma exclusão confirmada. Emitir resumo de mudanças.
7. Reconstruir cronologicamente features e `PlayerHistory` quando o conteúdo mudar.
   Publicar um conjunto consistente de artefatos, identificado por versão, e trocar
   a referência ativa apenas após validação. Sem mudanças, evitar reconstrução.
8. Começar com execução manual do fluxo completo. Após validar, propor uma execução
   diária no Dagster; só aumentar a frequência se a disponibilidade da fonte justificar.
   Tornar explícito `refresh` na configuração e corrigir sua documentação.

Critérios de aceite:

- Repetir a mesma coleta não duplica partidas nem altera o estado calculado.
- Correção de resultado e chegada atrasada produzem o mesmo estado de uma
  reconstrução completa sobre o conjunto corrigido.
- Migração do arquivo em andamento para o anual mantém uma única partida.
- Arquivos inválidos e falhas de download não substituem a versão ativa válida.
- Dados novos chegam aos exports e ao histórico usado pela aplicação, com versão
  e horário de coleta visíveis. Inicialmente, a troca pode usar reinício controlado.

## Entrega 2 — Programação das próximas partidas

Investigar primeiro a programação do TennisMyLife. Verificar acesso reproduzível,
cobertura ATP, antecedência, frequência de atualização e condições de uso. Um
calendário de torneios ou resultados de torneios em andamento não comprova a
existência de confrontos futuros disponíveis para coleta.

Contrato mínimo proposto para cada confronto:

| Campo | Regra |
| --- | --- |
| Identidade | ID interno estável e referências por fonte |
| Participantes | Dois IDs canônicos; preservar IDs e nomes originais |
| Contexto | Torneio, temporada, rodada e superfície |
| Programação | Horário previsto em UTC quando disponível, fuso original, data local e precisão conhecida |
| Estado | Programada, em andamento, concluída, adiada, cancelada ou walkover |
| Proveniência | Fonte, primeira/última observação, versão e hash do conteúdo |

Preservar as revisões de programação. Adiamento não cria outra partida;
substituição de participante invalida as predições da composição anterior.
Não inferir identidade somente por nomes. Resolver por IDs e aliases verificados;
ambiguidades, oponentes ainda indefinidos e jogadores sem histórico ficam pendentes.

Critério de aceite: coletar e reconciliar a programação de um torneio real em
consultas repetidas, incluindo adiamento e mudança de participante. Caso a fonte
atual não atenda, documentar a lacuna e comparar fontes adicionais por cobertura,
IDs, horários, acesso e custo antes de escolher uma.

## Entrega 3 — Predição em lote e uso na aplicação

1. Selecionar confrontos programados em uma janela configurável, inicialmente
   próximos dois dias, sem incluir partidas já iniciadas ou concluídas.
2. Reutilizar `features_for_players` e o carregamento do modelo compatível.
   Fixar a versão numérica do modelo para cada lote, mesmo quando escolhido por alias.
3. Usar somente o histórico publicado e conhecido no instante da predição.
   Guardar snapshots para reproduzir esse estado depois. Não usar o histórico final
   para reconstruir retrospectivamente o que teria sido previsto antes de uma partida.
4. Aplicar bloqueios explícitos: identidade ambígua, ausência de histórico,
   superfície desconhecida, modelo incompatível ou evidência temporal insuficiente.
   Preservar inicialmente a restrição de data existente; previsões no mesmo dia
   exigem uma política temporal adicional e evidência de que o jogo não começou.
5. Persistir cada previsão com ID da partida, revisão da programação, participantes,
   instante de geração, probabilidades, features, modelo/run ID, hashes e referências
   dos artefatos preservados. Um hash sozinho não garante reprodutibilidade.
6. Deduplicar pela partida/revisão, modelo, histórico e versão das features.
   Novas versões geram novos registros; previsões anteriores permanecem consultáveis.
7. Exibir próximas partidas e probabilidades na aplicação, com atualização dos dados,
   modelo utilizado e motivo quando não for possível prever.

Critérios de aceite: um lote repetido não duplica registros; previsão individual
e em lote coincidem com os mesmos artefatos; os registros permanecem reproduzíveis
após atualizar histórico/modelo; jogos iniciados, cancelados e pendências são excluídos.

## Entrega 4 — Resultado observado e avaliação prospectiva

Associar resultados ao ID interno da partida por mapeamentos de fonte verificados.
Guardar o resultado observado e suas revisões sem alterar a previsão original.
Definir previamente qual previsão conta nas métricas, por exemplo a última previsão
elegível antes do início confirmado, mantendo as demais para auditoria.

Separar walkovers, cancelamentos e partidas com temporalidade não comprovada.
Definir e registrar a política de abandonos antes de calcular métricas. Acompanhar
accuracy, Brier, log loss, cobertura de predições e defasagem dos dados. Medir
calibração quando houver volume suficiente. O conjunto avaliado deve conter apenas
previsões cuja geração anterior ao início possa ser comprovada.

Atualizar o histórico não exige retreinar o modelo. Manter o treinamento e a
promoção de versões como etapas independentes, orientadas pela avaliação temporal.

## Ordem de execução e otimização posterior

Implementar e validar a entrega 1; em paralelo conceitual, investigar a viabilidade
da fonte da entrega 2. Conectar o lote e a interface somente após definir identidade,
programação e política temporal. Encerrar o primeiro ciclo com avaliação prospectiva.

O processamento incremental dos estados fica para depois de medir o custo real
da reconstrução. Seu aceite exige equivalência com reconstrução completa, inclusive
grupos temporais ambíguos, correções antigas e referências de imputação.
O particionamento anual também permanece uma otimização, conforme `partitioning.md`.

## Referências e limites da investigação

- [TennisMyLife: downloads e documentação](https://stats.tennismylife.org/tennis-match-database):
  lista arquivos anuais e de torneios em andamento; documenta IDs ATP e `tourney_date`
  como semana do torneio, além de correções e remoções históricas.
- [TennisMyLife: página inicial](https://stats.tennismylife.org/): apresenta calendário
  e programação; isso não confirma um feed utilizável de confrontos futuros.
- Inspeção local: `pipelines/historical_matches/{config,assets,defs}.py`,
  `ml/predict.py`, `web/service.py`, `README.md` e `TODO.md`.
- A API do catálogo e o download do arquivo em andamento não puderam ser inspecionados
  pelo navegador nesta investigação. Não há confirmação de schema/status/horários.
- `uv run --no-sync dg list defs --json` não listou definições neste ambiente;
  reportou falhas de acesso ao cache do uv. A análise das definições foi estática.
