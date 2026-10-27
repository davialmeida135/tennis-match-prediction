# Melhorias e próximos passos

Objetivo: validar o ciclo de histórico → resultados novos → previsão reproduzível,
com features calculadas apenas a partir das informações disponíveis antes do confronto.

Plano proposto para o próximo ciclo: [resultados novos e predições de partidas
futuras](docs/results-and-predictions-plan.md). A sequência prioriza atualização
confiável com reconstrução completa, programação, predição em lote e avaliação
prospectiva. O processamento incremental dos estados fica como otimização posterior.

## Base implementada

- [x] Criar página local de uso pessoal com diretório/perfis de jogadores e predição manual por IDs, superfície e data.
- [x] Exibir probabilidades, odds decimais justas, referência do histórico e seleção de modelos compatíveis.
- [x] Exportar previsão em JSON com entradas, features, timestamp e hashes do modelo/histórico.
- [x] Testar API e controles da interface; executar pytest e validação das definições do Dagster na CI.
- [x] Definir features e contratos de dados com Pydantic (`src/tennis_match_prediction/contracts.py`).
- [x] Implementar download de temporadas e consolidação dos dados históricos.
- [x] Compartilhar as fórmulas das features entre processamento histórico e predição.
- [x] Persistir `PlayerHistory` com `last_source_date` e estados dos jogadores em `player_history.parquet`.
- [x] Prever com nomes, data e superfície usando o histórico salvo (`src/tennis_match_prediction/ml/predict.py`).
- [x] Registrar métricas e artefatos de treinamento no MLflow (`src/tennis_match_prediction/ml/train.py`).
- [x] Separar aquecimento, treino, validação e teste por datas explícitas.
- [x] Compartilhar interface de modelos e runner entre regressão logística e random forest.
- [x] Executar experimentos na validação; calcular métricas de teste apenas com avaliação final explícita.
- [x] Configurar MLflow com modos server/local/disabled e Docker Compose com volumes persistentes.
- [x] Salvar artefatos por experimento, configuração, hash do dataset e código/dependências.
- [x] Calcular comparações antes de atualizar resultados dos grupos com ordem ambígua.
- [x] Remover features de carga em 7/14 dias enquanto não houver datas reais das partidas.

## Ciclo operacional e avaliação prospectiva

- [x] Associar previsões arquivadas aos resultados pelo ID de partida da Live Tennis API e conferir os IDs dos participantes, mesmo com ordem invertida.
- [x] Preservar revisões dos resultados e relatórios sem alterar as previsões originais.
- [x] Avaliar a última previsão elegível por partida e versão exata do modelo; excluir cancelamentos, walkovers, abandonos e resultados sem evidência temporal suficiente.
- [x] Gerar relatório Markdown/JSON com accuracy, Brier, log loss, cobertura, defasagem do histórico, calibração descritiva e métricas por superfície; disponibilizar a página Performance.
- [x] Criar comando diário de atualização do histórico e avaliação, sem retreino, com relatórios de sucesso/falha por etapa e bloqueio de execuções concorrentes.
- [x] Registrar job `daily_update` e schedule diário às 06:00 em America/Sao_Paulo; execução depende do daemon Dagster em funcionamento.
- [x] Integrar resultados de `ongoing_tourneys.csv`, reter observações ausentes do arquivo parcial e dar precedência ao anual; bloquear identidades ambíguas e conflitos de participantes.
- [x] Evitar reconstrução quando o conteúdo consolidado, código/dependências e artefatos publicados continuam iguais.
- [x] Recarregar na aplicação snapshots publicados e verificados; preservar o histórico anterior se o novo snapshot for inválido.
- [ ] Validar longitudinalmente migração entre arquivo parcial/anual, adiamentos e correções reais do provedor.
- [ ] Fortalecer a comprovação do início efetivo: `live_at` é observação do provedor e pode ser regravado, não um horário oficial. A política atual usa também os horários agendados original/final como cortes conservadores.
- [ ] Acumular amostra prospectiva suficiente antes de concluir sobre desempenho ou calibração.
- [ ] Definir retenção de snapshots e relatórios e recuperação após encerramento abrupto com lock abandonado.

Uso e políticas: [rotina diária e desempenho](docs/daily-update-and-performance.md).

## 1. Cronologia e qualidade das features

- [ ] Documentar a granularidade de `tourney_date` por temporada/fonte: data da partida, início do torneio ou outra referência.
- [ ] Validar quando `match_num` representa uma sequência confiável e quando é necessário congelar o estado anterior para todo o grupo.
- [ ] Ampliar testes com várias rodadas do mesmo torneio, datas empatadas e números de partida ausentes; verificar que nenhum resultado do grupo ambíguo entra nas suas próprias features.
- [ ] Verificar que Elo, H2H, forma recente e estatísticas de saque usam a mesma fronteira de informação disponível.
- [ ] Obter datas efetivas das partidas antes de reintroduzir `minutes_7d_diff` e `matches_14d_diff`.
- [x] Manter janela independente das últimas 10 partidas por superfície, com teste após mais de 50 partidas em outras superfícies.
- [x] Medir ausência de ranking, pontos, idade e estatísticas de saque; distinguir valores desconhecidos de zeros legítimos. Relatório por temporada em `docs/feature-quality.md` e `docs/feature-quality-coverage.json`.
- [x] Calcular taxas de saque com denominadores específicos e observações completas; rejeitar contagens inválidas e preservar zeros observados.
- [x] Neutralizar comparações sem informação, compartilhando as fórmulas entre treino e predição.
- [ ] Medir defasagem dos últimos valores conhecidos de ranking, pontos e idade; avaliar alternativas na validação.

Critério de conclusão: política temporal documentada e testes que comprovem a
consistência das features nos casos de ordem conhecida e ambígua. `last_source_date`
deve continuar identificado como data da fonte, sem ser apresentado como data
efetiva do último resultado quando essa informação não estiver disponível.

## 2. Aquecimento e avaliação temporal

- [ ] Comparar períodos de aquecimento de 6, 12 e 24 meses, mantendo o mesmo conjunto de features e os mesmos períodos de validação/teste.
- [ ] Preservar as partidas de aquecimento na construção do histórico; excluí-las apenas do ajuste do modelo e das métricas.
- [ ] Não descartar linhas apenas por terem features zeradas: diferenças iguais a zero e ausência de confrontos anteriores podem ser legítimas.
- [ ] Registrar contadores de histórico disponível por jogador: partidas totais, partidas na superfície e pontos de saque observados.
- [ ] Medir desempenho separadamente para jogadores com pouco e muito histórico, incluindo estreantes.
- [ ] Verificar que os cortes não dividem grupos temporais ambíguos ou torneios cuja ordem real não pode ser reconstruída.
- [ ] Executar backtests em várias janelas futuras, com treino anterior à validação e teste posterior a ambos.
- [ ] Comparar a regressão logística com baselines de 50%, ranking e Elo; avaliar accuracy, Brier e log loss.
- [ ] Avaliar métricas por superfície, temporada, nível de torneio e quantidade de histórico.
- [ ] Gerar curvas de calibração e avaliar se probabilidades previstas correspondem às frequências observadas.
- [ ] Escolher features, parâmetros, aquecimento e eventual calibração usando a validação; reservar o teste final para a configuração escolhida.

Critério de conclusão: relatório reproduzível com períodos, contagens, baselines
e resultados de múltiplas janelas. O scaler e o modelo devem ser ajustados somente
no treino; qualquer calibrador deve respeitar a separação temporal.

## 3. Resultados novos e processamento incremental

- [x] Executar atualização de temporadas e reconstrução completa de features, datasets e histórico em um comando, sem retreinar.
- [x] Isolar as saídas de cada tentativa, verificar qualidade antes de publicar e registrar tempos por etapa/total e hashes.
- [x] Testar reconstrução após inclusões, correções e exclusões, repetição da atualização e preservação das saídas publicadas quando checks falham.
- [x] Medir reconstrução com o volume real: 200.888 linhas brutas / 199.313 normalizadas, 533,16 s; manter reconstrução completa inicialmente e acompanhar o custo nas próximas atualizações.
- [ ] Identificar partidas novas por uma chave estável e evitar reaplicar resultados já processados.
- [ ] Atualizar os estados dos jogadores a partir do histórico persistido, sem reconstruir tudo em cada atualização.
- [ ] Garantir que grupos ambíguos sejam processados completos; uma atualização parcial não deve quebrar o congelamento do estado anterior.
- [ ] Definir como detectar correções, exclusões e resultados atrasados em temporadas já ingeridas.
- [ ] Reconstruir o estado a partir de um ponto seguro quando houver alterações históricas.
- [ ] Avaliar particionamento da ingestão por ano, preservando a continuidade do histórico entre temporadas.
- [x] Validar os arquivos anuais baixados antes de substituir versões locais válidas e manter um manifesto das temporadas.
- [x] Validar o fluxo completo com reconstrução: histórico inicial → resultados novos → features → histórico atualizado → predição manual. Execução real em 27/10/2026, sem falhas nos checks; processamento incremental continua pendente.

Critério de conclusão: processamento incremental e reconstrução completa produzem
as mesmas features e estados, dentro da tolerância numérica definida. Reprocessar
o mesmo lote não altera o resultado.

## 4. Histórico persistido e rastreabilidade

- [ ] Documentar a função de `player_history.parquet`: guardar estados para calcular features futuras sem reler todo o histórico.
- [ ] Explicar a diferença entre o modelo aprendido, os estados dos jogadores e `last_source_date`.
- [ ] Documentar que previsões históricas exigem o estado anterior ao confronto; o estado final não pode ser usado para avaliar o passado.
- [ ] Versionar o schema do histórico e das features, com política de compatibilidade ou reconstrução.
- [x] Registrar hashes dos dados, commit, configuração, períodos e identificador do treinamento nos artefatos e no MLflow.
- [x] Identificar o snapshot de histórico usado em cada previsão e verificar compatibilidade com o modelo.
- [x] Salvar previsões com entradas, features, probabilidades, versão do modelo e referência ao histórico.
- [ ] Avaliar o formato de persistência do histórico conforme volume e uso: JSON dentro de Parquet, tabelas estruturadas ou banco de dados.

Critério de conclusão: uma previsão registrada pode ser reproduzida com os
artefatos e configurações identificados, mesmo após novas atualizações.

## 5. Partidas futuras e uso cotidiano

- [x] Gerar predições em lote pela Live Tennis API e exibir próximos jogos no frontend, ignorando jogadores não encontrados/ambíguos e partidas adiadas.
- [x] Preservar fonte, modelo, histórico e features por lote; disponibilizar download JSON e ocultar jogos cujo horário previsto já passou.
- [x] Testar a Live Tennis API com chave gratuita em branch separada; registrar cobertura, diferenças de IDs/ordenação e candidatos de identidade. [Resultado](docs/live-tennis-api-test.md).
- [x] Investigar o TennisMyLife: validar CSV em andamento, JSON de programação e amostras das APIs. [Evidências e lacunas](docs/upcoming-matches-source-investigation.md).
- [x] Definir uma fonte para buscar partidas futuras, com IDs, superfície e data prevista: Live Tennis API.
- [ ] Resolver jogadores por ID e manter aliases de nomes, tratando ambiguidades explicitamente.
- [x] Rejeitar confrontos do jogador consigo mesmo.
- [ ] Definir comportamento para jogadores ausentes do histórico e histórico insuficiente.
- [x] Informar a referência temporal do histórico e sua possível defasagem na saída da predição.
- [x] Implementar predição em lote para a programação de partidas.
- [x] Associar resultados posteriores às previsões para acompanhar desempenho fora da amostra, conforme política temporal documentada.

## 6. Dagster, CI e documentação

- [ ] Revisar dependências e artefatos do Dagster para conectar ingestão, atualização das features e predição em lote.
- [x] Definir automação da ingestão e atualização do histórico, com frequência de treinamento independente.
- [x] Evitar recomputação dos dados quando a fonte e a implementação não tiverem mudanças e os artefatos estiverem íntegros.
- [x] Adicionar `pytest` à CI e ampliar Ruff para `pipelines`, `ml` e `tests`.
- [x] Validar carregamento das definições do Dagster na CI.
- [ ] Atualizar `docs/project-diagram.md`, removendo referências ao antigo `feature_state.py` e ao treino direto dos dados brutos.
- [ ] Manter README, comandos de treinamento, schema das features e diagrama consistentes com o código atual.

## 7. Experimentos posteriores

Executar após estabilizar a cronologia e o protocolo de avaliação, com comparação
na validação e controle das versões dos experimentos.

- [ ] Avaliar Elo e forma recente por local/torneio, considerando tamanho da amostra.
- [ ] Avaliar vantagem relacionada ao pico de idade.
- [ ] Avaliar taxas históricas de quebra e confirmação de saque.
- [ ] Comparar janelas recentes de estatísticas de saque com os acumulados de carreira.
- [ ] Avaliar modelos alternativos e manter apenas mudanças com ganho consistente nas janelas temporais.

Referência exploratória original para ideias de features:
[publicação no Instagram](https://www.instagram.com/p/DdPLc3Goo6j/).
