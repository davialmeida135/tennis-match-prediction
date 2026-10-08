# Melhorias e próximos passos

Objetivo: validar o ciclo de histórico → resultados novos → previsão reproduzível,
com features calculadas apenas a partir das informações disponíveis antes do confronto.

## Base implementada

- [x] Definir features e contratos de dados com Pydantic (`src/tennis_match_prediction/contracts.py`).
- [x] Implementar download de temporadas e consolidação dos dados históricos.
- [x] Compartilhar as fórmulas das features entre processamento histórico e predição.
- [x] Persistir `PlayerHistory` com `last_source_date` e estados dos jogadores em `player_history.parquet`.
- [x] Prever com nomes, data e superfície usando o histórico salvo (`src/tennis_match_prediction/ml/predict.py`).
- [x] Registrar métricas e artefatos de treinamento no MLflow (`src/tennis_match_prediction/ml/train.py`).
- [x] Separar aquecimento, treino, validação e teste por datas explícitas.
- [x] Calcular comparações antes de atualizar resultados dos grupos com ordem ambígua.
- [x] Remover features de carga em 7/14 dias enquanto não houver datas reais das partidas.

## 1. Cronologia e qualidade das features

- [ ] Documentar a granularidade de `tourney_date` por temporada/fonte: data da partida, início do torneio ou outra referência.
- [ ] Validar quando `match_num` representa uma sequência confiável e quando é necessário congelar o estado anterior para todo o grupo.
- [ ] Ampliar testes com várias rodadas do mesmo torneio, datas empatadas e números de partida ausentes; verificar que nenhum resultado do grupo ambíguo entra nas suas próprias features.
- [ ] Verificar que Elo, H2H, forma recente e estatísticas de saque usam a mesma fronteira de informação disponível.
- [ ] Obter datas efetivas das partidas antes de reintroduzir `minutes_7d_diff` e `matches_14d_diff`.
- [ ] Avaliar histórico por superfície separado: as últimas 50 partidas gerais podem não conter as últimas 10 de determinada superfície.
- [ ] Medir ausência de ranking, pontos, idade e estatísticas de saque; distinguir valores desconhecidos de zeros legítimos.

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

- [ ] Identificar partidas novas por uma chave estável e evitar reaplicar resultados já processados.
- [ ] Atualizar os estados dos jogadores a partir do histórico persistido, sem reconstruir tudo em cada atualização.
- [ ] Garantir que grupos ambíguos sejam processados completos; uma atualização parcial não deve quebrar o congelamento do estado anterior.
- [ ] Definir como detectar correções, exclusões e resultados atrasados em temporadas já ingeridas.
- [ ] Reconstruir o estado a partir de um ponto seguro quando houver alterações históricas.
- [ ] Avaliar particionamento da ingestão por ano, preservando a continuidade do histórico entre temporadas.
- [ ] Validar os arquivos baixados antes de substituir versões locais válidas e manter um manifesto de todas as temporadas consolidadas.
- [ ] Validar o fluxo completo: histórico inicial → lote novo → features → histórico atualizado → predição.

Critério de conclusão: processamento incremental e reconstrução completa produzem
as mesmas features e estados, dentro da tolerância numérica definida. Reprocessar
o mesmo lote não altera o resultado.

## 4. Histórico persistido e rastreabilidade

- [ ] Documentar a função de `player_history.parquet`: guardar estados para calcular features futuras sem reler todo o histórico.
- [ ] Explicar a diferença entre o modelo aprendido, os estados dos jogadores e `last_source_date`.
- [ ] Documentar que previsões históricas exigem o estado anterior ao confronto; o estado final não pode ser usado para avaliar o passado.
- [ ] Versionar o schema do histórico e das features, com política de compatibilidade ou reconstrução.
- [ ] Registrar hashes dos dados, commit, configuração, períodos e identificador do treinamento nos artefatos e no MLflow.
- [ ] Identificar o snapshot de histórico usado em cada previsão e verificar compatibilidade com o modelo.
- [ ] Salvar previsões com entradas, features, probabilidades, versão do modelo e referência ao histórico.
- [ ] Avaliar o formato de persistência do histórico conforme volume e uso: JSON dentro de Parquet, tabelas estruturadas ou banco de dados.

Critério de conclusão: uma previsão registrada pode ser reproduzida com os
artefatos e configurações identificados, mesmo após novas atualizações.

## 5. Partidas futuras e uso cotidiano

- [ ] Definir uma fonte para buscar partidas futuras, com IDs, superfície e data prevista.
- [ ] Resolver jogadores por ID e manter aliases de nomes, tratando ambiguidades explicitamente.
- [ ] Rejeitar confrontos do jogador consigo mesmo.
- [ ] Definir comportamento para jogadores ausentes do histórico e histórico insuficiente.
- [ ] Informar a referência temporal do histórico e sua possível defasagem na saída da predição.
- [ ] Implementar predição em lote para a programação de partidas.
- [ ] Associar resultados posteriores às previsões para acompanhar desempenho fora da amostra.

## 6. Dagster, CI e documentação

- [ ] Revisar dependências e artefatos do Dagster para conectar ingestão, atualização das features e predição em lote.
- [ ] Definir automação da ingestão e atualização do histórico, com frequência de treinamento independente.
- [ ] Evitar recomputação dos dados quando a fonte não tiver mudanças.
- [ ] Adicionar `pytest` à CI e ampliar Ruff para `pipelines`, `ml` e `tests`.
- [ ] Validar carregamento das definições do Dagster na CI.
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
