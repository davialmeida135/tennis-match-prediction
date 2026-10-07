# Prioridade

- [ ] Ajustar ingestão(download) de dados novos de https://stats.tennismylife.org/tennis-match-database: Modelo base é o de 2026.csv. Podemos particionar por ano.
- [ ] Como calcular features de dados novos usando como base dados antigos pré calculados?
- [x] Definir features e contratos de dados com Pydantic (`pipelines/shared/contracts.py`)
- [ ] Definir forma de buscar partidas futuras para prever
- [ ] Validar pipeline end-to-end com dados históricos + novos
- [x] Predição com nomes, data e superfície usando o estado histórico salvo (`ml/predict.py`)
- [x] Registrar métricas e artefatos de treinamento no MLflow (`ml/train.py`)
- [ ] Revisar Dagster: boas práticas, artefatos e dependências

# Ordem recomendada

1. Dados históricos
2. Dados novos
3. Busca de partidas futuras
4. Pipeline validado
5. Predição com input minimalista
6. MLFlow
7. Refatoração do Dagster

# Observação

Os itens 1, 2 e 3 são a base do projeto. O item 4 é o objetivo funcional principal. O item 5 é melhoria de MLOps. O item 6 é refinamento arquitetural.

Construir features apenas considerando partidas anteriores à observada.

Algumas features recomendadas
- overall elo diff
- surface elo diff
- rank log advantage
- points log diff
- former 10 matches diff
- surface former 10 diff
- minutes played 7d diff
- matches played 14d diff
- age diff
- venue elo diff
- venue former 10 diff
- ace rate diff
- double fault rate diff
- h2h log odds
- experience log diff
- age peak advantage
- serve break rate diff
- serve confirm rate diff

Referencia: https://www.instagram.com/p/DdPLc3Goo6j/