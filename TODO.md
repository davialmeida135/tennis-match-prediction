# Prioridade

- [ ] Ajustar ingestão de dados novos de https://stats.tennismylife.org/tennis-match-database
- [ ] Definir features para modelo de ML
- [ ] Definir forma de buscar partidas futuras para prever
- [ ] Validar pipeline end-to-end com dados históricos + novos
- [ ] Predict com apenas nome dos jogadores, data e superfície como entrada; buscar/calcular os demais dados automaticamente
- [ ] Transferir Wandb -> MLFlow
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