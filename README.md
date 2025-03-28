# tennis-match-prediction

Repositório para trabalho da disciplina de Introdução à Inteligência Artificial

Envolve tratamento de dados e aplicaão de modelos de aprendizado de máquina

## Datasets

Partidas até 2022
https://www.kaggle.com/datasets/sijovm/atpdata

Partidas de 1968-2024

https://www.kaggle.com/datasets/guillemservera/tennis

API pra pegar mais dados no futuro

https://developer.sportradar.com/tennis/reference/overview

## Conceito inicial

Dadas informações sobre 2 jogadores, retornar se o vencedor é o jogador 1 ou 2

| Player1_info| Player2_info| Winner
    123             124         1

## TODO
### Limpeza inicial
- Tirar colunas desnecessárias
- Limpar W/O

### Cálculo de dados dos jogadores
- Winrate Total
- Winrate nas ultimas 50 partidas
- Winrate das ultimas 10 partidas
- Winrate em uma superfície
- Winrate superfície ultimas 50
- Winrate superfície ultimas 10
- Winrate em um torneio
- ELO
- Diferença de rank
- Diferença de ELO
- Diferença de idade
- Diferença de Altura


### Limpeza final
- Limpar colunas desnecessárias
- Anonimizar os dados

## DONE
### Limpeza inicial
- Merge datasets
- Refazer seeds (one-hot encoding nos entry_methods)

### Cálculo de dados dos jogadores
- H2H
- Tempo jogado em um torneio
- Helper functions de pegar todos os confrontos entre jogadores e todas as partidas de um jogador

### Limpeza final
- 