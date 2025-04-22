# tennis-match-prediction

Repositório para trabalho da disciplina de Introdução à Inteligência Artificial

Envolve tratamento de dados e aplicaão de modelos de aprendizado de máquina

## Datasets

Partidas até 2022
https://www.kaggle.com/datasets/sijovm/atpdata

Partidas de 1968-2024

https://www.kaggle.com/datasets/guillemservera/tennis

Partidas 2000-2025
https://www.kaggle.com/datasets/dissfya/atp-tennis-2000-2023daily-pull

Script tratamento partidas
https://www.kaggle.com/code/dissfya/atp-tennis-daily-pull
http://tennis-data.co.uk/2025/2025.xlsx

API pra pegar mais dados no futuro

https://developer.sportradar.com/tennis/reference/overview

## Conceito inicial

https://www.tennisabstract.com/blog/2019/12/03/an-introduction-to-tennis-elo/

Dadas informações sobre 2 jogadores, retornar se o vencedor é o jogador 1 ou 2

| Player1_info| Player2_info| Winner
    123             124         1

## TODO
### Limpeza inicial
- Tirar colunas desnecessárias

### Cálculo de dados dos jogadores
- Winrate em um torneio
- H2H recente (last 5) (polars) talvez fazer H2H padrao com polars tambem
- Diferença de rank
- Diferença de idade
- Diferença de Altura
- Win streak (polars)
- Win streak na superfície (polars)
- Win streak no torneio (polars)
- Winrate Total (polars)
- Winrate nas ultimas 50 partidas (polars)
- Winrate das ultimas 10 partidas (polars)
- Winrate em uma superfície (polars)
- Winrate superfície ultimas 50 Consertar isso (polars)
- Winrate superfície ultimas 10 Consertar isso (polars)

### Limpeza final
- Limpar colunas desnecessárias
- Surface one-hot encode
- Tirar player id, score, match_num, tourney_date
- tirar partidas RET
- Anonimizar os dados
- Numerar tourney level
- Numerar round

## DONE
### Limpeza inicial
- Merge datasets
- Refazer seeds (one-hot encoding nos entry_methods)
- Limpar W/O
- Fillar altura, idade, minutos(com media do torneio)
- Fillar falta de rank (não sei como)
- Numerar mão do forehand

### Cálculo de dados dos jogadores
- H2H
- ELO
- Diferença de ELO
- Tempo jogado em um torneio
- Helper functions de pegar todos os confrontos entre jogadores e todas as partidas de um jogador
- Partidas ganhas

### Limpeza final
- Mudar tipo das datas
