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

Dadas informações sobre 2 jogadores, retornar se o vencedor é o jogador 1 ou 2

| Player1_info| Player2_info| Winner
    123             124         1

## TODO
### Limpeza inicial
- Tirar colunas desnecessárias
- Numerar tourney level
- Numerar round
- Numerar mão do forehand

### Cálculo de dados dos jogadores
- Winrate em um torneio
- H2H recente (last 5)
- ELO
- Diferença de rank
- Diferença de ELO
- Diferença de idade
- Diferença de Altura
- Win streak
- Win streak na superfície
- Win streak no torneio (massa)

### Limpeza final
- Limpar colunas desnecessárias
- Surface one-hot encode
- Tirar player id, score, match_num, tourney_date
- tirar partidas RET
- Anonimizar os dados
- Mudar tipo das datas

## DONE
### Limpeza inicial
- Merge datasets
- Refazer seeds (one-hot encoding nos entry_methods)
- Limpar W/O
- Fillar altura, idade, minutos(com media do torneio)
- Fillar falta de rank (não sei como)

### Cálculo de dados dos jogadores
- H2H
- Tempo jogado em um torneio
- Helper functions de pegar todos os confrontos entre jogadores e todas as partidas de um jogador
- Winrate Total 
- Winrate nas ultimas 50 partidas
- Winrate das ultimas 10 partidas
- Winrate em uma superfície Consertar isso (não precisa de uma coluna pra cada superfície)
- Winrate superfície ultimas 50 Consertar isso
- Winrate superfície ultimas 10 Consertar isso
- Partidas ganhas

### Limpeza final
- 