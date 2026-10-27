# Partidas futuras: investigação do TennisMyLife

Data da verificação: 27/10/2026 (America/Sao_Paulo).

## Decisão

Os pontos de acesso consultados não atendem aos requisitos para coletar partidas
futuras automaticamente. O CSV de torneios em andamento é útil para resultados
recentes; não deve alimentar a lista de jogos ainda não iniciados. O JSON público
de programação encontrado também não contém os dados necessários.

Isso não comprova que o provedor não tenha outros dados ou serviços. A conclusão
se limita ao catálogo, às rotas públicas e ao código inspecionados nesta data.
O próximo passo é avaliar uma fonte adicional de programação, preservando o
TennisMyLife para resultados e histórico.

## Evidências consultadas

| Acesso | Resultado observado |
| --- | --- |
| `/api/data-files` | HTTP 200; catálogo inclui anual ATP e `ongoing_tourneys.csv` |
| `/data/ongoing_tourneys.csv` | HTTP 200; 18 linhas, 50 colunas, Basel e Vienna |
| `/data/2026.csv` | HTTP 200; 2.656 linhas; nenhuma das 18 partidas do arquivo em andamento aparece neste anual |
| `/schedule/matches.json` | HTTP 200; 19 pares de jogadores agrupados em `r64` (16) e `r32` (3) |
| `/api/matches?player_id=BK40&limit=3` | HTTP 200; três resultados com placar e `status: true` |
| `/api/matches` com `S0AG`, `A0E2` e `D0CO`, limite 10 | 30 registros adicionais com placar e `status: true` |
| `/api/tournaments/basel/2026` | HTTP 200; nove partidas, todas com placar; nenhuma com `score: "To play"` |

As contagens são retratos da coleta, não garantias permanentes de cobertura ou
atualização. A consulta por jogador foi amostral, não uma varredura de todos os IDs.

### CSV de torneios em andamento

Na amostra, todas as linhas têm `winner_id`, `loser_id` e `score` preenchidos.
Existem nomes, superfície, rodada, número de partida e estatísticas de resultado.
Não existem colunas de horário agendado, fuso, início efetivo, adiamento ou
cancelamento. `minutes` é duração, não horário de início.

Os 36 IDs distintos dos participantes foram encontrados em
`data/curated/player_history.parquet`. A superfície observada foi `Hard`, compatível
com o contrato de predição. Não houve duplicatas de `(tourney_id, match_num)` nesta
amostra; isso não demonstra estabilidade dessa chave entre revisões.

O catálogo informava atualização do arquivo em andamento em
`2026-10-27T16:16:28.117Z` e do anual em `2026-10-26T06:12:14.756Z`. A ausência de
sobreposição ilustra a necessidade de reconciliar essas duas fontes ao atualizar
resultados; não significa que elas sejam sempre disjuntas. Não houve observação
longitudinal da migração de uma partida para o anual nesta investigação.

`tourney_date` continua sendo data de referência da fonte. A documentação a descreve
como semana do torneio; não converter esse campo em horário previsto ou confirmado
de uma partida.

### JSON de programação

O arquivo publicado contém apenas `players` (slug do par), `href` (link H2H) e,
em parte das entradas, `status: "assente"`. O significado desse status não foi
documentado nas evidências consultadas; ele não pode ser mapeado para cancelamento
ou adiamento.

Faltam ID de partida, IDs ATP dos jogadores, torneio/edição, superfície, data,
horário, fuso e timestamp de atualização. Um par H2H não identifica uma partida:
os mesmos jogadores podem se enfrentar em diferentes eventos e datas.

O último commit desse arquivo no repositório consultado é de
`2026-05-08T15:40:50Z`, hash `6abb5c6c90ebb5d550eb7fab2ef5e4eefa85e4ea`.
Isso descreve o histórico do repositório, não prova a data de atualização do
servidor. O conteúdo do servidor foi inspecionado separadamente e apresenta as
mesmas lacunas de schema. Não há evidência de atualização contínua para a
programação de outubro.

### APIs e código do site

O modelo `Match` no schema Prisma consultado é orientado a vencedor/perdedor,
placar e `tourney_date`. Seu `status` é booleano opcional, sem enum de estados de
programação. O schema não contém um horário agendado ou histórico de adiamentos.
Não interpretar automaticamente `status: true` como "agendada" ou outro estado.

A rota por jogador retorna esse formato e não exige que o placar esteja preenchido;
portanto, as amostras de resultados não provam ausência global de registros futuros.
Já a rota de edição de torneio exclui explicitamente `score: "To play"`. A presença
desse filtro é uma indicação de que há uma convenção para registros não jogados;
não estabelece que exista um feed público completo e atualizado deles. Os registros
futuros não foram encontrados nas amostras consultadas.

## Atendimento aos requisitos

| Requisito | CSV em andamento | JSON de programação | APIs amostradas |
| --- | --- | --- | --- |
| Confrontos comprovadamente futuros | Não encontrados | Sem data/status verificável | Não encontrados |
| IDs de jogadores compatíveis | Sim, para os 36 participantes observados | Ausentes | Presentes nos resultados |
| Identidade estável da partida entre revisões | Não validada | Ausente | Há ID interno; estabilidade não validada |
| Superfície | Presente | Ausente | Presente |
| Horário previsto e fuso | Ausentes | Ausentes | Ausentes no formato consultado |
| Adiamento/cancelamento explícito | Ausentes | Status sem semântica validada | Sem estado adequado validado |
| Histórico de revisões | Não fornecido | Não fornecido | Não identificado |

Adiamentos não podem ser validados a partir de uma única fotografia. Não inferir
cancelamento quando uma linha desaparecer nem criar outra partida só porque o
horário mudou.

## Próxima implementação proposta

1. Avaliar uma fonte adicional com programação ATP simples, participantes e IDs,
   superfície, torneio/rodada, data e estados explícitos. Comparar acesso e custo;
   nenhuma assinatura ou integração adicional foi criada nesta investigação.
2. Validar o mapeamento dos IDs dessa fonte para os IDs ATP já usados pelo histórico.
   Usar nomes apenas para sugerir correspondências; manter ambiguidades pendentes.
3. Persistir snapshots e revisões por ID de origem, com coleta em UTC e precisão
   temporal explícita. Se houver somente data local, guardar data/fuso conhecidos
   sem inventar uma meia-noite UTC ou afirmar que o jogo ainda não começou.
4. Verificar a programação de um torneio real em consultas repetidas, incluindo
   alterações de horário, participantes, adiamentos e passagem para resultado.
5. Só habilitar predição automática quando houver evidência temporal suficiente
   para excluir partidas já iniciadas. Preservar os bloqueios atuais do motor até
   implementar a política para previsões no mesmo dia.

A integração do CSV em andamento ao histórico é um trabalho separado: precisa
reconciliar sobreposição e correções com os anuais antes da reconstrução completa.
Nenhum dataset de treino ou histórico foi atualizado durante esta investigação.

## Fontes e reprodução

- [Documentação e downloads](https://stats.tennismylife.org/tennis-match-database).
- [Catálogo](https://stats.tennismylife.org/api/data-files).
- [CSV de torneios em andamento](https://stats.tennismylife.org/data/ongoing_tourneys.csv).
- [JSON público de programação](https://stats.tennismylife.org/schedule/matches.json).
- [API por jogador, amostra BK40](https://stats.tennismylife.org/api/matches?player_id=BK40&limit=3).
- [API de Basel 2026](https://stats.tennismylife.org/api/tournaments/basel/2026).
- Código do provedor fixado no commit `741b74be2f1d155a1726af9b42cb138b554ea75d`:
  [schema Prisma](https://github.com/Tennismylife/TML-Website/blob/741b74be2f1d155a1726af9b42cb138b554ea75d/prisma/schema.prisma),
  [rota por jogador](https://github.com/Tennismylife/TML-Website/blob/741b74be2f1d155a1726af9b42cb138b554ea75d/app/api/matches/route.ts),
  [rota por torneio](https://github.com/Tennismylife/TML-Website/blob/741b74be2f1d155a1726af9b42cb138b554ea75d/app/api/tournaments/%5Bid%5D/%5Byear%5D/route.ts)
  e [JSON de programação](https://github.com/Tennismylife/TML-Website/blob/741b74be2f1d155a1726af9b42cb138b554ea75d/public/schedule/matches.json).
- [Último commit do arquivo de programação](https://github.com/Tennismylife/TML-Website/commit/6abb5c6c90ebb5d550eb7fab2ef5e4eefa85e4ea).

As respostas e trechos consultados foram preservados localmente em
`data/tmp/fixture-source-investigation/`; a árvore do repositório está em
`data/tmp/tml-website-tree.json`. Esses arquivos temporários não são versionados.
SHA-256 do CSV em andamento baixado:
`64fa3c142cd5f2ea2ad4a887c6fe8a882202458483e511c0a81fc4a997575ced`.

A página atual declara uso pessoal, educacional e pesquisa não comercial, com
restrições de redistribuição e uso comercial. As amostras brutas foram mantidas
localmente; a disponibilidade pública não deve ser tratada como licença irrestrita.
