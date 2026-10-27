# Predições em lote e próximos jogos

A aplicação abre a tela **Upcoming matches**, com os próximos confrontos ATP
simples. **Refresh & predict** consulta a Live Tennis API e usa o modelo selecionado
em **Model & data details**, pela mesma rotina da predição manual.

## Seleção dos confrontos

- Fonte principal: `/matches?status=upcoming`, com ATP, simples e qualifying excluído.
- Aceitar somente nomes completos com correspondência normalizada única no histórico.
  Não há fuzzy matching; jogador não encontrado ou ambíguo elimina o confronto.
- Ignorar partidas adiadas, canceladas, suspensas/interrompidas, ao vivo, com placar
  ou início já observado, horário ausente/sem fuso/já passado, superfície desconhecida
  ou data incompatível com o histórico usado pelo motor.
- Preservar a orientação de `players.p1/p2` e os respectivos IDs. Não usar a ordem
  nem o ID da lista de fixtures para associar participantes às probabilidades.
- Uma atualização lê até cinco páginas de 100 linhas. Erros ou paginação incompleta
  não publicam um lote parcial. Consultas dentro de um minuto reutilizam a fonte.

A resolução aceita igualdade do nome completo após normalizar acentos, caixa e
pontuação. Isso não cria um cadastro permanente de aliases ou equivalências entre
os IDs dos dois provedores. Nomes não encontrados, como o caso observado de
`Daniel Merida Aguilar`, permanecem ignorados.

## Persistência e reprodução

Diretório padrão: `data/predictions/<batch-id>/` (configurável por `--predictions-dir`).

| Arquivo | Conteúdo |
| --- | --- |
| `batch.json` | Confrontos, IDs nas duas fontes, inputs, features, probabilidades, timestamps, hashes do modelo/histórico, run ID e motivos dos ignorados |
| `source.json` | Snapshot da programação retornada pela API |
| `model.pkl` e `model.json` | Artefato exato carregado pelo serviço e metadados compatíveis |
| `player_history.parquet` | Snapshot exato carregado pelo serviço |

As versões usadas ficam preservadas mesmo quando os arquivos de entrada mudam ou
quando o download temporário de um modelo do MLflow é removido. O lote é publicado
após gravar todos os artefatos em diretório temporário. A referência do último lote
por modelo é trocada por substituição atômica de arquivo. A identidade do modelo
na interface independe do caminho temporário do download do registry.

Repetir a mesma fonte, modelo, histórico e conjunto elegível reutiliza o lote.
Modelos e históricos diferentes não compartilham a previsão atual. Jogos cujo
horário previsto já passou são ocultados na leitura e na interface, mas os
registros originais continuam no arquivo de auditoria/download.

A tela não faz polling do provedor: alterações e novos adiamentos são conhecidos
na próxima atualização. O horário da última consulta é mostrado. A ausência de
uma partida não é interpretada como cancelamento. Os filtros são conservadores;
mesmo um horário futuro e um status da fonte podem sofrer atraso de atualização.

## API e frontend

- `GET /api/upcoming?model_id=...`: último lote do modelo e histórico atuais;
  não consulta a fonte externa.
- `POST /api/upcoming/refresh`, body `{"model_id":"..."}`: busca, filtra,
  prediz e persiste o lote. A chave permanece no servidor.
- `GET /api/upcoming/batches/<batch-id>`: JSON completo para download.

Cards exibem torneio, rodada, superfície, horário local, jogadores e probabilidades.
O usuário pode abrir perfis, inspecionar as features e baixar o lote. Há estados
para nenhuma partida elegível, falta de configuração/modelo e erro de atualização;
uma falha de refresh preserva o lote anteriormente exibido. Mudar o modelo remove
os cards anteriores enquanto o lote correspondente é carregado.

## Validação real em 27/10/2026

Modelo utilizado: `teninho`, versão `1`, run
`a3552146b64d47c3bc7bd322d52aff02`, algoritmo `hist_gradient_boosting`.
Histórico carregado com data de fonte `2026-10-18`.

A consulta retornou 11 partidas futuras: 10 receberam predição e uma foi ignorada
por jogador não encontrado/ambíguo. Um dos lotes preservados está em
`data/predictions/657b0418998a2c419e1aa27338c196199fa7347c288c4ce38a09a5a7001cff68/`.
Atualizações posteriores podem produzir novos lotes e contagens diferentes.

Testes da aplicação e do probe: 48 passaram. Os testes cobrem equivalência entre
predição manual e lote, ignorados, replay do modelo arquivado, preservação dos
bytes de modelo/histórico após mudanças nos originais, idempotência, cache da fonte,
orçamento de paginação, falha HTTP e ocultação após o horário previsto.
Suíte completa: 191 testes passaram e duas integrações opcionais foram ignoradas.

No Chrome headless, a tela exibiu dez cards; abrir perfil/features, atualizar o
lote e baixar o JSON funcionaram sem exceções de JavaScript. O viewport mobile
de 390 px não apresentou overflow horizontal. Capturas locais de validação:
`data/tmp/upcoming-desktop.png` e `data/tmp/upcoming-mobile.png`.
