# Projeto: Sistema para troca de mensagens instantâneas

Projeto da disciplina de Sistemas Distribuídos. A ideia é fazer uma versão simples de um sistema de mensagens parecido com o BBS/IRC, onde bots fazem login em servidores, criam canais e publicam mensagens nesses canais.

Esta é a **Parte 3**: relógio lógico, serviço de referência (rank, lista de servidores e heartbeat) e sincronização do relógio físico dos servidores. As Partes 1 e 2 continuam funcionando.

## Como executar

```
docker compose up --build
```

Sobem 1 broker, 1 proxy, 1 serviço de referência, 2 servidores e 2 bots. Os bots não precisam de interação, tudo acontece sozinho e as mensagens trocadas aparecem no terminal.

## Escolhas feitas

### Linguagens
O projeto é individual, então usei 2 linguagens:
- **Python**: servidores, broker, proxy e serviço de referência
- **JavaScript (Node.js)**: clientes (bots)

### Troca de mensagens
Usei o **ZeroMQ**:

- **Req-Rep** (login, canais, pedido de publicação): bots (REQ) -> broker (ROUTER 5555 / DEALER 5556) -> servidores (REP).
- **Pub-Sub** (publicações): servidor (PUB) -> proxy (XSUB 5557 / XPUB 5558) -> bots (SUB). O nome do canal é o tópico.
- **Req-Rep com a referência** (rank, lista, heartbeat): servidor (REQ) -> serviço de referência (REP, porta 5559). A referência só conversa com os servidores.

### Serialização
**MessagePack** (binário). Todas as mensagens têm o formato:

```
{ "service": "<nome do serviço>", "data": { ...campos..., "timestamp": <segundos>, "clock": <relógio lógico> } }
```

| service     | requisição (data)            | resposta (data)                           |
|-------------|------------------------------|-------------------------------------------|
| `login`     | `user`                       | `status`, `description`                   |
| `channel`   | `channel`, `user`            | `status`, `description`                   |
| `channels`  | (vazio)                      | `status`, `channels`                      |
| `publish`   | `user`, `channel`, `message` | `status`, `description`                   |
| `rank`      | `user` (nome do servidor)    | `status`, `rank`                          |
| `list`      | `user`                       | `status`, `list` (nome e rank)            |
| `heartbeat` | `user`                       | `status` (OK), `time` (hora da referência)|

Todas as mensagens (requisições, respostas e publicações) têm `timestamp` e `clock`.

### Relógio lógico
Está nos bots, nos servidores e na referência. Cada processo tem um contador que começa em 0:
1. antes de enviar qualquer mensagem o contador é incrementado e vai no campo `clock`;
2. ao receber uma mensagem (inclusive as publicações do Pub-Sub), o contador passa a ser o máximo entre o valor atual e o `clock` recebido.

### Serviço de referência
- **rank**: o primeiro servidor a pedir recebe o rank 1, o segundo o 2 e assim por diante. Um nome nunca é repetido: se o mesmo servidor pedir de novo, recebe o mesmo rank.
- **lista**: devolve nome e rank dos servidores disponíveis.
- **heartbeat**: a cada 10 mensagens de clientes recebidas, o servidor envia um heartbeat. A referência guarda a hora do último contato e, se um servidor ficar mais de 30 segundos sem enviar, ele é removido da lista. Quando o servidor volta a pedir rank, entra na lista de novo com o mesmo rank.
- **relógio físico**: a resposta do heartbeat traz a hora da referência. O servidor soma metade do tempo de ida e volta (algoritmo de Cristian) e guarda a diferença como ajuste, que é usada em todos os `timestamp` que ele envia depois disso. Logo após o heartbeat o servidor também pede a lista de servidores e mostra na tela.

### Erros tratados
- Login: nome vazio, maior que 20 caracteres ou usuário já logado (o bot tenta de novo com outro nome).
- Criação de canal: nome vazio, caracteres inválidos ou canal já existente.
- Publicação: nome de canal inválido ou mensagem vazia. Como os servidores ainda não replicam os canais entre si (cada um só conhece os que foram criados nele), o servidor não exige que o canal esteja na sua própria lista.

### Funcionamento dos bots
Em loop infinito: lista os canais (cria um se houver menos de 5), se inscreve em mais um canal se estiver inscrito em menos de 3, escolhe um canal e envia 10 mensagens aleatórias com 1 segundo de intervalo. As mensagens recebidas mostram canal, texto, timestamp de envio, timestamp de recebimento e o relógio lógico.

### Persistência
Cada servidor salva um arquivo JSON próprio (`dados/<nome do servidor>.json`) com `logins`, `canais` e `mensagens` (incluindo o `clock` de cada publicação). Os arquivos não são compartilhados entre servidores.

## Estrutura

```
broker/        broker Req-Rep (Python)
proxy/         proxy Pub-Sub (Python)
referencia/    serviço de referência (Python)
servidor/      servidor (Python)
cliente/       bot (Node.js)
docker-compose.yaml
```
