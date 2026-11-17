# Projeto: Sistema para troca de mensagens instantâneas

Projeto da disciplina de Sistemas Distribuídos. A ideia é fazer uma versão simples de um sistema de mensagens parecido com o BBS/IRC, onde bots fazem login em servidores, criam canais e publicam mensagens nesses canais.

Esta é a **Parte 4**: eleição de coordenador e sincronização do relógio com o algoritmo de Berkeley. As partes anteriores continuam funcionando.

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
- **Req-Rep com a referência** (rank, lista, heartbeat): servidor (REQ) -> serviço de referência (REP, porta 5559).
- **Req-Rep entre servidores** (eleição e relógio): cada servidor tem um socket REP na porta `6000 + rank`. O endereço de um servidor é o nome dele (que é o nome do serviço no compose) mais essa porta, e a lista de nomes e ranks vem da referência.
- **Pub-Sub entre servidores**: o aviso de novo coordenador é publicado no tópico `servers`.

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
| `heartbeat` | `user`                       | `status` (OK)                             |
| `election`  | `user`                       | `status` (OK)                             |
| `clock`     | `user`, `time` (hora do servidor) | `status`, `time` (hora correta)      |
| `servers` (tópico) | `coordinator`         | -                                         |

Todas as mensagens têm `timestamp` e `clock` (relógio lógico, como na Parte 3).

### Eleição (algoritmo do valentão)
O coordenador é o servidor com o **maior rank** que estiver funcionando.
1. Quando um servidor inicia, ou quando percebe que o coordenador não responde, ele pede a lista de servidores à referência e envia `election` para todos os servidores com rank maior que o dele.
2. Quem recebe responde `OK` e também faz a sua própria eleição.
3. Se nenhum servidor maior responder (tempo limite de 2 segundos), o servidor se declara coordenador e publica o nome dele no tópico `servers`.
4. Todos os servidores assinam o tópico `servers` e guardam o nome do coordenador na variável `coordenador`.

### Sincronização do relógio (algoritmo de Berkeley)
A cada 15 mensagens de clientes recebidas, o servidor envia `clock` ao coordenador com a hora que ele tem. O coordenador guarda a diferença entre o relógio de cada servidor e o dele, calcula a **média das diferenças** (incluindo ele mesmo), ajusta o próprio relógio pela média e responde a hora correta. O servidor acerta o relógio com a hora recebida mais metade do tempo de ida e volta.

Se o coordenador não responder, o servidor inicia uma nova eleição.

Para dar para ver a sincronização funcionando (todos os containers estão na mesma máquina e já têm a mesma hora), o compose define `CLOCK_OFFSET` com um erro inicial no relógio: +5s no `servidor_1` e -3s no `servidor_2`. Nos logs aparece o ajuste total, que converge para a média (1s).

Simplificação: no Berkeley original o coordenador pergunta a hora para todos os servidores. Aqui o coordenador usa as horas que os servidores enviam quando pedem a sincronização, para manter o padrão Req-Rep do enunciado (servidor pede, coordenador responde).

### Serviço de referência
Guarda rank e lista de servidores. O heartbeat (a cada 10 mensagens de clientes) agora só responde `OK`, sem a hora. Servidores que ficam 30 segundos sem heartbeat saem da lista.

### Erros tratados
- Login: nome vazio, maior que 20 caracteres ou usuário já logado (o bot tenta de novo com outro nome).
- Criação de canal: nome vazio, caracteres inválidos ou canal já existente.
- Publicação: nome de canal inválido ou mensagem vazia. Os servidores ainda não replicam os canais, então o servidor não exige que o canal esteja na própria lista.

### Funcionamento dos bots
Em loop infinito: lista os canais (cria um se houver menos de 5), se inscreve em mais um canal se estiver inscrito em menos de 3, escolhe um canal e envia 10 mensagens aleatórias com 1 segundo de intervalo.

### Persistência
Cada servidor salva um arquivo JSON próprio (`dados/<nome do servidor>.json`) com `logins`, `canais` e `mensagens`.

## Estrutura

```
broker/        broker Req-Rep (Python)
proxy/         proxy Pub-Sub (Python)
referencia/    serviço de referência (Python)
servidor/      servidor (Python)
cliente/       bot (Node.js)
docker-compose.yaml
```
