# Projeto: Sistema para troca de mensagens instantâneas

Projeto da disciplina de Sistemas Distribuídos. A ideia é fazer uma versão simples de um sistema de mensagens parecido com o BBS/IRC, onde bots fazem login em servidores, criam canais e publicam mensagens nesses canais.

Esta é a versão final (**Parte 5**): replicação dos dados entre os servidores. Ela inclui tudo o que foi feito nas partes anteriores: login e canais, publicação com Pub/Sub, relógio lógico, serviço de referência, eleição de coordenador e sincronização de relógio (Berkeley).

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
- **Pub-Sub entre servidores**: o aviso de novo coordenador é publicado no tópico `servers` e os dados novos são replicados no tópico `replication`.

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
| `history`   | `user`, `channel`            | `status`, `messages` (últimas 10 do canal)|
| `state` (entre servidores) | `user`        | `status`, `dados` (tudo o que o servidor guarda) |
| `servers` (tópico) | `coordinator`         | -                                         |
| `replication` (tópico) | `origin`, `type`, `item` | -                                    |

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

## Replicação dos dados (Parte 5)

### Problema
O broker divide as requisições entre os servidores (round-robin), então cada servidor só guarda uma parte dos logins, canais e mensagens. Se um servidor cair, essa parte do histórico some, e um cliente que pede o histórico a um servidor só recebe o que está nele. O objetivo é que **todos os servidores tenham todos os dados**.

### Método escolhido
Replicação por **propagação das atualizações para todas as réplicas** (todos os servidores são réplicas iguais, não existe servidor principal) com **consistência eventual**. Cada vez que um servidor recebe um dado novo de um cliente (login, canal novo ou mensagem publicada), ele:
1. guarda o dado e salva no disco;
2. publica o dado no tópico `replication` pelo proxy Pub-Sub que já existia;
3. todos os outros servidores assinam o tópico `replication`, guardam o dado e salvam no disco.

Como o dado vai para todos, depois de poucos milissegundos todos os servidores têm o mesmo conjunto de dados. Escolhi esse método porque o proxy Pub-Sub já existe no projeto, então não precisa de nenhuma conexão nova entre servidores, e porque o balanceamento do broker continua funcionando (qualquer servidor pode atender qualquer cliente).

### Como foi implementado
- Cada mensagem e cada login recebe um `id` único (`nome do servidor` + hora em nanossegundos). Quando um servidor recebe um dado replicado ele confere o `id` e ignora se já tinha, assim o mesmo dado nunca é guardado duas vezes. Canais são identificados pelo nome.
- O servidor ignora os dados replicados que ele mesmo publicou (campo `origin`).
- As mensagens replicadas também carregam o relógio lógico (`clock`), então a ordem das mensagens no histórico é a mesma em todos os servidores (ordenadas por `clock` e depois pelo timestamp).
- O arquivo de cada servidor continua separado (`dados/<servidor>.json`), mas agora com o mesmo conteúdo.
- O serviço `history` devolve as últimas 10 mensagens de um canal a partir dos dados do próprio servidor, que agora é o histórico completo. O bot pede o histórico quando se inscreve em um canal.

### Mudanças necessárias no método
- **Servidor novo ou que reiniciou:** o Pub-Sub não guarda o que foi publicado enquanto o servidor estava fora, então no início o servidor pede o serviço `state` a todos os outros servidores (os que estão na lista da referência) e junta os dados recebidos com os que já tinha em disco (usando os mesmos `id`s).
- **Mesmo canal criado em dois servidores ao mesmo tempo:** os dois aceitam a criação, mas como o canal é identificado pelo nome, depois da replicação existe uma cópia só em cada servidor.
- **Consistência eventual:** entre o servidor guardar um dado e os outros receberem existe um pequeno atraso. Por isso o servidor não exige que o canal exista na própria lista para publicar (basta o nome ser válido).
- **Falhas:** se a mensagem de replicação se perder (o Pub-Sub não garante entrega) o servidor só recupera o dado na próxima vez que reiniciar.

### Como conferir
Depois de rodar o projeto, os arquivos `dados/servidor_1.json` e `dados/servidor_2.json` têm os mesmos logins, canais e mensagens (os mesmos `id`s). No terminal, as linhas `REPLICANDO` (servidor que recebeu do cliente) e `REPLICADO` (servidor que recebeu a réplica) mostram o processo.

### Erros tratados
- Login: nome vazio, maior que 20 caracteres ou usuário já logado (o bot tenta de novo com outro nome).
- Criação de canal: nome vazio, caracteres inválidos ou canal já existente.
- Publicação: nome de canal inválido ou mensagem vazia.

### Funcionamento dos bots
Em loop infinito: lista os canais (cria um se houver menos de 5), se inscreve em mais um canal se estiver inscrito em menos de 3 (e pede o histórico do canal), escolhe um canal e envia 10 mensagens aleatórias com 1 segundo de intervalo.

### Persistência
Cada servidor salva um arquivo JSON próprio (`dados/<nome do servidor>.json`) com `logins`, `canais` e `mensagens`. Com a replicação, os dois arquivos têm o mesmo conteúdo.

## Estrutura

```
broker/        broker Req-Rep (Python)
proxy/         proxy Pub-Sub (Python)
referencia/    serviço de referência (Python)
servidor/      servidor (Python)
cliente/       bot (Node.js)
docker-compose.yaml
```
