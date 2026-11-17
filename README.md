# Projeto: Sistema para troca de mensagens instantâneas

Projeto da disciplina de Sistemas Distribuídos. A ideia é fazer uma versão simples de um sistema de mensagens parecido com o BBS/IRC, onde bots fazem login em servidores, criam canais e publicam mensagens nesses canais.

Esta é a **Parte 2**: publicação em canais usando Publisher-Subscriber (a Parte 1, com login e canais, continua funcionando).

## Como executar

```
docker compose up --build
```

Sobem 1 broker, 1 proxy, 2 servidores e 2 bots. Os bots não precisam de interação, tudo acontece sozinho e as mensagens trocadas aparecem no terminal.

## Escolhas feitas

### Linguagens
O projeto é individual, então usei 2 linguagens:
- **Python**: servidores, broker e proxy
- **JavaScript (Node.js)**: clientes (bots)

### Troca de mensagens
Usei o **ZeroMQ** com dois padrões:

- **Req-Rep** (login, canais e pedido de publicação): bots (REQ) -> broker (ROUTER 5555 / DEALER 5556) -> servidores (REP).
- **Pub-Sub** (publicações): o servidor (PUB) publica no proxy (XSUB, porta 5557) e os bots (SUB) se inscrevem no proxy (XPUB, porta 5558). O nome do canal é usado como tópico da mensagem.

O proxy do Pub-Sub é um container separado do broker.

### Serialização
As mensagens são serializadas com **MessagePack** (binário). Todas as mensagens têm este formato:

```
{ "service": "<nome do serviço>", "data": { ...campos..., "timestamp": <segundos> } }
```

| service    | requisição (data)                | resposta (data)                           |
|------------|----------------------------------|-------------------------------------------|
| `login`    | `user`                           | `status` (sucesso/erro), `description`    |
| `channel`  | `channel`, `user`                | `status` (sucesso/erro), `description`    |
| `channels` | (vazio)                          | `status`, `channels` (lista com os nomes) |
| `publish`  | `user`, `channel`, `message`     | `status` (sucesso/erro), `description`    |

A publicação enviada no tópico do canal (multipart: tópico + mensagem) tem `service = "publish"` e `data` com `user`, `channel`, `message` e `timestamp` (hora em que o servidor publicou). Todas as mensagens têm timestamp de envio.

### Erros tratados
- Login: nome vazio, nome com mais de 20 caracteres ou usuário já logado (o bot tenta de novo com outro nome).
- Criação de canal: nome vazio, caracteres inválidos ou canal que já existe.
- Publicação: canal que não existe no servidor ou mensagem vazia.

### Inscrição nos canais
A escolha dos canais é feita só no cliente. O bot usa um único socket SUB conectado ao proxy e pode se inscrever em vários tópicos. Como o ZeroMQ filtra tópicos por prefixo (`canal_1` também casaria com `canal_12`), o bot confere se o tópico recebido é exatamente um dos canais em que está inscrito. Quando chega uma mensagem, o bot mostra o canal, quem enviou, a mensagem, o timestamp de envio e o de recebimento.

### Funcionamento dos bots
Em loop infinito, o bot:
1. lista os canais; se existirem menos de 5, cria um novo
2. se estiver inscrito em menos de 3 canais, se inscreve em mais um (escolhido aleatoriamente)
3. escolhe um canal da lista e envia 10 mensagens aleatórias com intervalo de 1 segundo

Obs.: os passos 1 e 2 são verificados a cada volta do loop para que o sistema chegue em 5 canais e 3 inscrições mesmo com poucos bots.

### Persistência
Cada servidor salva seus dados em um arquivo JSON próprio (`dados/<nome do servidor>.json`), reescrito a cada alteração:
- `logins`: usuário e timestamp de cada login
- `canais`: nomes dos canais criados
- `mensagens`: cada publicação (usuário, canal, texto, timestamp da requisição e timestamp da publicação)

Os arquivos não são compartilhados entre os servidores. Como ainda não existe replicação, cada servidor só conhece os canais criados nele, então uma publicação em um canal que o outro servidor criou retorna erro (isso aparece no log dos bots).

## Estrutura

```
broker/        broker Req-Rep (Python)
proxy/         proxy Pub-Sub (Python)
servidor/      servidor (Python)
cliente/       bot (Node.js)
docker-compose.yaml
```
