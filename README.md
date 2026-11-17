# Projeto: Sistema para troca de mensagens instantâneas

Projeto da disciplina de Sistemas Distribuídos. A ideia é fazer uma versão simples de um sistema de mensagens parecido com o BBS/IRC, onde bots fazem login em servidores, criam canais e (nas próximas partes) publicam mensagens nesses canais.

Esta é a **Parte 1**: login de usuário, criação e listagem de canais.

## Como executar

```
docker compose up --build
```

Sobem 1 broker, 2 servidores e 2 bots. Os bots não precisam de interação, tudo acontece sozinho e as mensagens trocadas aparecem no terminal.

## Escolhas feitas

### Linguagens
O projeto é individual, então usei 2 linguagens:
- **Python**: servidores e broker
- **JavaScript (Node.js)**: clientes (bots)

### Troca de mensagens
Usei o **ZeroMQ** com o padrão Req-Rep. Os bots (REQ) conectam no broker (ROUTER, porta 5555), o broker repassa para os servidores (DEALER, porta 5556) e os servidores (REP) respondem. Com isso as requisições são divididas entre os 2 servidores.

### Serialização
As mensagens são serializadas com **MessagePack**, que é um formato binário e tem biblioteca para Python e JavaScript. Todas as mensagens têm este formato:

```
{ "service": "<nome do serviço>", "data": { ...campos..., "timestamp": <segundos> } }
```

Serviços:

| service    | requisição (data)       | resposta (data)                              |
|------------|-------------------------|----------------------------------------------|
| `login`    | `user`                  | `status` (sucesso/erro), `description`       |
| `channel`  | `channel`, `user`       | `status` (sucesso/erro), `description`       |
| `channels` | (vazio)                 | `status`, `channels` (lista com os nomes)    |

O `timestamp` (hora do envio, em segundos) vai em todas as mensagens, tanto nas requisições quanto nas respostas.

### Erros tratados
- Login: nome vazio, nome com mais de 20 caracteres ou usuário que já está logado. Quando dá erro o bot tenta de novo com um novo nome (`bot_1_1`, `bot_1_2`...).
- Criação de canal: nome vazio, nome com caracteres que não sejam letras, números, `_` e `-`, e canal que já existe.

### Critério do bot para criar canais
Em loop, o bot lista os canais e, se existirem menos de 5, cria um canal com nome aleatório (`canal_123`).

### Persistência
Cada servidor salva seus dados em um arquivo JSON próprio (`dados/<nome do servidor>.json`), reescrito a cada alteração. Os arquivos são separados para cada servidor e guardam:
- `logins`: usuário e timestamp de cada login
- `canais`: nomes dos canais criados

O JSON é usado só para guardar em disco; as mensagens enviadas pela rede são sempre binárias (MessagePack).

## Estrutura

```
broker/        broker Req-Rep (Python)
servidor/      servidor (Python)
cliente/       bot (Node.js)
docker-compose.yaml
```
