import time
import zmq
import msgpack

# Serviço de referência: guarda o rank e a lista de servidores,
# recebe os heartbeats e informa a hora correta para os servidores.
# Só conversa com os servidores (porta 5559).

TIMEOUT = 30  # segundos sem heartbeat para o servidor ser removido da lista

ranks = {}       # nome -> rank (um nome sempre recebe o mesmo rank)
servidores = {}  # nome -> hora do último contato (só os que estão disponíveis)
proximo_rank = 1
relogio = 0      # relógio lógico do serviço


def remover_inativos():
    agora = time.time()
    for nome in list(servidores):
        if agora - servidores[nome] > TIMEOUT:
            del servidores[nome]
            print(f"[referencia] servidor {nome} removido (sem heartbeat)", flush=True)


def tratar_rank(msg):
    global proximo_rank
    nome = msg["user"]
    if nome not in ranks:
        ranks[nome] = proximo_rank
        proximo_rank += 1
    servidores[nome] = time.time()
    return {"status": "sucesso", "rank": ranks[nome]}


def tratar_lista(msg):
    lista = []
    for nome in servidores:
        lista.append({"name": nome, "rank": ranks[nome]})
    return {"status": "sucesso", "list": lista}


def tratar_heartbeat(msg):
    nome = msg["user"]
    if nome not in ranks:
        return {"status": "erro", "description": "servidor sem rank"}
    servidores[nome] = time.time()
    return {"status": "OK"}


context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5559")

poller = zmq.Poller()
poller.register(socket, zmq.POLLIN)

print("[referencia] serviço iniciado (porta 5559)", flush=True)

while True:
    # espera no máximo 1 segundo para poder remover servidores inativos
    eventos = dict(poller.poll(1000))
    remover_inativos()
    if socket not in eventos:
        continue

    msg = msgpack.unpackb(socket.recv())
    servico = msg["service"]
    conteudo = msg["data"]
    relogio = max(relogio, conteudo.get("clock", 0))
    print(f"[referencia] RECEBIDO | {servico} | {conteudo}", flush=True)

    if servico == "rank":
        resposta = tratar_rank(conteudo)
    elif servico == "list":
        resposta = tratar_lista(conteudo)
    elif servico == "heartbeat":
        resposta = tratar_heartbeat(conteudo)
    else:
        resposta = {"status": "erro", "description": "serviço desconhecido"}

    relogio += 1
    resposta["clock"] = relogio
    resposta["timestamp"] = time.time()
    print(f"[referencia] ENVIADO  | {servico} | {resposta}", flush=True)
    socket.send(msgpack.packb({"service": servico, "data": resposta}))
