import os
import re
import json
import time
import zmq
import msgpack

# Configurações (os valores padrão servem para o docker compose)
nome = os.environ.get("SERVER_NAME", "servidor_1")
broker = os.environ.get("BROKER_HOST", "broker")
proxy = os.environ.get("PROXY_HOST", "proxy")
referencia = os.environ.get("REFERENCE_HOST", "referencia")
pasta_dados = os.environ.get("DADOS_DIR", "/dados")

os.makedirs(pasta_dados, exist_ok=True)
arquivo = os.path.join(pasta_dados, nome + ".json")

relogio = 0                                              # relógio lógico
deslocamento = float(os.environ.get("CLOCK_OFFSET", 0))  # ajuste do relógio físico (para testar a sincronização)
rank = 0
mensagens_recebidas = 0
coordenador = None       # nome do servidor eleito como coordenador
precisa_eleicao = False  # fica True quando outro servidor pede uma eleição
diferencas = {}          # (só do coordenador) diferença de relógio de cada servidor


def agora():
    # hora física já com o ajuste
    return time.time() + deslocamento


def receber_clock(conteudo):
    # relógio lógico: ao receber, fica com o maior valor
    global relogio
    relogio = max(relogio, conteudo.get("clock", 0))


def preparar(conteudo):
    # relógio lógico: incrementa antes de enviar e coloca na mensagem
    global relogio
    relogio += 1
    conteudo["timestamp"] = agora()
    conteudo["clock"] = relogio
    return conteudo


def carregar():
    # le os dados salvos em disco (se o arquivo existir)
    if os.path.exists(arquivo):
        with open(arquivo, "r") as f:
            dados = json.load(f)
    else:
        dados = {}
    dados.setdefault("logins", [])
    dados.setdefault("canais", [])
    dados.setdefault("mensagens", [])
    return dados


def salvar():
    with open(arquivo, "w") as f:
        json.dump(dados, f, indent=2)


dados = carregar()
usuarios_online = set()  # usuários logados nesta execução do servidor

# ids de tudo o que já está guardado (para não guardar duas vezes o mesmo dado)
ids = set()
for lista in (dados["logins"], dados["mensagens"]):
    for item in lista:
        ids.add(item["id"])


def novo_id():
    return nome + "-" + str(time.time_ns())


def aplicar(tipo, item):
    # guarda o dado na memória. Devolve False se este servidor já tinha o dado.
    if tipo == "canal":
        if item in dados["canais"]:
            return False
        dados["canais"].append(item)
        return True

    if item["id"] in ids:
        return False
    ids.add(item["id"])
    if tipo == "login":
        dados["logins"].append(item)
    else:
        dados["mensagens"].append(item)
    return True


def registrar(tipo, item):
    # dado novo recebido de um cliente: guarda, salva em disco e replica nos outros servidores
    aplicar(tipo, item)
    salvar()
    conteudo = preparar({"origin": nome, "type": tipo, "item": item})
    pub.send_multipart([b"replication", msgpack.packb({"service": "replication", "data": conteudo})])
    print(f"[{nome}] REPLICANDO | {tipo} | {item}", flush=True)


def tratar_replicacao(msg):
    # dado novo vindo de outro servidor
    if msg["origin"] == nome:
        return
    if aplicar(msg["type"], msg["item"]):
        salvar()
        print(f"[{nome}] REPLICADO | de {msg['origin']} | {msg['type']} | {msg['item']}", flush=True)


def copiar_dados_dos_outros():
    # quando o servidor inicia pega os dados que já existem nos outros servidores
    lista = pedir_referencia("list", {"user": nome})["list"]
    for servidor in lista:
        if servidor["name"] == nome:
            continue
        resposta = perguntar(servidor, "state", {"user": nome})
        if resposta is None:
            continue
        for canal in resposta["dados"]["canais"]:
            aplicar("canal", canal)
        for item in resposta["dados"]["logins"]:
            aplicar("login", item)
        for item in resposta["dados"]["mensagens"]:
            aplicar("mensagem", item)
    salvar()


def tratar_login(msg):
    usuario = msg.get("user", "").strip()
    if usuario == "":
        return {"status": "erro", "description": "nome de usuário vazio"}
    if len(usuario) > 20:
        return {"status": "erro", "description": "nome de usuário muito grande"}
    if usuario in usuarios_online:
        return {"status": "erro", "description": "usuário já está logado"}

    usuarios_online.add(usuario)
    registrar("login", {"id": novo_id(), "user": usuario, "timestamp": msg["timestamp"], "server": nome})
    return {"status": "sucesso"}


def tratar_criar_canal(msg):
    canal = msg.get("channel", "").strip()
    if canal == "":
        return {"status": "erro", "description": "nome do canal vazio"}
    if not re.fullmatch(r"[A-Za-z0-9_-]+", canal):
        return {"status": "erro", "description": "nome do canal só pode ter letras, números, _ e -"}
    if canal in dados["canais"]:
        return {"status": "erro", "description": "canal já existe"}

    registrar("canal", canal)
    return {"status": "sucesso"}


def tratar_listar_canais(msg):
    return {"status": "sucesso", "channels": dados["canais"]}


def tratar_publicar(msg):
    usuario = msg.get("user", "")
    canal = msg.get("channel", "")
    texto = msg.get("message", "")

    if not re.fullmatch(r"[A-Za-z0-9_-]+", canal):
        return {"status": "erro", "description": "nome de canal inválido"}
    if texto == "":
        return {"status": "erro", "description": "mensagem vazia"}

    # publica no canal usando o nome do canal como tópico
    conteudo = preparar({"user": usuario, "channel": canal, "message": texto})
    pub.send_multipart([canal.encode(), msgpack.packb({"service": "publish", "data": conteudo})])
    print(f"[{nome}] PUBLICADO | {canal} | {conteudo}", flush=True)

    # guarda a publicação em disco e replica nos outros servidores
    registrar("mensagem", {
        "id": novo_id(),
        "user": usuario,
        "channel": canal,
        "message": texto,
        "timestamp_requisicao": msg["timestamp"],
        "timestamp_publicacao": conteudo["timestamp"],
        "clock": conteudo["clock"],
    })
    return {"status": "sucesso"}


def tratar_historico(msg):
    # devolve as últimas 10 mensagens do canal (em ordem pelo relógio lógico)
    canal = msg.get("channel", "")
    lista = [m for m in dados["mensagens"] if m["channel"] == canal]
    lista.sort(key=lambda m: (m["clock"], m["timestamp_publicacao"]))
    return {"status": "sucesso", "messages": lista[-10:]}


def pedir_referencia(servico, conteudo):
    # envia uma requisição para o serviço de referência e devolve a resposta
    conteudo = preparar(conteudo)
    print(f"[{nome}] ENVIADO   | referencia/{servico} | {conteudo}", flush=True)
    ref.send(msgpack.packb({"service": servico, "data": conteudo}))
    resposta = msgpack.unpackb(ref.recv())["data"]
    receber_clock(resposta)
    print(f"[{nome}] RECEBIDO  | referencia/{servico} | {resposta}", flush=True)
    return resposta


def perguntar(servidor, servico, conteudo):
    # envia uma requisição para outro servidor (com tempo limite).
    # Devolve None se o servidor não responder.
    s = context.socket(zmq.REQ)
    s.setsockopt(zmq.LINGER, 0)
    s.setsockopt(zmq.RCVTIMEO, 2000)
    s.setsockopt(zmq.SNDTIMEO, 2000)
    conteudo = preparar(conteudo)
    print(f"[{nome}] ENVIADO   | {servidor['name']}/{servico} | {conteudo}", flush=True)
    try:
        s.connect("tcp://" + servidor["name"] + ":" + str(6000 + servidor["rank"]))
        s.send(msgpack.packb({"service": servico, "data": conteudo}))
        resposta = msgpack.unpackb(s.recv())["data"]
        receber_clock(resposta)
        resumo = {k: v for k, v in resposta.items() if k != "dados"}
        print(f"[{nome}] RECEBIDO  | {servidor['name']}/{servico} | {resumo}", flush=True)
        return resposta
    except zmq.ZMQError:
        print(f"[{nome}] SEM RESPOSTA | {servidor['name']}/{servico}", flush=True)
        return None
    finally:
        s.close()


def heartbeat():
    # só avisa a referência que o servidor continua funcionando
    pedir_referencia("heartbeat", {"user": nome})


def eleicao():
    # algoritmo do valentão: pede eleição aos servidores com rank maior.
    # Se nenhum responder, este servidor vira o coordenador.
    global coordenador
    print(f"[{nome}] ELEICAO   | iniciada", flush=True)
    lista = pedir_referencia("list", {"user": nome})["list"]

    alguem_respondeu = False
    for servidor in lista:
        if servidor["rank"] > rank:
            resposta = perguntar(servidor, "election", {"user": nome})
            if resposta is not None and resposta["status"] == "OK":
                alguem_respondeu = True

    if alguem_respondeu:
        # um servidor maior vai assumir e avisar no tópico "servers"
        coordenador = None
        return

    coordenador = nome
    diferencas.clear()
    conteudo = preparar({"coordinator": nome})
    pub.send_multipart([b"servers", msgpack.packb({"service": "servers", "data": conteudo})])
    print(f"[{nome}] ELEITO    | {nome} é o novo coordenador", flush=True)


def tratar_relogio(msg):
    # algoritmo de Berkeley (feito pelo coordenador): calcula a média das
    # diferenças dos relógios, ajusta o próprio relógio e devolve a hora correta
    global deslocamento
    servidor = msg["user"]
    diferencas[servidor] = msg["time"] - agora()
    media = sum(diferencas.values()) / (len(diferencas) + 1)

    deslocamento += media
    for s in diferencas:
        diferencas[s] -= media
    diferencas[servidor] = 0
    return {"status": "OK", "time": agora()}


def sincronizar_relogio():
    # pede a hora correta ao coordenador (se ele não responder, faz eleição)
    global deslocamento, coordenador
    if coordenador is None:
        eleicao()
    if coordenador is None or coordenador == nome:
        return

    lista = pedir_referencia("list", {"user": nome})["list"]
    servidor = None
    for s in lista:
        if s["name"] == coordenador:
            servidor = s

    resposta = None
    if servidor is not None:
        t0 = time.time()
        resposta = perguntar(servidor, "clock", {"user": nome, "time": agora()})
        t1 = time.time()

    if resposta is None or resposta["status"] != "OK":
        print(f"[{nome}] coordenador {coordenador} indisponível", flush=True)
        coordenador = None
        eleicao()
        return

    # hora do coordenador + metade do tempo de ida e volta
    hora_certa = resposta["time"] + (t1 - t0) / 2
    deslocamento += hora_certa - agora()
    print(f"[{nome}] RELOGIO   | sincronizado com {coordenador} (ajuste total: {deslocamento:.3f}s)", flush=True)


def tratar_servidor(msg):
    # requisições vindas de outros servidores
    global precisa_eleicao
    servico = msg["service"]
    receber_clock(msg["data"])
    print(f"[{nome}] RECEBIDO  | {servico} | {msg['data']}", flush=True)

    if servico == "election":
        precisa_eleicao = True  # depois de responder, este servidor também faz eleição
        resposta = {"status": "OK"}
    elif servico == "clock" and coordenador == nome:
        resposta = tratar_relogio(msg["data"])
    elif servico == "state":
        resposta = {"status": "OK", "dados": dados}
    else:
        resposta = {"status": "erro", "description": "serviço desconhecido"}

    preparar(resposta)
    resumo = {k: v for k, v in resposta.items() if k != "dados"}
    print(f"[{nome}] ENVIADO   | {servico} | {resumo}", flush=True)
    return {"service": servico, "data": resposta}


context = zmq.Context()
rep = context.socket(zmq.REP)
rep.connect("tcp://" + broker + ":5556")

# socket para receber requisições dos outros servidores (porta 6000 + rank)
peer = context.socket(zmq.REP)

pub = context.socket(zmq.PUB)
pub.connect("tcp://" + proxy + ":5557")

sub = context.socket(zmq.SUB)
sub.connect("tcp://" + proxy + ":5558")
sub.setsockopt(zmq.SUBSCRIBE, b"servers")
sub.setsockopt(zmq.SUBSCRIBE, b"replication")
time.sleep(1)  # tempo para as conexões com o proxy ficarem prontas

ref = context.socket(zmq.REQ)
ref.connect("tcp://" + referencia + ":5559")

# pede o rank para o serviço de referência
rank = pedir_referencia("rank", {"user": nome})["rank"]
peer.bind("tcp://*:" + str(6000 + rank))
print(f"[{nome}] servidor iniciado com rank {rank}", flush=True)

copiar_dados_dos_outros()
eleicao()

poller = zmq.Poller()
poller.register(rep, zmq.POLLIN)
poller.register(peer, zmq.POLLIN)
poller.register(sub, zmq.POLLIN)

while True:
    eventos = dict(poller.poll())

    # requisições dos outros servidores (eleição e relógio)
    if peer in eventos:
        msg = msgpack.unpackb(peer.recv())
        peer.send(msgpack.packb(tratar_servidor(msg)))

    # avisos de novo coordenador (tópico servers) e dados replicados (tópico replication)
    if sub in eventos:
        topico, conteudo = sub.recv_multipart()
        msg = msgpack.unpackb(conteudo)
        receber_clock(msg["data"])
        if topico == b"servers":
            coordenador = msg["data"]["coordinator"]
            print(f"[{nome}] COORDENADOR | {coordenador}", flush=True)
        elif topico == b"replication":
            tratar_replicacao(msg["data"])

    # requisições dos clientes
    if rep in eventos:
        msg = msgpack.unpackb(rep.recv())
        servico = msg["service"]
        conteudo = msg["data"]
        receber_clock(conteudo)
        print(f"[{nome}] RECEBIDO  | {servico} | {conteudo}", flush=True)

        if servico == "login":
            resposta = tratar_login(conteudo)
        elif servico == "channel":
            resposta = tratar_criar_canal(conteudo)
        elif servico == "channels":
            resposta = tratar_listar_canais(conteudo)
        elif servico == "publish":
            resposta = tratar_publicar(conteudo)
        elif servico == "history":
            resposta = tratar_historico(conteudo)
        else:
            resposta = {"status": "erro", "description": "serviço desconhecido"}

        preparar(resposta)
        print(f"[{nome}] ENVIADO   | {servico} | {resposta}", flush=True)
        rep.send(msgpack.packb({"service": servico, "data": resposta}))

        mensagens_recebidas += 1
        # a cada 10 mensagens de clientes manda o heartbeat para a referência
        if mensagens_recebidas % 10 == 0:
            heartbeat()
        # a cada 15 mensagens sincroniza o relógio com o coordenador
        if mensagens_recebidas % 15 == 0:
            sincronizar_relogio()

    if precisa_eleicao:
        precisa_eleicao = False
        eleicao()
