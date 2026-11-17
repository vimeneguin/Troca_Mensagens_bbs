import os
import re
import json
import time
import zmq
import msgpack

# Configurações (os valores padrão servem para o docker compose)
nome = os.environ.get("SERVER_NAME", "servidor_1")
broker = os.environ.get("BROKER_HOST", "broker")
pasta_dados = os.environ.get("DADOS_DIR", "/dados")

os.makedirs(pasta_dados, exist_ok=True)
arquivo = os.path.join(pasta_dados, nome + ".json")


def carregar():
    # le os dados salvos em disco (se o arquivo existir)
    if os.path.exists(arquivo):
        with open(arquivo, "r") as f:
            return json.load(f)
    return {"logins": [], "canais": []}


def salvar():
    with open(arquivo, "w") as f:
        json.dump(dados, f, indent=2)


dados = carregar()
usuarios_online = set()  # usuários logados nesta execução do servidor


def tratar_login(msg):
    usuario = msg.get("user", "").strip()
    if usuario == "":
        return {"status": "erro", "description": "nome de usuário vazio"}
    if len(usuario) > 20:
        return {"status": "erro", "description": "nome de usuário muito grande"}
    if usuario in usuarios_online:
        return {"status": "erro", "description": "usuário já está logado"}

    usuarios_online.add(usuario)
    dados["logins"].append({"user": usuario, "timestamp": msg["timestamp"]})
    salvar()
    return {"status": "sucesso"}


def tratar_criar_canal(msg):
    canal = msg.get("channel", "").strip()
    if canal == "":
        return {"status": "erro", "description": "nome do canal vazio"}
    if not re.fullmatch(r"[A-Za-z0-9_-]+", canal):
        return {"status": "erro", "description": "nome do canal só pode ter letras, números, _ e -"}
    if canal in dados["canais"]:
        return {"status": "erro", "description": "canal já existe"}

    dados["canais"].append(canal)
    salvar()
    return {"status": "sucesso"}


def tratar_listar_canais(msg):
    return {"status": "sucesso", "channels": dados["canais"]}


context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://" + broker + ":5556")

print(f"[{nome}] servidor iniciado", flush=True)

while True:
    pacote = socket.recv()
    msg = msgpack.unpackb(pacote)
    servico = msg["service"]
    conteudo = msg["data"]
    print(f"[{nome}] RECEBIDO | {servico} | {conteudo}", flush=True)

    if servico == "login":
        resposta = tratar_login(conteudo)
    elif servico == "channel":
        resposta = tratar_criar_canal(conteudo)
    elif servico == "channels":
        resposta = tratar_listar_canais(conteudo)
    else:
        resposta = {"status": "erro", "description": "serviço desconhecido"}

    resposta["timestamp"] = time.time()
    print(f"[{nome}] ENVIADO   | {servico} | {resposta}", flush=True)
    socket.send(msgpack.packb({"service": servico, "data": resposta}))
