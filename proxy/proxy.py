import zmq

# Proxy do Pub-Sub: os servidores publicam na porta 5557 (XSUB)
# e os clientes se inscrevem na porta 5558 (XPUB).
context = zmq.Context()

xsub = context.socket(zmq.XSUB)
xsub.bind("tcp://*:5557")

xpub = context.socket(zmq.XPUB)
xpub.bind("tcp://*:5558")

print("Proxy iniciado (XSUB: 5557 / XPUB: 5558)", flush=True)

zmq.proxy(xsub, xpub)
