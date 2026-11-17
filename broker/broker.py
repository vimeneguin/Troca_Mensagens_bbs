import zmq

# Broker do Req-Rep: os clientes (bots) conectam na porta 5555
# e os servidores conectam na porta 5556.
context = zmq.Context()

frontend = context.socket(zmq.ROUTER)
frontend.bind("tcp://*:5555")

backend = context.socket(zmq.DEALER)
backend.bind("tcp://*:5556")

print("Broker iniciado (clientes: 5555 / servidores: 5556)", flush=True)

zmq.proxy(frontend, backend)
