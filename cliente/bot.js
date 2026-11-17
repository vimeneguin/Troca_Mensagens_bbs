const zmq = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");

const BROKER = process.env.BROKER_HOST || "broker";
const nomeBase = process.env.BOT_NAME || "bot";
let nome = nomeBase;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// envia uma requisição e espera a resposta
async function enviar(req, servico, dados) {
  dados.timestamp = Date.now() / 1000;
  console.log(`[${nome}] ENVIADO  | ${servico} | ${JSON.stringify(dados)}`);
  await req.send(encode({ service: servico, data: dados }));

  const [bytes] = await req.receive();
  const resposta = decode(bytes);
  console.log(`[${nome}] RECEBIDO | ${resposta.service} | ${JSON.stringify(resposta.data)}`);
  return resposta.data;
}

async function main() {
  const req = new zmq.Request();
  req.connect("tcp://" + BROKER + ":5555");

  // login: se der erro, tenta de novo com outro nome
  let tentativa = 0;
  while (true) {
    nome = tentativa === 0 ? nomeBase : `${nomeBase}_${tentativa}`;
    const r = await enviar(req, "login", { user: nome });
    if (r.status === "sucesso") break;
    tentativa++;
    await sleep(1000);
  }

  // loop: lista os canais e cria um novo se tiver menos de 5
  while (true) {
    const lista = await enviar(req, "channels", {});
    if (lista.channels.length < 5) {
      const novo = "canal_" + Math.floor(Math.random() * 1000);
      await enviar(req, "channel", { channel: novo, user: nome });
    }
    await sleep(3000);
  }
}

main();
