const zmq = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");

const BROKER = process.env.BROKER_HOST || "broker";
const PROXY = process.env.PROXY_HOST || "proxy";
const nomeBase = process.env.BOT_NAME || "bot";
let nome = nomeBase;

let relogio = 0; // relógio lógico
const inscritos = new Set(); // canais em que o bot está inscrito

const mensagens = [
  "oi pessoal",
  "bom dia",
  "alguém aí?",
  "testando o canal",
  "mensagem aleatória",
  "olá mundo",
  "tudo bem?",
  "sistemas distribuídos é legal",
];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const aleatorio = (lista) => lista[Math.floor(Math.random() * lista.length)];
const hora = (t) => new Date(t * 1000).toISOString();

// envia uma requisição e espera a resposta
async function enviar(req, servico, dados) {
  relogio++; // incrementa o relógio lógico antes de enviar
  dados.timestamp = Date.now() / 1000;
  dados.clock = relogio;
  console.log(`[${nome}] ENVIADO  | ${servico} | ${JSON.stringify(dados)}`);
  await req.send(encode({ service: servico, data: dados }));

  const [bytes] = await req.receive();
  const resposta = decode(bytes);
  relogio = Math.max(relogio, resposta.data.clock || 0);
  console.log(`[${nome}] RECEBIDO | ${resposta.service} | ${JSON.stringify(resposta.data)}`);
  return resposta.data;
}

// recebe as publicações dos canais em que o bot está inscrito
async function receber(sub) {
  for await (const [topico, conteudo] of sub) {
    const canal = topico.toString();
    // o ZeroMQ filtra por prefixo, então confere se o canal é exatamente o inscrito
    if (!inscritos.has(canal)) continue;

    const msg = decode(conteudo);
    relogio = Math.max(relogio, msg.data.clock || 0);
    console.log(
      `[${nome}] MENSAGEM | canal: ${canal} | ${msg.data.user}: ${msg.data.message}` +
        ` | enviada: ${hora(msg.data.timestamp)} | recebida: ${hora(Date.now() / 1000)}` +
        ` | clock: ${msg.data.clock} -> ${relogio}`
    );
  }
}

async function main() {
  const req = new zmq.Request();
  req.connect("tcp://" + BROKER + ":5555");

  const sub = new zmq.Subscriber();
  sub.connect("tcp://" + PROXY + ":5558");
  receber(sub);

  // login: se der erro, tenta de novo com outro nome
  let tentativa = 0;
  while (true) {
    nome = tentativa === 0 ? nomeBase : `${nomeBase}_${tentativa}`;
    const r = await enviar(req, "login", { user: nome });
    if (r.status === "sucesso") break;
    tentativa++;
    await sleep(1000);
  }

  while (true) {
    let canais = (await enviar(req, "channels", {})).channels;

    // se existirem menos de 5 canais, cria um novo
    if (canais.length < 5) {
      const novo = "canal_" + Math.floor(Math.random() * 1000);
      await enviar(req, "channel", { channel: novo, user: nome });
      canais = (await enviar(req, "channels", {})).channels;
    }

    // se estiver inscrito em menos de 3 canais, se inscreve em mais um
    if (inscritos.size < 3) {
      const disponiveis = canais.filter((c) => !inscritos.has(c));
      if (disponiveis.length > 0) {
        const canal = aleatorio(disponiveis);
        sub.subscribe(canal);
        inscritos.add(canal);
        console.log(`[${nome}] INSCRITO | canal: ${canal}`);
      }
    }

    // escolhe um canal e envia 10 mensagens com intervalo de 1 segundo
    if (canais.length > 0) {
      const canal = aleatorio(canais);
      for (let i = 0; i < 10; i++) {
        await enviar(req, "publish", { user: nome, channel: canal, message: aleatorio(mensagens) });
        await sleep(1000);
      }
    } else {
      await sleep(1000);
    }
  }
}

main();
