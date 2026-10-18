// Coleta da Lupa de Fundos. Roda no GitHub Actions (Node 20+, sem dependências).
// Baixa informe diário e cadastro da CVM, CDI (BCB) e Ibovespa, e gera arquivos estáticos em site/dados/.
//
//   node scripts/coleta.mjs
//
// Variáveis de ambiente (opcionais):
//   INICIO=2000            primeiro ano do histórico (a CVM tem informe diário desde 2005)
//   CACHE=cache            pasta com os zips já baixados (o workflow guarda entre execuções)
//   SAIDA=site/dados       pasta de saída
//   SITE_URL=https://...   site publicado; usado como reserva se o CDI ou o Ibovespa falharem
//   CVM_BASE, BCB_BASE, YAHOO_BASE   trocar as fontes (testes)
//   PL_MIN=0               PL mínimo (na última cota) para o fundo entrar no site

import { spawn } from "node:child_process";
import fs from "node:fs";
import zlib from "node:zlib";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import path from "node:path";
import readline from "node:readline";

const INICIO = +(process.env.INICIO || 2000);
const CACHE = process.env.CACHE || "cache";
const SAIDA = process.env.SAIDA || "site/dados";
const SITE_URL = (process.env.SITE_URL || "").replace(/\/$/, "");
const CVM = (process.env.CVM_BASE || "https://dados.cvm.gov.br/dados/FI").replace(/\/$/, "");
const BCB = (process.env.BCB_BASE || "https://api.bcb.gov.br").replace(/\/$/, "");
const YAHOO = (process.env.YAHOO_BASE || "https://query2.finance.yahoo.com").replace(/\/$/, "");
const PL_MIN = +(process.env.PL_MIN || 0);
const DIAS_ATIVO = 45;
const UA = "LupaDeFundos/1.0 (comparador de fundos; dados publicos CVM/BCB)";

const t0 = Date.now();
const log = (...a) => console.log(`[${((Date.now() - t0) / 1000).toFixed(0).padStart(4)}s]`, ...a);
const avisos = [];
const aviso = (m) => { avisos.push(m); console.log("::warning::" + m); };

// ------------------------------------------------------------------ datas
// Dia = número de dias desde 1970-01-01 (inteiro). Facilita guardar e comparar.
const DIA_MS = 864e5;
const dia = (iso) => Math.round(Date.parse(iso.slice(0, 10) + "T00:00:00Z") / DIA_MS);
const iso = (d) => new Date(d * DIA_MS).toISOString().slice(0, 10);
const hoje = new Date();
const DIA_INI = dia(`${INICIO}-01-01`);
const DIA_HOJE = Math.floor(Date.now() / DIA_MS);

// ------------------------------------------------------------------ http
async function baixar(url, destino, { forcar = false } = {}) {
  if (!forcar && fs.existsSync(destino)) return "cache";
  fs.mkdirSync(path.dirname(destino), { recursive: true });
  for (let tent = 1; tent <= 3; tent++) {
    try {
      const r = await fetch(url, { headers: { "User-Agent": UA } });
      if (r.status === 404) return "404";
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      await pipeline(Readable.fromWeb(r.body), fs.createWriteStream(destino + ".part"));
      fs.renameSync(destino + ".part", destino);
      return "ok";
    } catch (e) {
      if (tent === 3) throw new Error(`${url}: ${e.message}`);
      await new Promise((s) => setTimeout(s, 3000 * tent));
    }
  }
}

// Alguns servidores (o do Banco Central, por exemplo) recusam clientes que não parecem navegador.
const UA_NAVEGADOR = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36";
async function json(url, tentativas = 3) {
  let ultimo;
  for (let t = 1; t <= tentativas; t++) {
    try {
      const r = await fetch(url, { headers: { "User-Agent": UA_NAVEGADOR, Accept: "application/json, text/plain, */*", "Accept-Language": "pt-BR,pt;q=0.9" } });
      const txt = await r.text();
      if (!r.ok) throw new Error(`HTTP ${r.status}: ${txt.replace(/\s+/g, " ").slice(0, 160)}`);
      try { return JSON.parse(txt); } catch { throw new Error(`resposta não é JSON: ${txt.replace(/\s+/g, " ").slice(0, 160)}`); }
    } catch (e) { ultimo = e; await new Promise((s) => setTimeout(s, 4000 * t)); }
  }
  throw ultimo;
}

// ------------------------------------------------------------------ csv
// Lê CSVs (latin-1, ';') de dentro de um zip com o `unzip` do sistema, linha a linha.
function nomesNoZip(zip) {
  return new Promise((ok, erro) => {
    const p = spawn("unzip", ["-Z1", zip]);
    let s = "";
    p.stdout.on("data", (d) => (s += d));
    p.on("close", (c) => (c === 0 ? ok(s.split("\n").filter(Boolean)) : erro(new Error(`unzip -Z1 ${zip} (${c})`))));
  });
}

async function* linhasCsv(origem, membro) {
  const p = membro ? spawn("unzip", ["-p", origem, membro]) : null;
  const stream = p ? p.stdout : fs.createReadStream(origem);
  stream.setEncoding("latin1");
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  for await (const l of rl) yield l;
}

async function* registros(origem, membro) {
  let cab = null;
  for await (const l of linhasCsv(origem, membro)) {
    if (!l) continue;
    const c = l.split(";");
    if (!cab) { cab = c.map((h) => h.trim().replace(/^﻿/, "").toUpperCase()); yield { cab }; continue; }
    yield { c };
  }
}

const num = (s) => {
  if (s == null) return NaN;
  s = s.trim();
  if (!s) return NaN;
  if (s.includes(",") && !s.includes(".")) s = s.replace(",", ".");
  return +s;
};
const digitos = (s) => (s || "").replace(/\D/g, "");

// ------------------------------------------------------------------ informe diário
// Por fundo: arrays de dias e cotas, mais o último PL e cotistas.
const fundos = new Map(); // id -> {cnpj, sub, tipo, d:[], q:[], pl, cot, ult}

function fundo(id, cnpj, sub) {
  let f = fundos.get(id);
  if (!f) { f = { cnpj, sub, tipo: "", d: [], q: [], pl: NaN, cot: NaN, ult: -1 }; fundos.set(id, f); }
  return f;
}

async function carregarInforme(zip, filtroMes) {
  let n = 0;
  for (const membro of (await nomesNoZip(zip)).filter((x) => x.toLowerCase().endsWith(".csv"))) {
    let ix = null;
    for await (const r of registros(zip, membro)) {
      if (r.cab) {
        const h = r.cab, g = (...ns) => { for (const x of ns) { const i = h.indexOf(x); if (i >= 0) return i; } return -1; };
        ix = { c: g("CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO"), s: g("ID_SUBCLASSE"), t: g("TP_FUNDO_CLASSE", "TP_FUNDO"), d: g("DT_COMPTC"),
               q: g("VL_QUOTA"), pl: g("VL_PATRIM_LIQ"), n: g("NR_COTST") };
        if (ix.c < 0 || ix.d < 0 || ix.q < 0) { aviso(`${path.basename(zip)}/${membro}: colunas inesperadas: ${h.join(",")}`); break; }
        continue;
      }
      const c = r.c;
      const ds = c[ix.d];
      if (!ds || (filtroMes && !filtroMes.includes(ds.slice(0, 7)))) continue;
      const q = num(c[ix.q]);
      if (!(q > 0)) continue;
      const d = dia(ds);
      if (d < DIA_INI || d > DIA_HOJE + 1) continue; // descarta datas inválidas
      const cnpj = digitos(c[ix.c]).padStart(14, "0");
      const sub = ix.s >= 0 ? (c[ix.s] || "").trim() : "";
      const f = fundo(sub ? `${cnpj}.${sub}` : cnpj, cnpj, sub);
      f.d.push(d); f.q.push(q);
      if (ix.t >= 0 && c[ix.t]) f.tipo = c[ix.t].trim();
      if (d >= f.ult) { f.ult = d; f.pl = ix.pl >= 0 ? num(c[ix.pl]) : NaN; f.cot = ix.n >= 0 ? num(c[ix.n]) : NaN; }
      n++;
    }
  }
  return n;
}

async function informes() {
  // A CVM guarda os anos antigos em arquivos anuais (HIST) e os recentes em arquivos mensais (DADOS).
  // Arquivos anuais nunca mudam: ficam no cache para sempre. Os dois meses mais recentes são sempre
  // baixados de novo, porque a CVM revisa e acrescenta dias.
  const anoAtual = hoje.getUTCFullYear(), mesAtual = hoje.getUTCMonth() + 1;
  const ref = new Date(Date.UTC(anoAtual, mesAtual - 2, 1));
  const recentes = new Set([`${anoAtual}${String(mesAtual).padStart(2, "0")}`, `${ref.getUTCFullYear()}${String(ref.getUTCMonth() + 1).padStart(2, "0")}`]);
  const arqAusentes = path.join(CACHE, "hist", "ausentes-v2.json");
  let ausentes = {};
  try { ausentes = JSON.parse(fs.readFileSync(arqAusentes, "utf8")); } catch {}

  for (let a = INICIO; a <= anoAtual; a++) {
    const anual = path.join(CACHE, "hist", `inf_diario_fi_${a}.zip`);
    if (fs.existsSync(anual)) {
      const n = await carregarInforme(anual);
      log(`informe ${a} (anual, cache): ${n.toLocaleString("pt-BR")} cotas`);
      continue;
    }
    if (ausentes[a]) continue; // ano sem dados na CVM
    const faltam = [];
    for (let m = 1; m <= (a === anoAtual ? mesAtual : 12); m++) {
      const am = `${a}${String(m).padStart(2, "0")}`;
      const dest = path.join(CACHE, "mensal", `inf_diario_fi_${am}.zip`);
      const st = await baixar(`${CVM}/DOC/INF_DIARIO/DADOS/inf_diario_fi_${am}.zip`, dest, { forcar: recentes.has(am) });
      if (st === "404") { faltam.push(`${a}-${String(m).padStart(2, "0")}`); continue; }
      const n = await carregarInforme(dest);
      log(`informe ${am}: ${n.toLocaleString("pt-BR")} cotas (${st})`);
    }
    if (!faltam.length) continue;
    const st = await baixar(`${CVM}/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_${a}.zip`, anual);
    if (st === "404") {
      if (faltam.length === 12 && a < anoAtual - 1) ausentes[a] = true;
      else if (a >= 2005) aviso(`sem informe da CVM para ${faltam.join(", ")}`);
      continue;
    }
    const n = await carregarInforme(anual, faltam);
    log(`informe ${a} (anual, ${faltam.length} meses): ${n.toLocaleString("pt-BR")} cotas (${st})`);
  }
  fs.mkdirSync(path.dirname(arqAusentes), { recursive: true });
  fs.writeFileSync(arqAusentes, JSON.stringify(ausentes));
}

// ------------------------------------------------------------------ cadastro
const cad = new Map(); // cnpj -> {...}
const subs = new Map(); // cnpj.sub -> nome

function pct(v, suf = "") {
  const n = num(v);
  if (!isFinite(n)) return v && v.trim() ? v.trim() : null;
  return n.toFixed(2).replace(".", ",") + "%" + suf;
}

async function* dicts(origem, membro) {
  let cab;
  for await (const r of registros(origem, membro)) {
    if (r.cab) { cab = r.cab; continue; }
    const o = {};
    for (let i = 0; i < cab.length; i++) o[cab[i]] = (r.c[i] || "").trim();
    yield o;
  }
}
const pick = (o, ...ks) => { for (const k of ks) { const v = o[k]; if (v) return v; } return null; };
const junta = (cnpj, obj) => {
  const d = cad.get(cnpj) || {};
  for (const [k, v] of Object.entries(obj)) {
    if (v == null || v === "") continue;
    if (k === "inicio" && d.inicio && d.inicio <= v) continue; // fica a data mais antiga
    d[k] = v;
  }
  cad.set(cnpj, d);
};

async function cadastro() {
  const csv = path.join(CACHE, "cad", "cad_fi.csv");
  try {
    await baixar(`${CVM}/CAD/DADOS/cad_fi.csv`, csv, { forcar: true });
    let n = 0;
    for await (const o of dicts(csv)) {
      const c = digitos(o.CNPJ_FUNDO).padStart(14, "0");
      if (c === "00000000000000") continue;
      junta(c, {
        nome: o.DENOM_SOCIAL, classe: o.CLASSE, anbima: o.CLASSE_ANBIMA, situacao: o.SIT, gestor: o.GESTOR,
        admin: o.ADMIN, taxa_adm: pct(o.TAXA_ADM, " a.a."), taxa_perf: pct(o.TAXA_PERFM) || o.INF_TAXA_PERFM || null,
        publico: o.PUBLICO_ALVO, inicio: pick(o, "DT_INI_ATIV", "DT_CONST"),
      });
      n++;
    }
    log(`cad_fi.csv: ${n.toLocaleString("pt-BR")} registros`);
  } catch (e) { aviso(`cad_fi.csv: ${e.message}`); }

  const zip = path.join(CACHE, "cad", "registro_fundo_classe.zip");
  try {
    await baixar(`${CVM}/CAD/DADOS/registro_fundo_classe.zip`, zip, { forcar: true });
    const membros = await nomesNoZip(zip);
    const achar = (p) => membros.find((m) => m.toLowerCase().includes(p));
    const fundosReg = new Map(), classes = new Map();
    const mf = achar("registro_fundo"), mc = achar("registro_classe"), ms = achar("registro_subclasse");
    if (mf) for await (const o of dicts(zip, mf)) {
      fundosReg.set(o.ID_REGISTRO_FUNDO, o);
      const c = digitos(o.CNPJ_FUNDO).padStart(14, "0");
      junta(c, { nome: o.DENOMINACAO_SOCIAL, situacao: o.SITUACAO, gestor: o.GESTOR, admin: o.ADMINISTRADOR,
                 inicio: cad.get(c)?.inicio || pick(o, "DATA_INICIO", "DATA_CONSTITUICAO") });
    }
    if (mc) for await (const o of dicts(zip, mc)) {
      const c = digitos(o.CNPJ_CLASSE).padStart(14, "0");
      if (c === "00000000000000") continue;
      classes.set(o.ID_REGISTRO_CLASSE, c);
      const pai = fundosReg.get(o.ID_REGISTRO_FUNDO) || {};
      junta(c, { nome: o.DENOMINACAO_SOCIAL, classe: pick(o, "CLASSIFICACAO", "TIPO_CLASSE"), anbima: o.CLASSIFICACAO_ANBIMA,
                 situacao: o.SITUACAO, publico: o.PUBLICO_ALVO, inicio: pick(o, "DATA_INICIO", "DATA_CONSTITUICAO"),
                 gestor: pai.GESTOR, admin: pai.ADMINISTRADOR });
    }
    if (ms) for await (const o of dicts(zip, ms)) {
      const c = classes.get(o.ID_REGISTRO_CLASSE);
      if (c && o.ID_SUBCLASSE) subs.set(`${c}.${o.ID_SUBCLASSE}`, o.DENOMINACAO_SOCIAL);
    }
    log(`registro_fundo_classe: ${fundosReg.size} fundos, ${classes.size} classes, ${subs.size} subclasses`);
    if (!mf || !mc) aviso(`registro_fundo_classe.zip: arquivos encontrados ${membros.join(", ")}`);
  } catch (e) { aviso(`registro_fundo_classe.zip: ${e.message}`); }
}

// ------------------------------------------------------------------ índices
async function anterior(nome) {
  if (!SITE_URL) return null;
  try { return await json(`${SITE_URL}/dados/${nome}`); } catch { return null; }
}

async function cdi() {
  const pts = new Map();
  let ini = DIA_INI - 10;
  const fim = dia(hoje.toISOString());
  const br = (d) => iso(d).split("-").reverse().join("/");
  while (ini <= fim) {
    const ate = Math.min(fim, ini + 364); // consultas de até 1 ano
    const lista = await json(`${BCB}/dados/serie/bcdata.sgs.12/dados?formato=json&dataInicial=${br(ini)}&dataFinal=${br(ate)}`, 1);
    for (const x of lista) { const [d, m, a] = x.data.split("/"); pts.set(dia(`${a}-${m}-${d}`), +x.valor); }
    ini = ate + 1;
  }
  const ds = [...pts.keys()].sort((a, b) => a - b);
  if (ds.length < 20) throw new Error("série do CDI veio vazia");
  return { d: ds, v: ds.map((d) => pts.get(d)) };
}

// Reserva: Ipeadata. Selic over diária (% a.a., GM366_TJOVER366), que na prática acompanha o CDI,
// calibrada mês a mês para bater com o CDI mensal oficial (% a.m., BM12_TJCDI12).
async function ipea(codigo) {
  let ultimo;
  for (const base of ["https://www.ipeadata.gov.br", "http://www.ipeadata.gov.br"]) {
    try { return (await json(`${base}/api/odata4/ValoresSerie(SERCODIGO='${codigo}')`, 2)).value || []; }
    catch (e) { ultimo = e; }
  }
  throw ultimo;
}
async function cdiIpea() {
  const selic = await ipea("GM366_TJOVER366");
  const pts = [];
  for (const x of selic) {
    if (x.VALVALOR == null) continue;
    const d = dia(x.VALDATA.slice(0, 10));
    if (d < DIA_INI - 10) continue;
    const dt = new Date(d * DIA_MS).getUTCDay();
    if (dt === 0 || dt === 6) continue;
    pts.push([d, (Math.pow(1 + x.VALVALOR / 100, 1 / 252) - 1) * 100]);
  }
  pts.sort((a, b) => a[0] - b[0]);
  if (pts.length < 20) throw new Error("Selic do Ipeadata veio vazia");
  let calibrados = 0;
  try {
    const mensal = new Map((await ipea("BM12_TJCDI12")).filter((x) => x.VALVALOR != null).map((x) => [x.VALDATA.slice(0, 7), x.VALVALOR]));
    const mesAtual = iso(pts[pts.length - 1][0]).slice(0, 7);
    const porMes = new Map();
    for (const p of pts) { const m = iso(p[0]).slice(0, 7); if (!porMes.has(m)) porMes.set(m, []); porMes.get(m).push(p); }
    for (const [m, lista] of porMes) {
      if (m >= mesAtual || !mensal.has(m)) continue; // só meses fechados
      const alvo = 1 + mensal.get(m) / 100;
      const atual = lista.reduce((a, p) => a * (1 + p[1] / 100), 1);
      const ajuste = Math.pow(alvo / atual, 1 / lista.length);
      if (Math.abs(ajuste - 1) > 0.0005) continue; // diferença grande demais: dado suspeito, não mexe
      for (const p of lista) p[1] = ((1 + p[1] / 100) * ajuste - 1) * 100;
      calibrados++;
    }
  } catch (e) { aviso(`CDI mensal do Ipeadata indisponível (${e.message}); usando só a Selic over`); }
  log(`cdi (Ipeadata): ${pts.length} dias, ${calibrados} meses calibrados com o CDI oficial`);
  return { d: pts.map((p) => p[0]), v: pts.map((p) => p[1]), fonte: "Ipeadata: Selic over diária calibrada pelo CDI mensal" };
}

async function cdiComReserva() {
  try { const r = await cdi(); r.fonte = "Banco Central (SGS 12)"; return r; }
  catch (e) { log(`CDI no Banco Central falhou (${e.message.slice(0, 80)}); tentando Ipeadata`); return cdiIpea(); }
}

async function ibov() {
  const p1 = (DIA_INI - 10) * 86400, p2 = Math.floor(Date.now() / 1000);
  const j = await json(`${YAHOO}/v8/finance/chart/%5EBVSP?period1=${p1}&period2=${p2}&interval=1d`);
  const r = j.chart.result[0], pts = new Map();
  r.timestamp.forEach((ts, i) => {
    const v = r.indicators.quote[0].close[i];
    if (v != null) pts.set(Math.floor((ts - 3 * 3600) / 86400), v);
  });
  const ds = [...pts.keys()].sort((a, b) => a - b);
  if (ds.length < 20) throw new Error("série do Ibovespa veio vazia");
  return { d: ds, v: ds.map((d) => +pts.get(d).toFixed(2)) };
}

// ------------------------------------------------------------------ saída
// Série compacta: d0 = primeiro dia, dd = diferenças entre dias, q = valores com 10 dígitos significativos.
function compacta(d, v, dig = 10) {
  const dd = new Array(d.length - 1);
  for (let i = 1; i < d.length; i++) dd[i - 1] = d[i] - d[i - 1];
  return { d0: d[0], dd, q: v.map((x) => +x.toPrecision(dig)) };
}
// Série de fundo: l0 = ln(cota inicial) * 1e7; dl = diferenças de ln(cota) * 1e7 (inteiros). Precisão relativa de 1e-7, sem acúmulo de erro.
function serieLog(d, q) {
  const dd = new Array(d.length - 1), dl = new Array(d.length - 1);
  let ant = Math.round(Math.log(q[0]) * 1e7);
  for (let i = 1; i < d.length; i++) {
    const L = Math.round(Math.log(q[i]) * 1e7);
    dd[i - 1] = d[i] - d[i - 1]; dl[i - 1] = L - ant; ant = L;
  }
  return { d0: d[0], dd, l0: Math.round(Math.log(q[0]) * 1e7), dl };
}
const desc = (s) => (s || "").replace(/\s+/g, " ").trim() || null;

function ordenar(f) {
  const idx = f.d.map((_, i) => i).sort((a, b) => f.d[a] - f.d[b] || a - b);
  const d = [], q = [];
  for (const i of idx) {
    if (d.length && d[d.length - 1] === f.d[i]) { q[q.length - 1] = f.q[i]; continue; } // última revisão vence
    d.push(f.d[i]); q.push(f.q[i]);
  }
  f.d = d; f.q = q;
}

async function main() {
  fs.mkdirSync(CACHE, { recursive: true });
  log(`início: ${INICIO}`);
  await informes();
  for (const f of fundos.values()) ordenar(f);
  await cadastro();

  // Subclasse nova sem histórico: encadeia com a série do CNPJ da classe antes da data de criação.
  for (const [id, f] of fundos) {
    if (!f.sub || !f.d.length) continue;
    const pai = fundos.get(f.cnpj);
    if (!pai) continue;
    const primeiro = f.d[0];
    const corte = pai.d.findIndex((d) => d >= primeiro);
    const n = corte < 0 ? pai.d.length : corte; // dias da classe antes da subclasse existir
    if (n < 2) continue;
    const k = pai.d[n] === primeiro ? n : n - 1; // dia comum (ou o anterior mais próximo) para emendar
    const fat = f.q[0] / pai.q[k];
    f.d = pai.d.slice(0, n).concat(f.d);
    f.q = pai.q.slice(0, n).map((x) => x * fat).concat(f.q);
    f.encadeado = iso(pai.d[0]);
  }

  // Índices (com reserva no site já publicado)
  const prev = await anterior("indices.json");
  const indices = { atualizado: new Date().toISOString() };
  for (const [nome, fn] of [["cdi", cdiComReserva], ["ibov", ibov]]) {
    try {
      const r = await fn();
      indices[nome] = compacta(r.d, r.v, 10);
      indices[nome].fonte = r.fonte || (nome === "ibov" ? "Yahoo Finance (^BVSP)" : null);
      log(`${nome}: ${indices[nome].q.length} pontos`);
    }
    catch (e) {
      if (prev?.[nome]) { indices[nome] = prev[nome]; aviso(`${nome}: ${e.message}. Usando a série publicada anteriormente.`); }
      else aviso(`${nome}: ${e.message}. Sem série.`);
    }
  }

  // Todos os fundos com pelo menos duas cotas (ativos e encerrados)
  let ultimoDia = 0;
  for (const f of fundos.values()) if (f.ult > ultimoDia) ultimoDia = f.ult;
  const sel = [...fundos.entries()].filter(([, f]) => f.d.length >= 2 && (!(PL_MIN > 0) || f.pl >= PL_MIN));
  log(`fundos com cota: ${fundos.size.toLocaleString("pt-BR")}; publicados: ${sel.length.toLocaleString("pt-BR")}`);
  if (sel.length < 1) throw new Error("nenhum fundo; abortando para não publicar site vazio");

  fs.rmSync(SAIDA, { recursive: true, force: true });
  fs.mkdirSync(path.join(SAIDA, "f"), { recursive: true });

  const lista = [];
  let bytes = 0, pontos = 0;
  for (const [id, f] of sel) {
    const c = cad.get(f.cnpj) || {};
    const nome = desc(subs.get(id)) || (f.sub ? `${desc(c.nome) || f.cnpj} · subclasse ${f.sub}` : desc(c.nome)) || `Fundo ${f.cnpj}`;
    const ativo = f.ult >= ultimoDia - DIAS_ATIVO ? 1 : 0;
    const info = {
      id, cnpj: f.cnpj, sub: f.sub || null, nome, tipo: desc(f.tipo), classe: desc(c.classe), anbima: desc(c.anbima),
      gestor: desc(c.gestor), admin: desc(c.admin), taxa_adm: c.taxa_adm || null, taxa_perf: c.taxa_perf || null,
      publico: desc(c.publico), inicio: c.inicio || null, situacao: desc(c.situacao), ativo,
      pl: isFinite(f.pl) ? Math.round(f.pl) : null, cotistas: isFinite(f.cot) ? f.cot : null, ultima: iso(f.ult),
      primeira: iso(f.d[0]), encadeado: f.encadeado || null,
    };
    const gz = zlib.gzipSync(JSON.stringify({ ...info, serie: serieLog(f.d, f.q) }), { level: 9 });
    fs.writeFileSync(path.join(SAIDA, "f", `${id}.json.gz`), gz);
    bytes += gz.length; pontos += f.d.length;
    lista.push([id, nome, info.classe, info.gestor, info.pl, info.cotistas, ativo, info.primeira.slice(0, 4), info.ultima.slice(0, 7)]);
  }
  // Ativos primeiro, depois por PL
  lista.sort((a, b) => b[6] - a[6] || (b[4] || 0) - (a[4] || 0));
  const idx = zlib.gzipSync(JSON.stringify({ campos: ["id", "nome", "classe", "gestor", "pl", "cotistas", "ativo", "ano_inicio", "ultima"], fundos: lista }), { level: 9 });
  fs.writeFileSync(path.join(SAIDA, "fundos.json.gz"), idx);
  fs.writeFileSync(path.join(SAIDA, "indices.json"), JSON.stringify(indices));
  const meta = { atualizado: new Date().toISOString(), ultima_cota: iso(ultimoDia), fundos: lista.length,
    fundos_ativos: lista.filter((x) => x[6]).length, inicio: iso(sel.reduce((m, [, f]) => Math.min(m, f.d[0]), Infinity)), cotas: pontos,
    tamanho_mb: Math.round((bytes + idx.length) / 1e6),
    fontes: { cdi: indices.cdi?.fonte || null, ibov: indices.ibov?.fonte || null }, avisos };
  fs.writeFileSync(path.join(SAIDA, "meta.json"), JSON.stringify(meta, null, 1));
  log(`saída: ${lista.length} fundos (${meta.fundos_ativos} ativos), ${pontos.toLocaleString("pt-BR")} cotas, ${meta.tamanho_mb} MB, desde ${meta.inicio}, última cota ${meta.ultima_cota}`);
  if (meta.tamanho_mb > 950) aviso(`site com ${meta.tamanho_mb} MB, perto do limite de 1 GB do GitHub Pages`);
  if (avisos.length) log(`${avisos.length} aviso(s)`);
}

main().catch((e) => { console.error("::error::" + e.message); console.error(e); process.exit(1); });
