// Coleta da Lupa de Fundos. Roda no GitHub Actions (Node 20+, sem dependências).
// Baixa informe diário e cadastro da CVM, CDI (BCB) e Ibovespa, e gera arquivos estáticos em site/dados/.
//
//   node scripts/coleta.mjs
//
// Variáveis de ambiente (opcionais):
//   ANOS=5                 anos de histórico
//   CACHE=cache            pasta com os zips já baixados (o workflow guarda entre execuções)
//   SAIDA=site/dados       pasta de saída
//   SITE_URL=https://...   site publicado; usado como reserva se o CDI ou o Ibovespa falharem
//   CVM_BASE, BCB_BASE, YAHOO_BASE   trocar as fontes (testes)
//   PL_MIN=1000000         PL mínimo para o fundo entrar no site

import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

const ANOS = +(process.env.ANOS || 5);
const CACHE = process.env.CACHE || "cache";
const SAIDA = process.env.SAIDA || "site/dados";
const SITE_URL = (process.env.SITE_URL || "").replace(/\/$/, "");
const CVM = (process.env.CVM_BASE || "https://dados.cvm.gov.br/dados/FI").replace(/\/$/, "");
const BCB = (process.env.BCB_BASE || "https://api.bcb.gov.br").replace(/\/$/, "");
const YAHOO = (process.env.YAHOO_BASE || "https://query2.finance.yahoo.com").replace(/\/$/, "");
const PL_MIN = +(process.env.PL_MIN || 1e6);
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
const anoIni = hoje.getUTCFullYear() - ANOS;
const mesIni = hoje.getUTCMonth() + 1;
const DIA_INI = dia(`${anoIni}-${String(mesIni).padStart(2, "0")}-01`);

// ------------------------------------------------------------------ http
async function baixar(url, destino, { forcar = false } = {}) {
  if (!forcar && fs.existsSync(destino)) return "cache";
  fs.mkdirSync(path.dirname(destino), { recursive: true });
  for (let tent = 1; tent <= 3; tent++) {
    try {
      const r = await fetch(url, { headers: { "User-Agent": UA } });
      if (r.status === 404) return "404";
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const buf = Buffer.from(await r.arrayBuffer());
      fs.writeFileSync(destino + ".part", buf);
      fs.renameSync(destino + ".part", destino);
      return "ok";
    } catch (e) {
      if (tent === 3) throw new Error(`${url}: ${e.message}`);
      await new Promise((s) => setTimeout(s, 3000 * tent));
    }
  }
}

async function json(url) {
  const r = await fetch(url, { headers: { "User-Agent": UA, Accept: "application/json" } });
  if (!r.ok) throw new Error(`HTTP ${r.status} em ${url}`);
  return r.json();
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
      if (d < DIA_INI) continue;
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
  const meses = [];
  for (let a = anoIni, m = mesIni; a < hoje.getUTCFullYear() || (a === hoje.getUTCFullYear() && m <= hoje.getUTCMonth() + 1); m++) {
    if (m === 13) { a++; m = 1; if (a > hoje.getUTCFullYear()) break; }
    meses.push(`${a}${String(m).padStart(2, "0")}`);
  }
  // Os dois meses mais recentes são sempre baixados de novo: a CVM revisa e acrescenta dias.
  const recentes = new Set(meses.slice(-2));
  const hist = new Map(); // ano -> [meses que precisam do arquivo anual]
  for (const am of meses) {
    const url = `${CVM}/DOC/INF_DIARIO/DADOS/inf_diario_fi_${am}.zip`;
    const dest = path.join(CACHE, "inf", `inf_diario_fi_${am}.zip`);
    const st = await baixar(url, dest, { forcar: recentes.has(am) });
    if (st === "404") { const a = am.slice(0, 4); if (!hist.has(a)) hist.set(a, []); hist.get(a).push(`${a}-${am.slice(4)}`); continue; }
    const n = await carregarInforme(dest);
    log(`informe ${am}: ${n.toLocaleString("pt-BR")} cotas (${st})`);
  }
  for (const [a, ms] of hist) {
    const url = `${CVM}/DOC/INF_DIARIO/HIST/inf_diario_fi_${a}.zip`;
    const dest = path.join(CACHE, "inf", `inf_diario_fi_${a}.zip`);
    const st = await baixar(url, dest);
    if (st === "404") { aviso(`sem informe para ${ms.join(", ")}`); continue; }
    const n = await carregarInforme(dest, ms);
    log(`informe anual ${a} (${ms.length} meses): ${n.toLocaleString("pt-BR")} cotas (${st})`);
  }
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
  for (const [k, v] of Object.entries(obj)) if (v != null && v !== "") d[k] = v;
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
    const ate = Math.min(fim, ini + 3600);
    const lista = await json(`${BCB}/dados/serie/bcdata.sgs.12/dados?formato=json&dataInicial=${br(ini)}&dataFinal=${br(ate)}`);
    for (const x of lista) { const [d, m, a] = x.data.split("/"); pts.set(dia(`${a}-${m}-${d}`), +x.valor); }
    ini = ate + 1;
  }
  const ds = [...pts.keys()].sort((a, b) => a - b);
  if (ds.length < 20) throw new Error("série do CDI veio vazia");
  return { d: ds, v: ds.map((d) => pts.get(d)) };
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
  log(`início: ${iso(DIA_INI)} (${ANOS} anos)`);
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
  for (const [nome, fn] of [["cdi", cdi], ["ibov", ibov]]) {
    try { indices[nome] = compacta(...Object.values(await fn()), 10); log(`${nome}: ${indices[nome].q.length} pontos`); }
    catch (e) {
      if (prev?.[nome]) { indices[nome] = prev[nome]; aviso(`${nome}: ${e.message}. Usando a série publicada anteriormente.`); }
      else aviso(`${nome}: ${e.message}. Sem série.`);
    }
  }

  // Seleção de fundos ativos
  let ultimoDia = 0;
  for (const f of fundos.values()) if (f.ult > ultimoDia) ultimoDia = f.ult;
  const ativos = [...fundos.entries()].filter(([, f]) => f.ult >= ultimoDia - DIAS_ATIVO && f.d.length >= 2 && (f.pl >= PL_MIN || !isFinite(f.pl)));
  log(`fundos com cota: ${fundos.size.toLocaleString("pt-BR")}; ativos com PL >= ${PL_MIN}: ${ativos.length.toLocaleString("pt-BR")}`);
  if (ativos.length < 1) throw new Error("nenhum fundo ativo; abortando para não publicar site vazio");

  fs.rmSync(SAIDA, { recursive: true, force: true });
  fs.mkdirSync(path.join(SAIDA, "f"), { recursive: true });

  const lista = [];
  let bytes = 0;
  for (const [id, f] of ativos) {
    const c = cad.get(f.cnpj) || {};
    const nome = desc(subs.get(id)) || (f.sub ? `${desc(c.nome) || f.cnpj} · subclasse ${f.sub}` : desc(c.nome)) || `Fundo ${f.cnpj}`;
    const info = {
      id, cnpj: f.cnpj, sub: f.sub || null, nome, tipo: desc(f.tipo), classe: desc(c.classe), anbima: desc(c.anbima),
      gestor: desc(c.gestor), admin: desc(c.admin), taxa_adm: c.taxa_adm || null, taxa_perf: c.taxa_perf || null,
      publico: desc(c.publico), inicio: c.inicio || null, situacao: desc(c.situacao),
      pl: isFinite(f.pl) ? Math.round(f.pl) : null, cotistas: isFinite(f.cot) ? f.cot : null, ultima: iso(f.ult),
      encadeado: f.encadeado || null,
    };
    const txt = JSON.stringify({ ...info, serie: compacta(f.d, f.q) });
    fs.writeFileSync(path.join(SAIDA, "f", `${id}.json`), txt);
    bytes += txt.length;
    lista.push([id, nome, info.classe, info.gestor, info.pl, info.cotistas, f.sub ? 1 : 0]);
  }
  lista.sort((a, b) => (b[4] || 0) - (a[4] || 0));
  fs.writeFileSync(path.join(SAIDA, "fundos.json"), JSON.stringify({ campos: ["id", "nome", "classe", "gestor", "pl", "cotistas", "subclasse"], fundos: lista }));
  fs.writeFileSync(path.join(SAIDA, "indices.json"), JSON.stringify(indices));
  const meta = { atualizado: new Date().toISOString(), ultima_cota: iso(ultimoDia), fundos: lista.length, inicio: iso(DIA_INI), avisos };
  fs.writeFileSync(path.join(SAIDA, "meta.json"), JSON.stringify(meta, null, 1));
  log(`saída: ${lista.length} fundos, ${(bytes / 1e6).toFixed(0)} MB de séries, última cota ${meta.ultima_cota}`);
  if (avisos.length) log(`${avisos.length} aviso(s)`);
}

main().catch((e) => { console.error("::error::" + e.message); console.error(e); process.exit(1); });
