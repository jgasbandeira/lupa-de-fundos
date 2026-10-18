// Coleta da Lupa de Fundos. Roda no GitHub Actions (Node 20+, sem dependências).
// Baixa da CVM o informe diário, o cadastro (antigo e RCVM 175) e o extrato de informações;
// CDI (Banco Central ou Ipeadata) e Ibovespa; e gera arquivos estáticos em site/dados/.
//
//   node scripts/coleta.mjs
//
// Variáveis de ambiente (opcionais):
//   INICIO=2000            primeiro ano do histórico
//   CACHE=cache            pasta com os arquivos já baixados (o workflow guarda entre execuções)
//   SAIDA=site/dados       pasta de saída
//   SITE_URL=https://...   site publicado; reserva se o CDI ou o Ibovespa falharem
//   CVM_BASE, BCB_BASE, YAHOO_BASE   trocar as fontes (testes)

import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import zlib from "node:zlib";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

const INICIO = +(process.env.INICIO || 2000);
const CACHE = process.env.CACHE || "cache";
const SAIDA = process.env.SAIDA || "site/dados";
const SITE_URL = (process.env.SITE_URL || "").replace(/\/$/, "");
const CVM = (process.env.CVM_BASE || "https://dados.cvm.gov.br/dados/FI").replace(/\/$/, "");
const BCB = (process.env.BCB_BASE || "https://api.bcb.gov.br").replace(/\/$/, "");
const YAHOO = (process.env.YAHOO_BASE || "https://query2.finance.yahoo.com").replace(/\/$/, "");
const DIAS_ATIVO = 45;
const UA = "LupaDeFundos/1.0 (comparador de fundos; dados publicos CVM/BCB)";

const t0 = Date.now();
const log = (...a) => console.log(`[${((Date.now() - t0) / 1000).toFixed(0).padStart(4)}s]`, ...a);
const avisos = [];
const aviso = (m) => { avisos.push(m); console.log("::warning::" + m); };

// ------------------------------------------------------------------ datas
// Dia = número de dias desde 1970-01-01 (inteiro).
const DIA_MS = 864e5;
const dia = (s) => Math.round(Date.parse(s.slice(0, 10) + "T00:00:00Z") / DIA_MS);
const iso = (d) => new Date(d * DIA_MS).toISOString().slice(0, 10);
const hoje = new Date();
const DIA_INI = dia(`${INICIO}-01-01`);
const DIA_HOJE = Math.floor(Date.now() / DIA_MS);
const dataOk = (s) => /^\d{4}-\d{2}-\d{2}/.test(s || "") ? s.slice(0, 10) : null;

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
    } catch (e) { ultimo = e; if (t < tentativas) await new Promise((s) => setTimeout(s, 4000 * t)); }
  }
  throw ultimo;
}

// ------------------------------------------------------------------ csv
// Lê CSVs (latin-1, ';') de arquivos soltos ou de dentro de zips (com o `unzip` do sistema).
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
async function* dicts(origem, membro) {
  let cab;
  for await (const r of registros(origem, membro)) {
    if (r.cab) { cab = r.cab; continue; }
    const o = {};
    for (let i = 0; i < cab.length; i++) o[cab[i]] = (r.c[i] || "").trim();
    yield o;
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
const cnpj14 = (s) => { const d = digitos(s); return d ? d.padStart(14, "0") : null; };
const desc = (s) => (s || "").replace(/\s+/g, " ").trim() || null;
const sn = (v) => { const x = (v || "").trim().toUpperCase(); return x === "S" || x === "SIM" ? true : x === "N" || x === "NAO" || x === "NÃO" ? false : null; };

// ------------------------------------------------------------------ informe diário
// Por fundo: dias, cotas, PL e cotistas.
const fundos = new Map(); // id -> {cnpj, sub, tipo, d:[], q:[], p:[], n:[], ult}
function fundo(id, cnpj, sub) {
  let f = fundos.get(id);
  if (!f) { f = { cnpj, sub, tipo: "", d: [], q: [], p: [], n: [], ult: -1 }; fundos.set(id, f); }
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
      if (!(d >= DIA_INI && d <= DIA_HOJE + 1)) continue; // descarta datas inválidas
      const cnpj = digitos(c[ix.c]).padStart(14, "0");
      const sub = ix.s >= 0 ? (c[ix.s] || "").trim() : "";
      const f = fundo(sub ? `${cnpj}.${sub}` : cnpj, cnpj, sub);
      f.d.push(d); f.q.push(q);
      f.p.push(ix.pl >= 0 ? num(c[ix.pl]) : NaN);
      f.n.push(ix.n >= 0 ? num(c[ix.n]) : NaN);
      if (ix.t >= 0 && c[ix.t]) f.tipo = c[ix.t].trim();
      if (d > f.ult) f.ult = d;
      n++;
    }
  }
  return n;
}

async function informes() {
  // Anos antigos: arquivos anuais (DADOS/HIST), que não mudam e ficam no cache.
  // Anos recentes: arquivos mensais (DADOS). Os dois meses mais recentes são sempre baixados de novo.
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
    if (ausentes[a]) continue;
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
      else if (a >= 2005 && !(a === anoAtual && faltam.length === 1)) aviso(`sem informe da CVM para ${faltam.join(", ")}`);
      continue;
    }
    const n = await carregarInforme(anual, faltam);
    log(`informe ${a} (anual, ${faltam.length} meses): ${n.toLocaleString("pt-BR")} cotas (${st})`);
  }
  fs.mkdirSync(path.dirname(arqAusentes), { recursive: true });
  fs.writeFileSync(arqAusentes, JSON.stringify(ausentes));
}

function ordenar(f) {
  const idx = f.d.map((_, i) => i).sort((a, b) => f.d[a] - f.d[b] || a - b);
  const d = [], q = [], p = [], n = [];
  for (const i of idx) {
    if (d.length && d[d.length - 1] === f.d[i]) { q[q.length - 1] = f.q[i]; p[p.length - 1] = f.p[i]; n[n.length - 1] = f.n[i]; continue; } // última revisão vence
    d.push(f.d[i]); q.push(f.q[i]); p.push(f.p[i]); n.push(f.n[i]);
  }
  f.d = d; f.q = q; f.p = p; f.n = n;
}

// ------------------------------------------------------------------ cadastro
// cad: CNPJ -> dados consolidados (cadastro antigo cad_fi.csv + registro RCVM 175)
const cad = new Map();
const subs = new Map(); // "cnpj.sub" -> dados da subclasse
const guardaChuva = new Set(); // CNPJs de fundos adaptados cujas classes têm outro CNPJ (não são produtos)

const TIPOS_ESTRUTURADOS = /^(FII|FIDC|FIP|FIAGRO|FICART|FUNCINE|FMIEE|FIC ?FIP|FIC ?FIDC|FII-FIAGRO|FIDC-FIAGRO|FIP-FIAGRO|FIIM|FMP-FGTS)/i;

function tipoCurto(t) {
  if (!t) return null;
  t = t.replace(/^Classes de Cotas de Fundos\s*/i, "").replace(/^Classe de\s*/i, "").trim();
  return t || null;
}
function situacaoPadrao(s) {
  if (!s) return null;
  const x = s.toLowerCase();
  if (x.startsWith("cancel")) return "Cancelada";
  if (x.includes("funcionamento normal")) return "Em funcionamento normal";
  if (x.includes("pré-operacional") || x.includes("pre-operacional")) return "Fase pré-operacional";
  if (x.includes("liquida")) return "Em liquidação";
  if (x.includes("incorpora")) return "Incorporação";
  if (x.includes("especial")) return "Em situação especial";
  if (x.includes("análise") || x.includes("analise")) return "Em análise";
  return s.charAt(0).toUpperCase() + s.slice(1).toLowerCase();
}
function publicoPadrao(s) {
  if (!s) return null;
  const x = s.toLowerCase();
  if (x.includes("profission")) return "Profissional";
  if (x.includes("qualific")) return "Qualificado";
  if (x.includes("geral") || x.includes("varejo")) return "Público geral";
  return desc(s);
}
const junta = (cnpj, obj, sobrescrever = true) => {
  const d = cad.get(cnpj) || {};
  for (const [k, v] of Object.entries(obj)) {
    if (v == null || v === "") continue;
    if (k === "inicio" && d.inicio && d.inicio <= v) continue; // fica a data mais antiga
    if (!sobrescrever && d[k] != null) continue;
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
      const c = cnpj14(o.CNPJ_FUNDO);
      if (!c || c === "00000000000000") continue;
      junta(c, {
        nome: desc(o.DENOM_SOCIAL), classe: desc(o.CLASSE), anbima: desc(o.CLASSE_ANBIMA), situacao: situacaoPadrao(o.SIT),
        gestor: desc(o.GESTOR), admin: desc(o.ADMIN), tipo: tipoCurto(o.TP_FUNDO), inicio: dataOk(o.DT_INI_ATIV) || dataOk(o.DT_CONST),
        cancelamento: dataOk(o.DT_CANCEL), condominio: desc(o.CONDOM), exclusivo: sn(o.FUNDO_EXCLUSIVO), publico: publicoPadrao(o.PUBLICO_ALVO),
        fic: sn(o.FUNDO_COTAS), taxa_adm_cad: isFinite(num(o.TAXA_ADM)) ? num(o.TAXA_ADM) : null,
        taxa_perf_cad: isFinite(num(o.TAXA_PERFM)) ? num(o.TAXA_PERFM) : null, perf_info: desc(o.INF_TAXA_PERFM),
        pl_cad: isFinite(num(o.VL_PATRIM_LIQ)) ? num(o.VL_PATRIM_LIQ) : null, data_pl_cad: dataOk(o.DT_PATRIM_LIQ),
        auditor: desc(o.AUDITOR), custodiante: desc(o.CUSTODIANTE), regime555: situacaoPadrao(o.SIT), registro555: dataOk(o.DT_REG),
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
    const fundosReg = new Map(), classes = new Map(), classesPorFundo = new Map();
    const mf = achar("registro_fundo"), mc = achar("registro_classe"), ms = achar("registro_subclasse");
    if (mf) for await (const o of dicts(zip, mf)) {
      fundosReg.set(o.ID_REGISTRO_FUNDO, o);
      const c = cnpj14(o.CNPJ_FUNDO);
      if (!c) continue;
      junta(c, {
        nome: desc(o.DENOMINACAO_SOCIAL), situacao: situacaoPadrao(o.SITUACAO), gestor: desc(o.GESTOR), admin: desc(o.ADMINISTRADOR),
        tipo: tipoCurto(o.TIPO_FUNDO), inicio: dataOk(o.DATA_CONSTITUICAO), cancelamento: dataOk(o.DATA_CANCELAMENTO),
        adaptacao175: dataOk(o.DATA_ADAPTACAO_RCVM175), pl_cad: isFinite(num(o.PATRIMONIO_LIQUIDO)) ? num(o.PATRIMONIO_LIQUIDO) : null,
        data_pl_cad: dataOk(o.DATA_PATRIMONIO_LIQUIDO),
      });
    }
    if (mc) for await (const o of dicts(zip, mc)) {
      const c = cnpj14(o.CNPJ_CLASSE);
      if (!c || c === "00000000000000") continue;
      classes.set(o.ID_REGISTRO_CLASSE, c);
      if (!classesPorFundo.has(o.ID_REGISTRO_FUNDO)) classesPorFundo.set(o.ID_REGISTRO_FUNDO, []);
      classesPorFundo.get(o.ID_REGISTRO_FUNDO).push(c);
      const pai = fundosReg.get(o.ID_REGISTRO_FUNDO) || {};
      junta(c, {
        nome: desc(o.DENOMINACAO_SOCIAL), classe: desc(o.CLASSIFICACAO), anbima: desc(o.CLASSIFICACAO_ANBIMA),
        situacao: situacaoPadrao(o.SITUACAO), tipo: tipoCurto(o.TIPO_CLASSE) || tipoCurto(pai.TIPO_FUNDO),
        inicio: dataOk(o.DATA_INICIO) || dataOk(o.DATA_CONSTITUICAO), condominio: desc(o.FORMA_CONDOMINIO),
        exclusivo: sn(o.EXCLUSIVO), publico: publicoPadrao(o.PUBLICO_ALVO), fic: o.CLASSE_COTAS ? sn(o.CLASSE_COTAS) : null,
        pl_cad: isFinite(num(o.PATRIMONIO_LIQUIDO)) ? num(o.PATRIMONIO_LIQUIDO) : null, data_pl_cad: dataOk(o.DATA_PATRIMONIO_LIQUIDO),
        gestor: desc(pai.GESTOR), admin: desc(pai.ADMINISTRADOR), auditor: desc(o.AUDITOR), custodiante: desc(o.CUSTODIANTE),
        adaptacao175: dataOk(pai.DATA_ADAPTACAO_RCVM175) || dataOk(o.DATA_REGISTRO), regime175: situacaoPadrao(o.SITUACAO),
        fundo_cnpj: cnpj14(pai.CNPJ_FUNDO), fundo_nome: desc(pai.DENOMINACAO_SOCIAL), indicador: desc(o.INDICADOR_DESEMPENHO),
      });
    }
    for (const [idf, lista] of classesPorFundo) {
      const f = fundosReg.get(idf);
      const c = f && cnpj14(f.CNPJ_FUNDO);
      if (c && !lista.includes(c)) guardaChuva.add(c);
    }
    if (ms) for await (const o of dicts(zip, ms)) {
      const c = classes.get(o.ID_REGISTRO_CLASSE);
      if (!c || !o.ID_SUBCLASSE) continue;
      subs.set(`${c}.${o.ID_SUBCLASSE}`, {
        nome: desc(o.DENOMINACAO_SOCIAL), situacao: situacaoPadrao(o.SITUACAO), inicio: dataOk(o.DATA_INICIO) || dataOk(o.DATA_CONSTITUICAO),
        condominio: desc(o.FORMA_CONDOMINIO), exclusivo: sn(o.EXCLUSIVO), publico: publicoPadrao(o.PUBLICO_ALVO), previdenciario: sn(o.PREVIDENCIARIO),
      });
    }
    log(`registro_fundo_classe: ${fundosReg.size} fundos, ${classes.size} classes, ${subs.size} subclasses, ${guardaChuva.size} fundos guarda-chuva`);
    if (!mf || !mc) aviso(`registro_fundo_classe.zip: arquivos encontrados ${membros.join(", ")}`);
  } catch (e) { aviso(`registro_fundo_classe.zip: ${e.message}`); }
}

// ------------------------------------------------------------------ extrato de informações
// Taxas, aplicação mínima, prazos e público. Fica a competência mais recente de cada CNPJ.
const extrato = new Map();
async function extratos() {
  const anoAtual = hoje.getUTCFullYear();
  const arquivos = [];
  for (let a = 2015; a <= anoAtual; a++) arquivos.push([`extrato_fi_${a}.csv`, a >= anoAtual - 1]);
  arquivos.push(["extrato_fi.csv", true]);
  let n = 0;
  for (const [nome, forcar] of arquivos) {
    const dest = path.join(CACHE, "extrato", nome);
    try {
      const st = await baixar(`${CVM}/DOC/EXTRATO/DADOS/${nome}`, dest, { forcar });
      if (st === "404") continue;
      for await (const o of dicts(dest)) {
        const c = cnpj14(o.CNPJ_FUNDO_CLASSE || o.CNPJ_FUNDO);
        if (!c) continue;
        const dt = dataOk(o.DT_COMPTC) || "";
        const ant = extrato.get(c);
        if (ant && ant.dt > dt) continue;
        const tx = num(o.TAXA_ADM), perf = num(o.TAXA_PERFM), min = num(o.APLIC_MIN);
        extrato.set(c, {
          dt, taxa_adm: isFinite(tx) ? tx : null, perf_existe: sn(o.EXISTE_TAXA_PERFM), taxa_perf: isFinite(perf) ? perf : null,
          perf_param: desc(o.PARAM_TAXA_PERFM), perf_ref: isFinite(num(o.PR_INDICE_REFER_TAXA_PERFM)) ? num(o.PR_INDICE_REFER_TAXA_PERFM) : null,
          aplic_min: isFinite(min) ? min : null, conversao: isFinite(num(o.QT_DIA_CONVERSAO_COTA)) ? num(o.QT_DIA_CONVERSAO_COTA) : null,
          pagamento: isFinite(num(o.QT_DIA_PAGTO_RESGATE)) ? num(o.QT_DIA_PAGTO_RESGATE) : null, tipo_dia: desc(o.TP_DIA_PAGTO_RESGATE),
          publico: publicoPadrao(o.PUBLICO_ALVO), condominio: desc(o.CONDOM), anbima: desc(o.CLASSE_ANBIMA),
          distribuicao: desc(o.DISTRIB), taxa_saida: sn(o.EXISTE_TAXA_SAIDA), custodia: isFinite(num(o.TAXA_CUSTODIA_MAX)) ? num(o.TAXA_CUSTODIA_MAX) : null,
        });
        n++;
      }
    } catch (e) { aviso(`${nome}: ${e.message}`); }
  }
  log(`extrato: ${n.toLocaleString("pt-BR")} registros, ${extrato.size.toLocaleString("pt-BR")} CNPJs`);
}

// ------------------------------------------------------------------ índices
async function anterior(nome) {
  if (!SITE_URL) return null;
  try { return await json(`${SITE_URL}/dados/${nome}`); } catch { return null; }
}
async function cdiBcb() {
  const pts = new Map();
  let ini = DIA_INI - 10;
  const fim = DIA_HOJE;
  const br = (d) => iso(d).split("-").reverse().join("/");
  while (ini <= fim) {
    const ate = Math.min(fim, ini + 364);
    const lista = await json(`${BCB}/dados/serie/bcdata.sgs.12/dados?formato=json&dataInicial=${br(ini)}&dataFinal=${br(ate)}`, 1);
    for (const x of lista) { const [d, m, a] = x.data.split("/"); pts.set(dia(`${a}-${m}-${d}`), +x.valor); }
    ini = ate + 1;
  }
  const ds = [...pts.keys()].sort((a, b) => a - b);
  if (ds.length < 20) throw new Error("série do CDI veio vazia");
  return { d: ds, v: ds.map((d) => pts.get(d)), fonte: "Banco Central (SGS 12)" };
}
// Reserva: Ipeadata. Selic over diária (% a.a.), que na prática acompanha o CDI, calibrada mês a mês pelo CDI mensal oficial.
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
    const dw = new Date(d * DIA_MS).getUTCDay();
    if (dw === 0 || dw === 6) continue;
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
      if (m >= mesAtual || !mensal.has(m)) continue;
      const alvo = 1 + mensal.get(m) / 100;
      const atual = lista.reduce((a, p) => a * (1 + p[1] / 100), 1);
      const ajuste = Math.pow(alvo / atual, 1 / lista.length);
      if (Math.abs(ajuste - 1) > 0.0005) continue;
      for (const p of lista) p[1] = ((1 + p[1] / 100) * ajuste - 1) * 100;
      calibrados++;
    }
  } catch (e) { aviso(`CDI mensal do Ipeadata indisponível (${e.message}); usando só a Selic over`); }
  log(`cdi (Ipeadata): ${pts.length} dias, ${calibrados} meses calibrados com o CDI oficial`);
  return { d: pts.map((p) => p[0]), v: pts.map((p) => p[1]), fonte: "Ipeadata: Selic over diária calibrada pelo CDI mensal" };
}
async function cdi() {
  try { return await cdiBcb(); }
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
  return { d: ds, v: ds.map((d) => +pts.get(d).toFixed(2)), fonte: "Yahoo Finance (^BVSP)" };
}

// ------------------------------------------------------------------ formatos de saída
function compacta(d, v, dig = 10) {
  const dd = new Array(d.length - 1);
  for (let i = 1; i < d.length; i++) dd[i - 1] = d[i] - d[i - 1];
  return { d0: d[0], dd, q: v.map((x) => +x.toPrecision(dig)) };
}
// Cota: l0 = ln(cota inicial)*1e7; dl = diferenças de ln(cota)*1e7 (inteiros). Precisão relativa de 1e-7, sem acúmulo de erro.
function serieLog(d, q) {
  const dd = new Array(d.length - 1), dl = new Array(d.length - 1);
  let ant = Math.round(Math.log(q[0]) * 1e7);
  const l0 = ant;
  for (let i = 1; i < d.length; i++) {
    const L = Math.round(Math.log(q[i]) * 1e7);
    dd[i - 1] = d[i] - d[i - 1]; dl[i - 1] = L - ant; ant = L;
  }
  return { d0: d[0], dd, l0, dl };
}
// PL (R$ mil) e cotistas: último valor de cada semana, em diferenças inteiras.
function serieSemanal(d, p, n) {
  const sd = [], sp = [], sn_ = [];
  let semana = null;
  for (let i = 0; i < d.length; i++) {
    const w = Math.floor((d[i] + 3) / 7);
    const pv = isFinite(p[i]) ? Math.round(p[i] / 1000) : null, nv = isFinite(n[i]) ? Math.round(n[i]) : null;
    if (w === semana) { sd[sd.length - 1] = d[i]; if (pv != null) sp[sp.length - 1] = pv; if (nv != null) sn_[sn_.length - 1] = nv; continue; }
    semana = w; sd.push(d[i]); sp.push(pv ?? (sp.length ? sp[sp.length - 1] : 0)); sn_.push(nv ?? (sn_.length ? sn_[sn_.length - 1] : 0));
  }
  const dd = [], dp = [], dn = [];
  for (let i = 1; i < sd.length; i++) { dd.push(sd[i] - sd[i - 1]); dp.push(sp[i] - sp[i - 1]); dn.push(sn_[i] - sn_[i - 1]); }
  return { d0: sd[0], dd, p0: sp[0], dp, n0: sn_[0], dn };
}

// Métricas de 12 meses para o ranking (janela termina na data de referência do site)
function metricas12(f, fatorCdi, refDia) {
  if (f.d.length < 30 || f.ult < refDia - 10) return null;
  let i0 = f.d.findIndex((x) => x >= refDia - 365);
  if (i0 < 0) return null;
  const d = f.d, q = f.q;
  const n = d.length - i0;
  if (n < 120 || d[i0] > refDia - 330) return null; // precisa cobrir quase 12 meses
  const ret = q[q.length - 1] / q[i0] - 1;
  let s = 0, s2 = 0;
  for (let i = i0 + 1; i < q.length; i++) { const r = q[i] / q[i - 1] - 1; s += r; s2 += r * r; }
  const m = s / (n - 1), vol = Math.sqrt(Math.max(s2 / (n - 1) - m * m, 0) * (n - 1) / Math.max(n - 2, 1)) * Math.sqrt(252);
  const anos = (n - 1) / 252, ann = ret > -1 ? Math.pow(1 + ret, 1 / anos) - 1 : null;
  const a = fatorCdi(d[i0]), b = fatorCdi(d[d.length - 1]);
  const cAnn = a && b ? Math.pow(b / a, 1 / anos) - 1 : null;
  const sharpe = ann != null && cAnn != null && vol > 1e-9 ? (ann - cAnn) / vol : null;
  const r4 = (x) => (x == null || !isFinite(x) ? null : Math.round(x * 1e4) / 1e4);
  return { ret: r4(ret), vol: r4(vol), sharpe: sharpe == null || !isFinite(sharpe) ? null : Math.round(sharpe * 100) / 100 };
}

// ------------------------------------------------------------------ principal
async function main() {
  fs.mkdirSync(CACHE, { recursive: true });
  log(`início: ${INICIO}`);
  await informes();
  for (const f of fundos.values()) ordenar(f);
  await cadastro();
  await extratos();

  // Subclasse nova sem histórico: encadeia com a série do CNPJ da classe antes da data de criação.
  for (const [, f] of fundos) {
    if (!f.sub || !f.d.length) continue;
    const pai = fundos.get(f.cnpj);
    if (!pai) continue;
    const primeiro = f.d[0];
    const corte = pai.d.findIndex((d) => d >= primeiro);
    const n = corte < 0 ? pai.d.length : corte;
    if (n < 2) continue;
    const k = pai.d[n] === primeiro ? n : n - 1;
    const fat = f.q[0] / pai.q[k];
    f.d = pai.d.slice(0, n).concat(f.d);
    f.q = pai.q.slice(0, n).map((x) => x * fat).concat(f.q);
    f.p = pai.p.slice(0, n).concat(f.p);
    f.n = pai.n.slice(0, n).concat(f.n);
    f.encadeado = iso(pai.d[0]);
  }

  // Índices (com reserva no site já publicado)
  const prev = await anterior("indices.json");
  const indices = { atualizado: new Date().toISOString() };
  for (const [nome, fn] of [["cdi", cdi], ["ibov", ibov]]) {
    try {
      const r = await fn();
      indices[nome] = compacta(r.d, r.v, 10);
      indices[nome].fonte = r.fonte;
      log(`${nome}: ${r.d.length} pontos`);
    } catch (e) {
      if (prev?.[nome]) { indices[nome] = prev[nome]; aviso(`${nome}: ${e.message}. Usando a série publicada anteriormente.`); }
      else aviso(`${nome}: ${e.message}. Sem série.`);
    }
  }
  // Fator acumulado do CDI para as métricas do ranking
  let fatorCdi = () => null;
  if (indices.cdi) {
    const ds = [indices.cdi.d0], fat = [1];
    let x = indices.cdi.d0, acc = 1;
    for (let i = 0; i < indices.cdi.dd.length; i++) { acc *= 1 + indices.cdi.q[i] / 100; x += indices.cdi.dd[i]; ds.push(x); fat.push(acc); }
    fatorCdi = (d) => { let lo = 0, hi = ds.length - 1; if (d < ds[0]) return null; while (lo < hi) { const m = (lo + hi + 1) >> 1; if (ds[m] <= d) lo = m; else hi = m - 1; } return fat[lo]; };
  }

  let refDia = 0;
  for (const f of fundos.values()) if (f.ult > refDia && f.d.length > 5) refDia = f.ult;

  fs.rmSync(SAIDA, { recursive: true, force: true });
  fs.mkdirSync(path.join(SAIDA, "f"), { recursive: true });

  const lista = [];
  let bytes = 0, pontos = 0, soCadastro = 0;
  const escrever = (id, obj) => {
    const gz = zlib.gzipSync(JSON.stringify(obj), { level: 9 });
    fs.writeFileSync(path.join(SAIDA, "f", `${id}.json.gz`), gz);
    bytes += gz.length;
  };
  const r2 = (x) => (x == null ? null : Math.round(x * 100) / 100);

  function montarInfo(id, cnpj, sub, f) {
    const c = cad.get(cnpj) || {};
    const s = sub ? subs.get(id) || {} : {};
    const e = extrato.get(cnpj) || {};
    const temSerie = !!(f && f.d.length >= 2);
    const tipo = c.tipo || (f && tipoCurto(f.tipo)) || null;
    const estruturado = !!(tipo && TIPOS_ESTRUTURADOS.test(tipo));
    const ativoInforme = temSerie && f.ult >= refDia - DIAS_ATIVO;
    let situacao = s.situacao || c.situacao || (temSerie ? "Sem cadastro (só no informe)" : null);
    const ativo = ativoInforme || (!temSerie && situacao === "Em funcionamento normal") ? 1 : 0;
    const nome = s.nome && c.nome && !s.nome.toUpperCase().includes(c.nome.toUpperCase().slice(0, 20)) ? `${c.nome} · ${s.nome}` : s.nome || c.nome || (sub ? `${cnpj} · subclasse ${sub}` : `Fundo ${cnpj}`);
    const publico = s.publico || c.publico || e.publico || null;
    const condominio = s.condominio || c.condominio || e.condominio || null;
    const exclusivo = s.exclusivo ?? c.exclusivo ?? null;
    let taxaAdm = e.taxa_adm ?? null, fonteTaxa = taxaAdm != null ? "extrato" : null;
    if (taxaAdm == null && c.taxa_adm_cad != null) { taxaAdm = c.taxa_adm_cad; fonteTaxa = "cadastro"; }
    let perf = null;
    if (e.perf_existe === false) perf = "não cobra";
    else if (e.taxa_perf != null) perf = `${String(r2(e.taxa_perf)).replace(".", ",")}%${e.perf_param ? " " + e.perf_param.toLowerCase() : ""}`;
    else if (c.taxa_perf_cad != null) perf = `${String(r2(c.taxa_perf_cad)).replace(".", ",")}%`;
    let pl = null, cot = null;
    if (temSerie) {
      for (let i = f.p.length - 1; i >= 0 && i >= f.p.length - 10; i--) if (isFinite(f.p[i])) { pl = f.p[i]; break; }
      for (let i = f.n.length - 1; i >= 0 && i >= f.n.length - 10; i--) if (isFinite(f.n[i])) { cot = f.n[i]; break; }
    }
    if (pl == null && c.pl_cad != null && (!temSerie || !ativoInforme)) pl = c.pl_cad;
    const univ = (publico === "Público geral" ? 1 : 0) | ((condominio || "").toLowerCase().startsWith("aberto") ? 2 : 0) | (exclusivo ? 4 : 0);
    return {
      id, cnpj, sub: sub || null, nome: desc(nome), tipo, estruturado, classe: c.classe || (estruturado ? tipo : null), anbima: c.anbima || e.anbima || null,
      situacao, ativo, gestor: c.gestor || null, admin: c.admin || null, inicio: s.inicio || c.inicio || null,
      condominio, exclusivo, publico, qualificado: publico ? publico !== "Público geral" : null, previdenciario: s.previdenciario ?? null,
      fic: c.fic ?? null, taxa_adm: taxaAdm, fonte_taxa: fonteTaxa, taxa_perf: perf, aplic_min: e.aplic_min ?? null,
      conversao: e.conversao ?? null, pagamento: e.pagamento ?? null, tipo_dia: e.tipo_dia || null, taxa_saida: e.taxa_saida ?? null,
      custodia: e.custodia ?? null, distribuicao: e.distribuicao || null, extrato_data: e.dt || null,
      auditor: c.auditor || null, custodiante: c.custodiante || null, cancelamento: c.cancelamento || null,
      adaptacao175: c.adaptacao175 || null, registro555: c.registro555 || null, regime555: c.regime555 || null, regime175: c.regime175 || null,
      fundo_cnpj: c.fundo_cnpj && c.fundo_cnpj !== cnpj ? c.fundo_cnpj : null, fundo_nome: c.fundo_cnpj && c.fundo_cnpj !== cnpj ? c.fundo_nome : null,
      indicador: c.indicador || null, pl: pl != null ? Math.round(pl) : null, data_pl: temSerie && pl != null ? iso(f.ult) : c.data_pl_cad || null,
      cotistas: cot, serie: temSerie ? "D" : estruturado ? "M" : "",
      primeira: temSerie ? iso(f.d[0]) : null, ultima: temSerie ? iso(f.ult) : null, pregoes: temSerie ? f.d.length : 0,
      encadeado: f?.encadeado || null, univ,
    };
  }

  // 1) Fundos com informe diário
  for (const [id, f] of fundos) {
    if (f.d.length < 2) continue;
    const info = montarInfo(id, f.cnpj, f.sub, f);
    const m = metricas12(f, fatorCdi, refDia);
    escrever(id, { ...info, serie: serieLog(f.d, f.q), semanal: serieSemanal(f.d, f.p, f.n) });
    pontos += f.d.length;
    lista.push([id, info.nome, info.classe, info.situacao, info.ativo, info.serie, m?.ret ?? null, m?.vol ?? null, m?.sharpe ?? null,
      info.taxa_adm, info.pl, info.gestor, info.admin, info.inicio, info.univ, info.cotistas, info.tipo, info.ultima]);
  }
  // 2) Fundos e classes só com cadastro (estruturados, pré-operacionais, cancelados sem informe)
  const cnpjsComSerie = new Set([...fundos.values()].filter((f) => f.d.length >= 2).map((f) => f.cnpj));
  for (const [cnpj] of cad) {
    if (cnpjsComSerie.has(cnpj) || guardaChuva.has(cnpj)) continue;
    const info = montarInfo(cnpj, cnpj, "", null);
    if (!info.nome || info.nome.startsWith("Fundo ")) continue;
    escrever(cnpj, info);
    soCadastro++;
    lista.push([cnpj, info.nome, info.classe, info.situacao, info.ativo, info.serie, null, null, null,
      info.taxa_adm, info.pl, info.gestor, info.admin, info.inicio, info.univ, info.cotistas, info.tipo, info.ultima]);
  }

  // Ativos primeiro, depois por PL
  lista.sort((a, b) => b[4] - a[4] || (b[10] || 0) - (a[10] || 0));
  const campos = ["id", "nome", "classe", "situacao", "ativo", "serie", "ret12", "vol12", "sharpe12", "taxa_adm", "pl", "gestor", "admin", "inicio", "univ", "cotistas", "tipo", "ultima"];
  const idx = zlib.gzipSync(JSON.stringify({ campos, ref: iso(refDia), fundos: lista }), { level: 9 });
  fs.writeFileSync(path.join(SAIDA, "fundos.json.gz"), idx);
  fs.writeFileSync(path.join(SAIDA, "indices.json"), JSON.stringify(indices));
  const comSerie = lista.filter((x) => x[5] === "D").length;
  const meta = {
    atualizado: new Date().toISOString(), ultima_cota: iso(refDia), fundos: lista.length, com_serie_diaria: comSerie,
    so_cadastro: soCadastro, fundos_ativos: lista.filter((x) => x[4]).length,
    inicio: iso([...fundos.values()].reduce((m, f) => (f.d.length ? Math.min(m, f.d[0]) : m), Infinity)), cotas: pontos,
    tamanho_mb: Math.round((bytes + idx.length) / 1e6), fontes: { cdi: indices.cdi?.fonte || null, ibov: indices.ibov?.fonte || null }, avisos,
  };
  fs.writeFileSync(path.join(SAIDA, "meta.json"), JSON.stringify(meta, null, 1));
  log(`saída: ${lista.length} fundos (${comSerie} com série diária, ${soCadastro} só cadastro, ${meta.fundos_ativos} ativos), ${pontos.toLocaleString("pt-BR")} cotas, ${meta.tamanho_mb} MB, índice ${(idx.length / 1e6).toFixed(1)} MB`);
  if (meta.tamanho_mb > 950) aviso(`site com ${meta.tamanho_mb} MB, perto do limite de 1 GB do GitHub Pages`);
  if (avisos.length) log(`${avisos.length} aviso(s)`);
}

main().catch((e) => { console.error("::error::" + e.message); console.error(e); process.exit(1); });
