// Coleta do Comparador de Fundos. Roda no GitHub Actions (Node 20+, sem dependências).
// Baixa da CVM o informe diário, o cadastro (antigo e RCVM 175) e o extrato de informações;
// CDI (Banco Central ou Ipeadata), Ibovespa e demais benchmarks (B3, ANBIMA, Ipeadata, Yahoo); e gera arquivos estáticos em site/dados/.
//
//   node scripts/coleta.mjs
//
// Variáveis de ambiente (opcionais):
//   INICIO=2000            primeiro ano do histórico
//   CACHE=cache            pasta com os arquivos já baixados (o workflow guarda entre execuções)
//   SAIDA=site/dados       pasta de saída
//   SITE_URL=https://...   site publicado; reserva se o CDI ou o Ibovespa falharem
//   SITE_SENHA=...         senha de acesso: os arquivos de dados saem criptografados (AES-GCM, chave PBKDF2)
//   CVM_BASE, BCB_BASE, YAHOO_BASE, B3_BASE, ANBIMA_BASE, IPEA_BASE   trocar as fontes (testes)

import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import zlib from "node:zlib";
import crypto from "node:crypto";
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
const UA = "ComparadorDeFundos/1.0 (comparador de fundos; dados publicos CVM/BCB)";

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
  if (!f) { f = { cnpj, sub, tipo: "", d: [], q: [], p: [], n: [], fl: [], ult: -1 }; fundos.set(id, f); }
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
               q: g("VL_QUOTA"), pl: g("VL_PATRIM_LIQ"), n: g("NR_COTST"), cap: g("CAPTC_DIA"), res: g("RESG_DIA") };
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
      // Captação e resgate do dia (R$). Só guarda dias com movimento, para economizar memória.
      const cap = ix.cap >= 0 ? num(c[ix.cap]) : NaN, res = ix.res >= 0 ? num(c[ix.res]) : NaN;
      if ((cap > 0) || (res > 0)) f.fl.push(d, cap > 0 ? cap : 0, res > 0 ? res : 0);
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
  // Captação e resgate somados por mês (chave = ano*12 + mês-1). Revisão de um mesmo dia: a última vence.
  const porDia = new Map();
  for (let i = 0; i < f.fl.length; i += 3) porDia.set(f.fl[i], [f.fl[i + 1], f.fl[i + 2]]);
  f.fm = new Map();
  for (const [x, [c, r]] of porDia) {
    const dt = new Date(x * DIA_MS), k = dt.getUTCFullYear() * 12 + dt.getUTCMonth();
    const a = f.fm.get(k) || [0, 0]; a[0] += c; a[1] += r; f.fm.set(k, a);
  }
  f.fl = null;
}
const mesDe = (x) => { const dt = new Date(x * DIA_MS); return dt.getUTCFullYear() * 12 + dt.getUTCMonth(); };
const mesIso = (k) => `${Math.floor(k / 12)}-${String((k % 12) + 1).padStart(2, "0")}`;
// Fluxo mensal em R$ mil, meses consecutivos do primeiro com movimento até o último dia da série.
function serieFluxo(f) {
  if (!f.fm || !f.fm.size) return null;
  const ks = [...f.fm.keys()], k0 = Math.min(...ks), k1 = Math.max(mesDe(f.ult), ...ks);
  const c = [], r = [];
  for (let k = k0; k <= k1; k++) { const a = f.fm.get(k); c.push(a ? Math.round(a[0] / 1000) : 0); r.push(a ? Math.round(a[1] / 1000) : 0); }
  return { m0: mesIso(k0), c, r };
}
// Captação líquida (R$) dos 12 meses que terminam no mês da data de referência (o mês corrente entra parcial).
function captacao12(f, refDia) {
  if (!f.fm || f.ult < refDia - 10) return null;
  const k1 = mesDe(refDia);
  let s = 0;
  for (let k = k1 - 11; k <= k1; k++) { const a = f.fm.get(k); if (a) s += a[0] - a[1]; }
  return Math.round(s);
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


// ------------------------------------------------------------------ estruturados (informes mensais)
// mensal: CNPJ -> Map("AAAA-MM" -> {pl, cot, vpc, rent, dy, series:{nome: rent}})
const mensal = new Map();
const estrInfo = new Map(); // CNPJ -> {segmento, mandato, isin, gestao, gestor}
const FII_BASE = CVM.replace(/\/FI$/, "/FII"), FIDC_BASE = CVM.replace(/\/FI$/, "/FIDC"), FIAGRO_BASE = CVM.replace(/\/FI$/, "/FIAGRO");
function pontoMensal(cnpj, ym) {
  let m = mensal.get(cnpj);
  if (!m) { m = new Map(); mensal.set(cnpj, m); }
  let o = m.get(ym);
  if (!o) { o = {}; m.set(ym, o); }
  return o;
}
const nOk = (v) => { const x = num(v); return isFinite(x) ? x : null; };

// Documentos do fundo: os da classe, os da subclasse e os do fundo guarda-chuva (onde costuma ficar o regulamento após a RCVM 175).
function juntaDocs(cnpj, sub, fundoCnpj) {
  const l = [...(docs.get(cnpj) || []), ...(sub ? docs.get(`${cnpj}.${sub}`) || [] : []), ...(fundoCnpj && fundoCnpj !== cnpj ? docs.get(fundoCnpj) || [] : [])];
  if (!l.length) return null;
  const vistos = new Set();
  return l.filter((x) => !vistos.has(x.u) && vistos.add(x.u)).sort((a, b) => (b.d || "").localeCompare(a.d || ""));
}
// Documentos eventuais (regulamento, anexo, apêndice, fato relevante, relatório gerencial, assembleias...) com o link no site da CVM.
const TIPOS_DOC = [
  [/^REGUL FDO$/i, "Regulamento"], [/^SGF ANEXO$/i, "Anexo da classe"], [/^SGF APENDICE$/i, "Apêndice da subclasse"],
  [/^FATO RELEV$/i, "Fato relevante"], [/^RELAT GERENCIAL$/i, "Relatório gerencial"], [/^(DEMONST CONTAB|DF)$/i, "Demonstrações financeiras"],
  [/^(AGO|AGE|AGO\/AGE|EDITAL AGO|AG[OE] PROPOST ADM|AGO\/AGE PROPADM|PROPOSTA ADMINI)/i, "Assembleia"],
  [/^(PROSPEC\.DISTRIB|MIN PROSP DEF)$/i, "Prospecto"],
];
const docs = new Map(); // "cnpj" ou "cnpj.sub" -> [{t, d, u}]
async function documentos() {
  const anoAtual = hoje.getUTCFullYear();
  let n = 0;
  for (let a = 2005; a <= anoAtual; a++) {
    const dest = path.join(CACHE, a >= anoAtual - 1 ? "estr" : "hist", `eventual_fi_${a}.csv`);
    try {
      const st = await baixar(`${CVM}/DOC/EVENTUAL/DADOS/eventual_fi_${a}.csv`, dest, { forcar: a >= anoAtual - 1 });
      if (st === "404") continue;
    } catch (e) { aviso(`documentos ${a}: ${e.message}`); continue; }
    let h = null, iC, iS, fim;
    for await (const r of registros(dest)) {
      if (r.cab) { h = r.cab; iC = h.findIndex((x) => /^CNPJ_FUNDO/.test(x)); iS = h.indexOf("ID_SUBCLASSE"); fim = (nome) => h.length - h.indexOf(nome); continue; }
      const c = r.c;
      if (c.length < h.length) continue;
      const col = (nome) => c[c.length - fim(nome)]; // conta do fim: o nome do fundo pode ter ";" e deslocar as colunas do meio
      const tipo = (col("TP_DOC") || "").trim(), u = (col("LINK_ARQ") || "").trim();
      const rot = TIPOS_DOC.find(([re]) => re.test(tipo));
      if (!rot || !/^https?:\/\//.test(u)) continue;
      const cnpj = cnpj14(c[iC]);
      if (!cnpj) continue;
      const sub = iS >= 0 ? (col("ID_SUBCLASSE") || "").trim() : "";
      const k = sub ? `${cnpj}.${sub}` : cnpj, d = dataOk(col("DT_RECEB")) || dataOk(col("DT_COMPTC"));
      let l = docs.get(k);
      if (!l) { l = []; docs.set(k, l); }
      l.push({ t: rot[1], d, u: u.replace(/^http:\/\/sistemas\.cvm/, "https://sistemas.cvm") });
      n++;
    }
  }
  // Mantém, por fundo, o mais recente de cada tipo e até 3 fatos relevantes e 3 assembleias.
  for (const [k, l] of docs) {
    l.sort((a, b) => (b.d || "").localeCompare(a.d || ""));
    const cont = {}, out = [];
    for (const x of l) { const lim = x.t === "Fato relevante" || x.t === "Assembleia" ? 3 : 1; if ((cont[x.t] = (cont[x.t] || 0) + 1) <= lim) out.push(x); }
    docs.set(k, out);
  }
  log(`documentos eventuais: ${n.toLocaleString("pt-BR")} documentos de ${docs.size.toLocaleString("pt-BR")} fundos/classes`);
}

async function estruturados() {
  const anoAtual = hoje.getUTCFullYear(), mesAtual = hoje.getUTCMonth() + 1;
  // FII: arquivos anuais (o do ano corrente e o anterior são revisados)
  let nfii = 0;
  for (let a = 2016; a <= anoAtual; a++) {
    const dest = path.join(CACHE, "estr", `inf_mensal_fii_${a}.zip`);
    try {
      const st = await baixar(`${FII_BASE}/DOC/INF_MENSAL/DADOS/inf_mensal_fii_${a}.zip`, dest, { forcar: a >= anoAtual - 1 });
      if (st === "404") continue;
      const membros = await nomesNoZip(dest);
      const versao = new Map();
      const mc = membros.find((m) => /complemento/i.test(m)), mg = membros.find((m) => /geral/i.test(m));
      if (mc) for await (const o of dicts(dest, mc)) {
        const c = cnpj14(o.CNPJ_FUNDO_CLASSE || o.CNPJ_FUNDO), ym = (o.DATA_REFERENCIA || "").slice(0, 7);
        if (!c || !ym) continue;
        const v = +(o.VERSAO || 1), k = c + ym;
        if ((versao.get(k) || 0) > v) continue;
        versao.set(k, v);
        const p = pontoMensal(c, ym);
        p.pl = nOk(o.PATRIMONIO_LIQUIDO); p.cot = nOk(o.TOTAL_NUMERO_COTISTAS); p.vpc = nOk(o.VALOR_PATRIMONIAL_COTAS);
        p.rent = nOk(o.PERCENTUAL_RENTABILIDADE_EFETIVA_MES); p.rentp = nOk(o.PERCENTUAL_RENTABILIDADE_PATRIMONIAL_MES); p.dy = nOk(o.PERCENTUAL_DIVIDEND_YIELD_MES);
        nfii++;
      }
      if (mg) for await (const o of dicts(dest, mg)) {
        const c = cnpj14(o.CNPJ_FUNDO_CLASSE || o.CNPJ_FUNDO);
        if (!c) continue;
        estrInfo.set(c, { segmento: desc(o.SEGMENTO_ATUACAO), mandato: desc(o.MANDATO), isin: desc(o.CODIGO_ISIN), gestao: desc(o.TIPO_GESTAO),
          bolsa: sn(o.MERCADO_NEGOCIACAO_BOLSA), admin: desc(o.NOME_ADMINISTRADOR) });
      }
    } catch (e) { aviso(`FII ${a}: ${e.message}`); }
  }
  log(`FII: ${nfii.toLocaleString("pt-BR")} registros mensais`);

  // Fiagro: arquivos mensais desde 2025
  let nfg = 0;
  for (let a = 2025, m = 1; a < anoAtual || (a === anoAtual && m <= mesAtual); m++) {
    if (m === 13) { a++; m = 1; if (a > anoAtual) break; }
    const am = `${a}${String(m).padStart(2, "0")}`;
    const dest = path.join(CACHE, "estr", `inf_mensal_fiagro_${am}.zip`);
    try {
      const st = await baixar(`${FIAGRO_BASE}/DOC/INF_MENSAL/DADOS/inf_mensal_fiagro_${am}.zip`, dest, { forcar: a === anoAtual && m >= mesAtual - 2 });
      if (st === "404") continue;
      const membro = (await nomesNoZip(dest)).find((x) => /fiagro_\d{6}\.csv$/i.test(x));
      if (!membro) continue;
      for await (const o of dicts(dest, membro)) {
        const c = cnpj14(o.CNPJ_CLASSE), ym = (o.DATA_REFERENCIA || "").slice(0, 7);
        if (!c || !ym) continue;
        const p = pontoMensal(c, ym);
        p.pl = nOk(o.PATRIMONIO_LIQUIDO); p.cot = nOk(o.NUMERO_COTISTAS); p.vpc = nOk(o.VALOR_PATRIMONIAL_COTAS);
        p.rent = nOk(o.RENTABILIDADE_EFETIVA_MES); p.rentp = nOk(o.RENTABILIDADE_PATRIMONIAL_MES); p.dy = nOk(o.DIVIDEND_YIELD_MES);
        const g = desc(o.NOME_GESTOR);
        if (g) estrInfo.set(c, { ...(estrInfo.get(c) || {}), gestor: g, admin: desc(o.NOME_ADMINISTRADOR) });
        nfg++;
      }
    } catch (e) { aviso(`Fiagro ${am}: ${e.message}`); }
  }
  log(`Fiagro: ${nfg.toLocaleString("pt-BR")} registros mensais`);

  // FIDC: arquivos mensais (DADOS) e anuais (DADOS/HIST) desde 2021. Rentabilidade em % ao mês por série.
  let nfd = 0;
  async function lerFidc(dest, filtroMes) {
    const membros = await nomesNoZip(dest);
    const ok = (ds) => !filtroMes || filtroMes.includes(ds.slice(0, 7));
    for (const mb of membros.filter((x) => /tab_iv_/i.test(x))) for await (const o of dicts(dest, mb)) {
      const c = cnpj14(o.CNPJ_FUNDO_CLASSE || o.CNPJ_FUNDO), ds = o.DT_COMPTC || "";
      if (!c || !ok(ds)) continue;
      pontoMensal(c, ds.slice(0, 7)).pl = nOk(o.TAB_IV_A_VL_PL); nfd++;
    }
    for (const mb of membros.filter((x) => /tab_x_1_\d{4,6}\.csv$/i.test(x))) for await (const o of dicts(dest, mb)) {
      const c = cnpj14(o.CNPJ_FUNDO_CLASSE || o.CNPJ_FUNDO), ds = o.DT_COMPTC || "";
      if (!c || !ok(ds)) continue;
      const p = pontoMensal(c, ds.slice(0, 7)), n = nOk(o.TAB_X_NR_COTST);
      if (n != null) p.cot = (p.cot || 0) + n;
    }
    const tam = new Map();
    for (const mb of membros.filter((x) => /tab_x_2_\d{4,6}\.csv$/i.test(x))) for await (const o of dicts(dest, mb)) {
      const c = cnpj14(o.CNPJ_FUNDO_CLASSE || o.CNPJ_FUNDO), ds = o.DT_COMPTC || "";
      if (!c || !ok(ds)) continue;
      const q = nOk(o.TAB_X_QT_COTA), v = nOk(o.TAB_X_VL_COTA), serie = desc(o.TAB_X_CLASSE_SERIE) || "única";
      if (q != null && v != null) tam.set(c + ds.slice(0, 7) + "|" + serie, q * v);
    }
    for (const mb of membros.filter((x) => /tab_x_3_\d{4,6}\.csv$/i.test(x))) for await (const o of dicts(dest, mb)) {
      const c = cnpj14(o.CNPJ_FUNDO_CLASSE || o.CNPJ_FUNDO), ds = o.DT_COMPTC || "";
      if (!c || !ok(ds)) continue;
      const r = nOk(o.TAB_X_VL_RENTAB_MES), serie = desc(o.TAB_X_CLASSE_SERIE) || "única", ym = ds.slice(0, 7);
      if (r == null) continue;
      const p = pontoMensal(c, ym);
      p.series = p.series || {};
      p.series[serie] = r / 100;
      const t = tam.get(c + ym + "|" + serie) || 0;
      if (t > (p._tam || -1)) { p._tam = t; p.rent = r / 100; p.serie_ref = serie; }
    }
  }
  for (let a = 2021; a <= anoAtual; a++) {
    const anual = path.join(CACHE, "estr", `inf_mensal_fidc_${a}.zip`);
    const faltam = [];
    for (let m = 1; m <= (a === anoAtual ? mesAtual : 12); m++) {
      const am = `${a}${String(m).padStart(2, "0")}`;
      const dest = path.join(CACHE, "estr", `inf_mensal_fidc_${am}.zip`);
      try {
        const st = await baixar(`${FIDC_BASE}/DOC/INF_MENSAL/DADOS/inf_mensal_fidc_${am}.zip`, dest, { forcar: a === anoAtual && m >= mesAtual - 2 });
        if (st === "404") { faltam.push(`${a}-${String(m).padStart(2, "0")}`); continue; }
        await lerFidc(dest);
      } catch (e) { aviso(`FIDC ${am}: ${e.message}`); }
    }
    if (faltam.length && a < anoAtual) {
      try {
        const st = await baixar(`${FIDC_BASE}/DOC/INF_MENSAL/DADOS/HIST/inf_mensal_fidc_${a}.zip`, anual);
        if (st !== "404") await lerFidc(anual, faltam);
      } catch (e) { aviso(`FIDC ${a} (anual): ${e.message}`); }
    }
  }
  log(`FIDC: ${nfd.toLocaleString("pt-BR")} registros mensais; estruturados com dados mensais: ${mensal.size.toLocaleString("pt-BR")}`);
}

// Série mensal compacta para a ficha e métricas para o ranking
function serieMensal(cnpj) {
  const m = mensal.get(cnpj);
  if (!m || !m.size) return null;
  const meses = [...m.keys()].sort();
  const r6 = (x) => (x == null ? null : Math.round(x * 1e6) / 1e6);
  const out = { m: meses, pl: [], cot: [], vpc: [], rent: [], dy: [] };
  const nomesSeries = new Set();
  for (const ym of meses) { const p = m.get(ym); if (p.series) Object.keys(p.series).forEach((k) => nomesSeries.add(k)); }
  const series = [...nomesSeries].slice(0, 6);
  if (series.length > 1) out.series = series.map((nome) => ({ nome, rent: [] }));
  for (const ym of meses) {
    const p = m.get(ym);
    out.pl.push(p.pl != null ? Math.round(p.pl) : null); out.cot.push(p.cot ?? null); out.vpc.push(p.vpc != null ? +p.vpc.toPrecision(10) : null);
    out.rent.push(r6(p.rent)); out.dy.push(r6(p.dy));
    if (out.series) out.series.forEach((s) => s.rent.push(r6(p.series?.[s.nome])));
  }
  if (!out.dy.some((x) => x != null)) delete out.dy;
  if (!out.vpc.some((x) => x != null)) delete out.vpc;
  // ranking: retorno dos últimos 12 meses (precisa dos 12 meses), PL e cotistas mais recentes
  let ret12 = null;
  const ult = meses.length - 1;
  if (meses.length >= 12) {
    const ult12 = out.rent.slice(-12);
    if (ult12.every((x) => x != null)) ret12 = Math.round((ult12.reduce((a, r) => a * (1 + r), 1) - 1) * 1e4) / 1e4;
  }
  let pl = null, cot = null, dataPl = null;
  for (let i = ult; i >= 0 && i >= ult - 3; i--) if (out.pl[i] != null) { pl = out.pl[i]; dataPl = meses[i]; break; }
  for (let i = ult; i >= 0 && i >= ult - 3; i--) if (out.cot[i] != null) { cot = out.cot[i]; break; }
  return { serie: out, ret12, pl, cot, dataPl, ultimo: meses[ult] };
}

// ------------------------------------------------------------------ carteira (CDA)
// Últimas competências da composição da carteira de cada fundo. Grava site/dados/c/<cnpj>.txt
const CDA_MESES = +(process.env.CDA_MESES || 12), CDA_MAX_POS = 80;
async function carteiras() {
  const meses = [];
  const d = new Date(Date.UTC(hoje.getUTCFullYear(), hoje.getUTCMonth(), 1));
  for (let i = 0; i < CDA_MESES + 6; i++) { meses.push(`${d.getUTCFullYear()}${String(d.getUTCMonth() + 1).padStart(2, "0")}`); d.setUTCMonth(d.getUTCMonth() - 1); }
  const porFundo = new Map(); // cnpj -> [string JSON por competência]
  let usados = 0, posTotal = 0;
  for (const am of meses) {
    if (usados >= CDA_MESES) break;
    const dest = path.join(CACHE, "cda", `cda_fi_${am}.zip`);
    let st;
    try { st = await baixar(`${CVM}/DOC/CDA/DADOS/cda_fi_${am}.zip`, dest, { forcar: usados < 3 }); }
    catch (e) { aviso(`CDA ${am}: ${e.message}`); continue; }
    if (st === "404") continue;
    usados++;
    const membros = await nomesNoZip(dest);
    const pl = new Map(), pos = new Map(), conf = new Map();
    const add = (c, t) => { let a = pos.get(c); if (!a) { a = []; pos.set(c, a); } a.push(t); };
    for (const mb of membros) {
      const nome = mb.toLowerCase();
      if (nome.includes("_pl_")) { for await (const o of dicts(dest, mb)) { const c = cnpj14(o.CNPJ_FUNDO_CLASSE || o.CNPJ_FUNDO); const v = nOk(o.VL_PATRIM_LIQ); if (c && v != null) pl.set(c, v); } continue; }
      if (nome.includes("confid")) { for await (const o of dicts(dest, mb)) { const c = cnpj14(o.CNPJ_FUNDO_CLASSE || o.CNPJ_FUNDO); const v = nOk(o.VL_MERC_POS_FINAL); if (!c) continue; let a = conf.get(c); if (!a) { a = []; conf.set(c, a); } a.push([desc(o.TP_APLIC) || "—", v]); } continue; }
      const blc = (nome.match(/blc_(\d)/) || [])[1] || (nome.includes("fie") ? "fie" : null);
      if (!blc) continue;
      for await (const o of dicts(dest, mb)) {
        const c = cnpj14(o.CNPJ_FUNDO_CLASSE || o.CNPJ_FUNDO);
        const v = nOk(o.VL_MERC_POS_FINAL);
        if (!c || v == null || v === 0) continue;
        if (blc === "fie" && o.VL_PATRIM_LIQ && !pl.has(c)) { const x = nOk(o.VL_PATRIM_LIQ); if (x != null) pl.set(c, x); }
        let ativo, cod = desc(o.CD_ISIN) || desc(o.CD_ATIVO) || null, emissor = desc(o.EMISSOR) || null, cota = null;
        switch (blc) {
          case "1": ativo = desc(o.TP_TITPUB) || desc(o.TP_ATIVO); emissor = "TESOURO NACIONAL"; cod = desc(o.CD_ISIN) || desc(o.CD_SELIC); break;
          case "2": ativo = desc(o.NM_FUNDO_CLASSE_SUBCLASSE_COTA) || "Cotas de fundo"; cota = cnpj14(o.CNPJ_FUNDO_CLASSE_COTA); cod = cota; emissor = null; break;
          case "3": ativo = desc(o.DS_SWAP) || "Swap"; cod = desc(o.CD_SWAP); break;
          case "4": ativo = [desc(o.CD_ATIVO), desc(o.DS_ATIVO)].filter(Boolean).join(" · ") || desc(o.TP_ATIVO); break;
          case "5": ativo = desc(o.TP_ATIVO) || "Depósito"; break;
          case "6": ativo = desc(o.TP_ATIVO) || "Crédito privado"; break;
          case "7": ativo = desc(o.DS_ATIVO_EXTERIOR) || desc(o.EMISSOR) || desc(o.TP_ATIVO); cod = desc(o.CD_ATIVO_BV_MERC) || cod; break;
          case "8": ativo = desc(o.DS_ATIVO) || desc(o.TP_ATIVO); break;
          default: ativo = desc(o.DS_ATIVO) || desc(o.CD_ATIVO) || desc(o.TP_ATIVO);
        }
        add(c, [desc(o.TP_APLIC) || "—", ativo || "—", cod, emissor, dataOk(o.DT_VENC), nOk(o.QT_POS_FINAL), Math.round(v * 100) / 100, cota]);
      }
    }
    const comp = `${am.slice(0, 4)}-${am.slice(4)}`;
    for (const [c, lista] of pos) {
      lista.sort((a, b) => Math.abs(b[6]) - Math.abs(a[6]));
      const total = lista.reduce((s, x) => s + x[6], 0);
      const top = lista.slice(0, CDA_MAX_POS), resto = lista.slice(CDA_MAX_POS);
      const plc = pl.get(c) ?? null, cf = conf.get(c) || null;
      const obj = { c: comp, pl: plc, n: lista.length, total: Math.round(total), sigilo: !!cf, conf: cf, pos: top,
        outros: resto.length ? { n: resto.length, v: Math.round(resto.reduce((s, x) => s + x[6], 0)) } : null };
      let a = porFundo.get(c); if (!a) { a = []; porFundo.set(c, a); }
      a.push(JSON.stringify(obj));
      posTotal += top.length;
    }
    for (const [c, cf] of conf) if (!pos.has(c)) {
      let a = porFundo.get(c); if (!a) { a = []; porFundo.set(c, a); }
      a.push(JSON.stringify({ c: comp, pl: pl.get(c) ?? null, n: 0, total: 0, sigilo: true, conf: cf, pos: [], outros: null }));
    }
    log(`CDA ${am}: ${pos.size.toLocaleString("pt-BR")} fundos, ${st}`);
  }
  fs.mkdirSync(path.join(SAIDA, "c"), { recursive: true });
  let bytes = 0;
  for (const [c, lista] of porFundo) {
    const gz = gzTexto(`{"cnpj":"${c}","competencias":[${lista.join(",")}]}`);
    fs.writeFileSync(path.join(SAIDA, "c", `${c}.txt`), gz);
    bytes += gz.length;
  }
  log(`carteiras: ${porFundo.size.toLocaleString("pt-BR")} fundos, ${usados} competências, ${posTotal.toLocaleString("pt-BR")} posições, ${Math.round(bytes / 1e6)} MB`);
  return Math.round(bytes / 1e6);
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
  const bases = process.env.IPEA_BASE ? [process.env.IPEA_BASE] : ["https://www.ipeadata.gov.br", "http://www.ipeadata.gov.br"];
  for (const base of bases) {
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
async function yahoo(simbolo, fuso = -3) {
  const p1 = (DIA_INI - 10) * 86400, p2 = Math.floor(Date.now() / 1000);
  const j = await json(`${YAHOO}/v8/finance/chart/${encodeURIComponent(simbolo)}?period1=${p1}&period2=${p2}&interval=1d`);
  const r = j.chart.result[0], pts = new Map();
  r.timestamp.forEach((ts, i) => {
    const v = r.indicators.quote[0].close[i];
    if (v != null) pts.set(Math.floor((ts + fuso * 3600) / 86400), v);
  });
  const ds = [...pts.keys()].sort((a, b) => a - b);
  if (ds.length < 20) throw new Error(`série ${simbolo} do Yahoo veio vazia`);
  return { d: ds, v: ds.map((d) => pts.get(d)), fonte: `Yahoo Finance (${simbolo})` };
}

// B3: evolução diária do índice, um ano por chamada (tabela dia × mês). Anos fechados ficam em cache.
const B3 = (process.env.B3_BASE || "https://sistemaswebb3-listados.b3.com.br").replace(/\/$/, "");
async function b3(indice) {
  const pts = [];
  const anoHoje = hoje.getUTCFullYear();
  for (let ano = INICIO; ano <= anoHoje; ano++) {
    const arq = path.join(CACHE, "hist", `b3-${indice}-${ano}.json`);
    let lista = null;
    if (ano < anoHoje && fs.existsSync(arq)) lista = JSON.parse(fs.readFileSync(arq, "utf8"));
    else {
      const arg = Buffer.from(JSON.stringify({ index: indice, language: "pt-br", year: String(ano) })).toString("base64");
      const j = await json(`${B3}/indexStatisticsProxy/IndexCall/GetPortfolioDay/${arg}`, 2);
      lista = [];
      for (const r of j.results || []) for (let m = 1; m <= 12; m++) {
        const s = r[`rateValue${m}`];
        if (!s) continue;
        const v = +String(s).replace(/\./g, "").replace(",", ".");
        if (v > 0) lista.push([dia(`${ano}-${String(m).padStart(2, "0")}-${String(r.day).padStart(2, "0")}`), v]);
      }
      if (ano < anoHoje) { fs.mkdirSync(path.dirname(arq), { recursive: true }); fs.writeFileSync(arq, JSON.stringify(lista)); }
      await new Promise((s) => setTimeout(s, 150));
    }
    pts.push(...lista);
  }
  pts.sort((a, b) => a[0] - b[0]);
  if (pts.length < 20) throw new Error(`série ${indice} da B3 veio vazia`);
  return { d: pts.map((p) => p[0]), v: pts.map((p) => p[1]), fonte: `B3 (${indice})` };
}
async function ibov() {
  try { return await b3("IBOV"); }
  catch (e) { log(`Ibovespa na B3 falhou (${e.message.slice(0, 80)}); tentando Yahoo`); const r = await yahoo("^BVSP"); r.v = r.v.map((x) => +x.toFixed(2)); return r; }
}

// ANBIMA: planilha com o histórico completo de cada índice (xlsx). Coluna B = data (serial do Excel), C = número-índice.
const ANBIMA = (process.env.ANBIMA_BASE || "https://s3-data-prd-use1-precos.s3.us-east-1.amazonaws.com/arquivos/indices-historico").replace(/\/$/, "");
function lerMembro(zip, membro) {
  return new Promise((ok, erro) => {
    const p = spawn("unzip", ["-p", zip, membro]);
    const partes = [];
    p.stdout.on("data", (d) => partes.push(d));
    p.on("close", (c) => (c === 0 ? ok(Buffer.concat(partes).toString("utf8")) : erro(new Error(`unzip -p ${zip} ${membro} (${c})`))));
  });
}
async function anbima(codigo) {
  const arq = path.join(CACHE, "anbima", `${codigo}.xlsx`);
  const st = await baixar(`${ANBIMA}/${codigo}-HISTORICO.xls`, arq, { forcar: true });
  if (st === "404") throw new Error(`${codigo}: arquivo da ANBIMA não encontrado`);
  const xml = await lerMembro(arq, "xl/worksheets/sheet1.xml");
  const pts = new Map();
  for (const m of xml.matchAll(/<row\b[^>]*>(.*?)<\/row>/gs)) {
    let d = null, v = null;
    for (const c of m[1].matchAll(/<c r="([A-Z]+)\d+"[^>]*?(?:\/>|>(?:<v>([^<]*)<\/v>)?.*?<\/c>)/gs)) {
      if (c[1] === "B" && c[2] != null) d = Math.round(+c[2]) - 25569;
      if (c[1] === "C" && c[2] != null) v = +c[2];
    }
    if (d != null && v > 0 && d >= DIA_INI - 10 && d <= DIA_HOJE) pts.set(d, v);
  }
  const ds = [...pts.keys()].sort((a, b) => a - b);
  if (ds.length < 20) throw new Error(`${codigo}: planilha da ANBIMA sem dados`);
  return { d: ds, v: ds.map((x) => pts.get(x)), fonte: `ANBIMA (${codigo})` };
}

// Índices de preço mensais (Ipeadata) viram série diária: a variação do mês é distribuída nos dias úteis do mês.
function mensalParaDiario(mensal, calendario) {
  const porMes = new Map();
  for (const d of calendario) { const m = iso(d).slice(0, 7); if (!porMes.has(m)) porMes.set(m, []); porMes.get(m).push(d); }
  const meses = [...porMes.keys()].sort();
  const d = [], v = [];
  let nivel = 1000;
  for (let i = 1; i < meses.length; i++) {
    const r = mensal.get(meses[i]);
    if (r == null) { if (d.length) break; else continue; }
    if (!d.length) { const ant = porMes.get(meses[i - 1]); d.push(ant[ant.length - 1]); v.push(nivel); }
    const ds = porMes.get(meses[i]), n = ds.length;
    ds.forEach((x, k) => { d.push(x); v.push(nivel * Math.pow(1 + r / 100, (k + 1) / n)); });
    nivel *= 1 + r / 100;
  }
  return { d, v };
}
async function precoMensal(codigo, calendario) {
  const lista = await ipea(codigo);
  const mensal = new Map(lista.filter((x) => x.VALVALOR != null).map((x) => [x.VALDATA.slice(0, 7), +x.VALVALOR]));
  const r = mensalParaDiario(mensal, calendario);
  if (r.d.length < 20) throw new Error(`${codigo}: série mensal do Ipeadata veio vazia`);
  return { ...r, fonte: `Ipeadata (${codigo}), variação mensal distribuída nos dias úteis` };
}
async function dolar() {
  try {
    const pts = (await ipea("GM366_ERC366")).filter((x) => x.VALVALOR != null).map((x) => [dia(x.VALDATA.slice(0, 10)), +x.VALVALOR]).filter((p) => p[0] >= DIA_INI - 10).sort((a, b) => a[0] - b[0]);
    if (pts.length < 20) throw new Error("PTAX do Ipeadata veio vazia");
    return { d: pts.map((p) => p[0]), v: pts.map((p) => p[1]), fonte: "Ipeadata (GM366_ERC366): PTAX venda" };
  } catch (e) { log(`dólar no Ipeadata falhou (${e.message.slice(0, 80)}); tentando Yahoo`); return yahoo("BRL=X"); }
}
// S&P 500 em reais: índice (Yahoo) × dólar do mesmo dia (ou do último dia com cotação)
async function spxReais(usd) {
  if (!usd) throw new Error("sem série do dólar");
  const s = await yahoo("^GSPC", -5);
  const d = [], v = [];
  let j = 0;
  for (let i = 0; i < s.d.length; i++) {
    while (j + 1 < usd.d.length && usd.d[j + 1] <= s.d[i]) j++;
    if (usd.d[j] > s.d[i] || s.d[i] - usd.d[j] > 7) continue;
    d.push(s.d[i]); v.push(s.v[i] * usd.v[j]);
  }
  if (d.length < 20) throw new Error("S&P 500 em reais sem dados");
  return { d, v, fonte: "Yahoo Finance (^GSPC) × dólar" };
}

// Benchmarks além do CDI. tipo "taxa" = série suave (sem sentido calcular beta ou correlação diária).
const BENCHMARKS = [
  { id: "IPCA", nome: "IPCA", grupo: "Inflação", tipo: "taxa", fn: (cal) => precoMensal("PRECOS12_IPCAG12", cal) },
  { id: "IGPM", nome: "IGP-M", grupo: "Inflação", tipo: "taxa", fn: (cal) => precoMensal("IGP12_IGPMG12", cal) },
  { id: "IMAB", nome: "IMA-B", grupo: "Renda fixa", fn: () => anbima("IMAB") },
  { id: "IMAB5", nome: "IMA-B 5", grupo: "Renda fixa", fn: () => anbima("IMAB5") },
  { id: "IMAB5MAIS", nome: "IMA-B 5+", grupo: "Renda fixa", fn: () => anbima("IMAB5MAIS") },
  { id: "IRFM", nome: "IRF-M", grupo: "Renda fixa", fn: () => anbima("IRFM") },
  { id: "IMAS", nome: "IMA-S", grupo: "Renda fixa", fn: () => anbima("IMAS") },
  { id: "IMAGERAL", nome: "IMA-Geral", grupo: "Renda fixa", fn: () => anbima("IMAGERAL") },
  { id: "IHFA", nome: "IHFA", grupo: "Multimercado", fn: () => anbima("IHFA") },
  { id: "SMLL", nome: "Small Caps", grupo: "Ações", fn: () => b3("SMLL") },
  { id: "IDIV", nome: "IDIV", grupo: "Ações", fn: () => b3("IDIV") },
  { id: "IFIX", nome: "IFIX", grupo: "Imobiliário", fn: () => b3("IFIX") },
  { id: "USD", nome: "Dólar", grupo: "Exterior", fn: () => dolar() },
  { id: "SPXBRL", nome: "S&P 500 (R$)", grupo: "Exterior", fn: (cal, ja) => spxReais(ja.USD) },
];

// ------------------------------------------------------------------ formatos de saída
// Dados publicados como texto: JSON compactado com gzip e codificado em base64, em arquivos .txt.
// Firewalls corporativos costumam bloquear arquivos .gz (erro 403); texto passa normalmente.
// Com SITE_SENHA, o gzip é cifrado (AES-256-GCM: iv de 12 bytes + texto cifrado + tag), e o navegador decifra com a senha.
const SENHA = process.env.SITE_SENHA || "";
const ITER = 210000, TESTE = "comparador-de-fundos";
let CHAVE = null, SAL = null;
function cifra(buf) {
  const iv = crypto.randomBytes(12), c = crypto.createCipheriv("aes-256-gcm", CHAVE, iv);
  return Buffer.concat([iv, c.update(buf), c.final(), c.getAuthTag()]);
}
function decifra(buf, chave = CHAVE) {
  const d = crypto.createDecipheriv("aes-256-gcm", chave, buf.subarray(0, 12));
  d.setAuthTag(buf.subarray(buf.length - 16));
  return Buffer.concat([d.update(buf.subarray(12, buf.length - 16)), d.final()]);
}
// Mantém o sal da publicação anterior quando a senha não mudou, para quem marcou "lembrar" não ter de digitar de novo.
async function prepararChave() {
  if (!SENHA) { log("sem SITE_SENHA: dados publicados sem senha"); return; }
  let prev = null;
  try { prev = await anterior("chave.json"); } catch {}
  if (prev?.sal) {
    const k = crypto.pbkdf2Sync(SENHA, Buffer.from(prev.sal, "base64"), prev.iter || ITER, 32, "sha256");
    try { if (decifra(Buffer.from(prev.teste, "base64"), k).toString() === TESTE) { SAL = Buffer.from(prev.sal, "base64"); CHAVE = k; } } catch {}
  }
  if (!CHAVE) { SAL = crypto.randomBytes(16); CHAVE = crypto.pbkdf2Sync(SENHA, SAL, ITER, 32, "sha256"); log("senha nova (ou primeira publicação com senha): quem acessa terá de digitar a senha de novo"); }
  else log("senha mantida");
}
const gzTexto = (str) => { const gz = zlib.gzipSync(str, { level: 9 }); return Buffer.from((CHAVE ? cifra(gz) : gz).toString("base64")); };
async function anteriorTexto(nome) {
  if (!SITE_URL) return null;
  try {
    const r = await fetch(`${SITE_URL}/dados/${nome}`, { headers: { "User-Agent": UA_NAVEGADOR } });
    if (!r.ok) return null;
    let buf = Buffer.from((await r.text()).trim(), "base64");
    if (CHAVE) buf = decifra(buf);
    return JSON.parse(zlib.gunzipSync(buf).toString());
  } catch { return null; }
}
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

// Métricas por janela para o ranking. A janela termina na data de referência do site e começa no mesmo dia
// N meses antes (no ano: 31/12 do ano anterior). Base = última cota até o início da janela (no máximo 10 dias antes).
const JANELAS_RK = [["1m", 1], ["ano", 0], ["12", 12], ["24", 24], ["36", 36]];
function inicioJanelaDia(refDia, meses) {
  const r = new Date(refDia * DIA_MS);
  if (meses === 0) return dia(`${r.getUTCFullYear() - 1}-12-31`);
  let a = r.getUTCFullYear(), m = r.getUTCMonth() - meses;
  while (m < 0) { m += 12; a--; }
  const ult = new Date(Date.UTC(a, m + 1, 0)).getUTCDate();
  return Math.round(Date.UTC(a, m, Math.min(r.getUTCDate(), ult)) / DIA_MS);
}
function metricasJanela(f, fatorCdi, refDia, meses) {
  const d = f.d, q = f.q, n = d.length;
  if (n < 3 || f.ult < refDia - 10) return null;
  const ini = inicioJanelaDia(refDia, meses);
  let lo = 0, hi = n - 1;
  if (d[0] > ini) return null;
  while (lo < hi) { const m = (lo + hi + 1) >> 1; if (d[m] <= ini) lo = m; else hi = m - 1; }
  const i0 = lo, i1 = n - 1;
  if (ini - d[i0] > 10 || i1 - i0 < 5) return null;
  const ret = q[i1] / q[i0] - 1, k = i1 - i0;
  let s = 0, s2 = 0;
  for (let i = i0 + 1; i <= i1; i++) { const r = q[i] / q[i - 1] - 1; s += r; s2 += r * r; }
  const med = s / k, vol = k > 2 ? Math.sqrt(Math.max(s2 / k - med * med, 0) * k / (k - 1)) * Math.sqrt(252) : null;
  const a = fatorCdi(d[i0]), b = fatorCdi(d[i1]), cdi = a && b ? b / a - 1 : null;
  const anos = k / 252, ann = ret > -1 ? Math.pow(1 + ret, 1 / anos) - 1 : null, cAnn = cdi != null ? Math.pow(1 + cdi, 1 / anos) - 1 : null;
  const sharpe = ann != null && cAnn != null && vol > 1e-9 ? (ann - cAnn) / vol : null;
  const r4 = (x) => (x == null || !isFinite(x) ? null : Math.round(x * 1e4) / 1e4);
  return { r: r4(ret), c: cdi > 1e-6 ? r4(ret / cdi) : null, v: r4(vol), s: sharpe == null || !isFinite(sharpe) ? null : Math.round(sharpe * 100) / 100 };
}

// ------------------------------------------------------------------ principal
async function main() {
  fs.mkdirSync(CACHE, { recursive: true });
  log(`início: ${INICIO}`);
  await prepararChave();
  await informes();
  for (const f of fundos.values()) ordenar(f);
  await cadastro();
  await extratos();
  await estruturados();
  try { await documentos(); } catch (e) { aviso(`documentos: ${e.message}`); }

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
    f.iProprio = n; f.fmProprio = f.fm || new Map(); // para agregados por gestora: só a parte do próprio fundo, sem o histórico emendado
    if (pai.fm) { const k0 = mesDe(primeiro); f.fm = new Map(f.fmProprio); for (const [k, a] of pai.fm) if (k < k0) f.fm.set(k, a); }
  }

  // Índices (com reserva no site já publicado)
  const prev = (await anteriorTexto("indices.txt")) || (await anterior("indices.json"));
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
  // Demais benchmarks. Calendário de dias úteis = datas do CDI (ou dias da semana, sem CDI).
  let calendario = [];
  if (indices.cdi) { let x = indices.cdi.d0; calendario.push(x); for (const k of indices.cdi.dd) { x += k; calendario.push(x); } }
  else for (let x = DIA_INI; x <= DIA_HOJE; x++) { const w = new Date(x * DIA_MS).getUTCDay(); if (w && w < 6) calendario.push(x); }
  indices.extra = {};
  const brutos = {};
  for (const b of BENCHMARKS) {
    try {
      const r = await b.fn(calendario, brutos);
      brutos[b.id] = r;
      indices.extra[b.id] = { nome: b.nome, grupo: b.grupo, tipo: b.tipo || "preco", fonte: r.fonte, ...compacta(r.d, r.v, 8) };
      log(`${b.nome}: ${r.d.length} pontos, ${iso(r.d[0])} → ${iso(r.d[r.d.length - 1])} (${r.fonte})`);
    } catch (e) {
      const p = prev?.extra?.[b.id];
      if (p) { indices.extra[b.id] = p; aviso(`${b.nome}: ${e.message}. Usando a série publicada anteriormente.`); }
      else aviso(`${b.nome}: ${e.message}. Sem série.`);
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

  const G_K1 = mesDe(refDia), G_K0 = G_K1 - 36, G_N = 37;
  const grupos = { gestor: new Map(), admin: new Map() };
  function somaGrupos(info, f) {
    const fm = f.fmProprio || f.fm, i0 = f.iProprio || 0;
    const pl = new Array(G_N).fill(null);
    for (let i = i0; i < f.d.length; i++) { const k = mesDe(f.d[i]) - G_K0; if (k >= 0 && k < G_N && isFinite(f.p[i])) pl[k] = f.p[i]; }
    for (const campo of ["gestor", "admin"]) {
      const nome = info[campo];
      if (!nome) continue;
      let g = grupos[campo].get(nome);
      if (!g) { g = { c: new Array(G_N).fill(0), r: new Array(G_N).fill(0), pl: new Array(G_N).fill(0) }; grupos[campo].set(nome, g); }
      for (let k = 0; k < G_N; k++) {
        const a = fm && fm.get(G_K0 + k);
        if (a) { g.c[k] += a[0]; g.r[k] += a[1]; }
        if (pl[k] != null) g.pl[k] += pl[k];
      }
    }
  }
  const grupoSaida = (m) => Object.fromEntries([...m].map(([nome, g]) => [nome, { c: g.c.map((x) => Math.round(x / 1000)), r: g.r.map((x) => Math.round(x / 1000)), pl: g.pl.map((x) => Math.round(x / 1000)) }]));

  fs.rmSync(SAIDA, { recursive: true, force: true });
  fs.mkdirSync(path.join(SAIDA, "f"), { recursive: true });

  const lista = [];
  let bytes = 0, pontos = 0, soCadastro = 0, comMensal = 0;
  const escrever = (id, obj) => {
    const gz = gzTexto(JSON.stringify(obj));
    fs.writeFileSync(path.join(SAIDA, "f", `${id}.txt`), gz);
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
    // (a série mensal, quando existe, entra logo abaixo)
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
    const sm = !temSerie ? serieMensal(cnpj) : null;
    const ei = estrInfo.get(cnpj) || {};
    if (sm) { if (sm.pl != null) pl = sm.pl; if (sm.cot != null) cot = sm.cot; }
    if (pl == null && c.pl_cad != null && (!temSerie || !ativoInforme)) pl = c.pl_cad;
    const univ = (publico === "Público geral" ? 1 : 0) | ((condominio || "").toLowerCase().startsWith("aberto") ? 2 : 0) | (exclusivo ? 4 : 0);
    return {
      id, cnpj, sub: sub || null, nome: desc(nome), tipo, estruturado, classe: c.classe || (estruturado ? tipo : null), anbima: c.anbima || e.anbima || null,
      situacao, ativo, gestor: c.gestor || ei.gestor || null, admin: c.admin || ei.admin || null, inicio: s.inicio || c.inicio || null,
      condominio, exclusivo, publico, qualificado: publico ? publico !== "Público geral" : null, previdenciario: s.previdenciario ?? null,
      fic: c.fic ?? null, taxa_adm: taxaAdm, fonte_taxa: fonteTaxa, taxa_perf: perf, aplic_min: e.aplic_min ?? null,
      conversao: e.conversao ?? null, pagamento: e.pagamento ?? null, tipo_dia: e.tipo_dia || null, taxa_saida: e.taxa_saida ?? null,
      custodia: e.custodia ?? null, distribuicao: e.distribuicao || null, extrato_data: e.dt || null,
      auditor: c.auditor || null, custodiante: c.custodiante || null, cancelamento: c.cancelamento || null,
      adaptacao175: c.adaptacao175 || null, registro555: c.registro555 || null, regime555: c.regime555 || null, regime175: c.regime175 || null,
      fundo_cnpj: c.fundo_cnpj && c.fundo_cnpj !== cnpj ? c.fundo_cnpj : null, fundo_nome: c.fundo_cnpj && c.fundo_cnpj !== cnpj ? c.fundo_nome : null,
      documentos: juntaDocs(cnpj, sub, c.fundo_cnpj),
      indicador: c.indicador || null, pl: pl != null ? Math.round(pl) : null, data_pl: temSerie && pl != null ? iso(f.ult) : sm?.dataPl || c.data_pl_cad || null,
      segmento: ei.segmento || null, mandato: ei.mandato || null, isin: ei.isin || null, gestao: ei.gestao || null, bolsa: ei.bolsa ?? null,
      ret12_mensal: sm?.ret12 ?? null, ultimo_mes: sm?.ultimo || null, _mensal: sm?.serie || null,
      cotistas: cot, serie: temSerie ? "D" : sm || estruturado ? "M" : "",
      primeira: temSerie ? iso(f.d[0]) : null, ultima: temSerie ? iso(f.ult) : null, pregoes: temSerie ? f.d.length : 0,
      encadeado: f?.encadeado || null, univ,
    };
  }

  // 1) Fundos com informe diário
  for (const [id, f] of fundos) {
    if (f.d.length < 2) continue;
    const info = montarInfo(id, f.cnpj, f.sub, f);
    const ms = JANELAS_RK.map(([, meses]) => metricasJanela(f, fatorCdi, refDia, meses));
    delete info._mensal;
    const cap12 = captacao12(f, refDia);
    // "Para investidor": exclui exclusivos, masters e fundos de um cotista só (espelhos e veículos internos)
    const inv = !info.estruturado && !(info.univ & 4) && !/\bMASTER\b/i.test(info.nome || "") && (info.cotistas == null || info.cotistas > 1) ? 1 : 0;
    if (inv && info.ativo) somaGrupos(info, f);
    escrever(id, { ...info, cap12, serie: serieLog(f.d, f.q), semanal: serieSemanal(f.d, f.p, f.n), fluxo: serieFluxo(f) });
    pontos += f.d.length;
    lista.push([id, info.nome, info.classe, info.situacao, info.ativo, info.serie,
      info.taxa_adm, info.pl, info.gestor, info.admin, info.inicio, info.univ, info.cotistas, info.tipo, info.ultima, cap12, inv,
      ...ms.flatMap((m) => [m?.r ?? null, m?.c ?? null, m?.v ?? null, m?.s ?? null])]);
  }
  // 2) Fundos e classes só com cadastro (estruturados, pré-operacionais, cancelados sem informe)
  const cnpjsComSerie = new Set([...fundos.values()].filter((f) => f.d.length >= 2).map((f) => f.cnpj));
  for (const [cnpj] of cad) {
    if (cnpjsComSerie.has(cnpj) || guardaChuva.has(cnpj)) continue;
    const info = montarInfo(cnpj, cnpj, "", null);
    if (!info.nome || info.nome.startsWith("Fundo ")) continue;
    const { _mensal, ...resto } = info;
    escrever(cnpj, _mensal ? { ...resto, mensal: _mensal } : resto);
    soCadastro++;
    if (_mensal) comMensal++;
    lista.push([cnpj, info.nome, info.classe, info.situacao, info.ativo, info.serie,
      info.taxa_adm, info.pl, info.gestor, info.admin, info.inicio, info.univ, info.cotistas, info.tipo, info.ultima, null, 0,
      ...JANELAS_RK.flatMap(([k]) => [k === "12" ? info.ret12_mensal : null, null, null, null])]);
  }

  // Ativos primeiro, depois por PL
  const campos = ["id", "nome", "classe", "situacao", "ativo", "serie", "taxa_adm", "pl", "gestor", "admin", "inicio", "univ", "cotistas", "tipo", "ultima", "cap12", "inv",
    ...JANELAS_RK.flatMap(([k]) => ["r" + k, "c" + k, "v" + k, "s" + k])];
  const iPl = campos.indexOf("pl");
  lista.sort((a, b) => b[4] - a[4] || (b[iPl] || 0) - (a[iPl] || 0));
  // Captação e PL por gestora e por administradora (fundos para investidor, ativos), últimos 37 meses, em R$ mil
  const gz2 = gzTexto(JSON.stringify({ m0: mesIso(G_K0), ref: iso(refDia), gestor: grupoSaida(grupos.gestor), admin: grupoSaida(grupos.admin) }));
  fs.writeFileSync(path.join(SAIDA, "grupos.txt"), gz2);
  const idx = gzTexto(JSON.stringify({ campos, ref: iso(refDia), fundos: lista }));
  fs.writeFileSync(path.join(SAIDA, "fundos.txt"), idx);
  fs.writeFileSync(path.join(SAIDA, "indices.txt"), gzTexto(JSON.stringify(indices)));
  // Peer groups da casa: publicados junto com os dados (cifrados quando há senha). Com senha, o peers.json aberto sai do site publicado.
  const arqPeers = process.env.PEERS_JSON_FILE || path.join(path.dirname(SAIDA), "peers.json");
  try {
    const txt = process.env.PEERS_JSON || fs.readFileSync(arqPeers, "utf8");
    JSON.parse(txt);
    fs.writeFileSync(path.join(SAIDA, "peers.txt"), gzTexto(txt));
    if (CHAVE && !process.env.PEERS_JSON_FILE && fs.existsSync(arqPeers)) fs.rmSync(arqPeers);
  } catch (e) { aviso(`peers: ${e.message}`); }
  if (CHAVE) fs.writeFileSync(path.join(SAIDA, "chave.json"), JSON.stringify({ v: 1, iter: ITER, sal: SAL.toString("base64"), teste: cifra(Buffer.from(TESTE)).toString("base64") }));
  const comSerie = lista.filter((x) => x[5] === "D").length;
  const meta = {
    atualizado: new Date().toISOString(), ultima_cota: iso(refDia), fundos: lista.length, com_serie_diaria: comSerie,
    so_cadastro: soCadastro, fundos_ativos: lista.filter((x) => x[4]).length,
    inicio: iso([...fundos.values()].reduce((m, f) => (f.d.length ? Math.min(m, f.d[0]) : m), Infinity)), cotas: pontos,
    tamanho_mb: Math.round((bytes + idx.length) / 1e6), fontes: { cdi: indices.cdi?.fonte || null, ibov: indices.ibov?.fonte || null, ...Object.fromEntries(Object.entries(indices.extra || {}).map(([k, x]) => [k, x.fonte])) }, avisos,
  };
  fundos.clear();
  mensal.clear();
  let mbCarteira = 0;
  try { mbCarteira = await carteiras(); } catch (e) { aviso(`carteiras: ${e.message}`); }
  meta.tamanho_mb += mbCarteira;
  meta.com_mensal = comMensal;
  fs.writeFileSync(path.join(SAIDA, "meta.json"), JSON.stringify(meta, null, 1));
  log(`saída: ${lista.length} fundos (${comSerie} com série diária, ${soCadastro} só cadastro, ${meta.fundos_ativos} ativos), ${pontos.toLocaleString("pt-BR")} cotas, ${meta.tamanho_mb} MB, índice ${(idx.length / 1e6).toFixed(1)} MB`);
  if (meta.tamanho_mb > 950) aviso(`site com ${meta.tamanho_mb} MB, perto do limite de 1 GB do GitHub Pages`);
  if (avisos.length) log(`${avisos.length} aviso(s)`);
}

main().catch((e) => { console.error("::error::" + e.message); console.error(e); process.exit(1); });
