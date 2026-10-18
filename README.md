# Lupa de Fundos

Comparador de fundos de investimento brasileiros: busque por nome, gestora ou CNPJ e compare até cinco fundos lado a lado (rentabilidade, % do CDI, volatilidade, Sharpe, drawdown e cadastro), contra CDI e Ibovespa.

**Site:** https://jgasbandeira.github.io/lupa-de-fundos/

## Como funciona

Tudo roda no GitHub, sem servidor e sem nada no seu computador.

1. Duas vezes por dia (7h30 e 20h30, horário de Brasília) o GitHub Actions roda `scripts/coleta.mjs`.
2. O script baixa os dados públicos:
   - CVM: informe diário (cota, PL, cotistas) e cadastro de fundos, classes e subclasses
   - Banco Central: CDI diário (SGS, série 12)
   - Ibovespa (Yahoo Finance)
3. Gera arquivos estáticos em `site/dados/` e publica o site no GitHub Pages.

Os arquivos já baixados da CVM ficam em cache, então só os meses recentes são baixados de novo.

## Rodar a atualização na hora

Aba **Actions** → **Atualizar dados e publicar site** → **Run workflow**.

## O que entra no site

- Fundos com cota nos últimos 45 dias e PL de pelo menos R$ 1 milhão (`PL_MIN` no workflow).
- Histórico dos últimos 5 anos (`ANOS` no workflow).
- Subclasses criadas na adaptação à RCVM 175 têm o histórico anterior emendado a partir da classe.

## Métricas

- Rentabilidade pela variação da cota (líquida de taxas). Anualização e volatilidade com 252 dias úteis.
- Sharpe: (retorno anualizado − CDI anualizado no mesmo período) ÷ volatilidade.
- % do CDI: retorno do fundo ÷ retorno do CDI entre as mesmas datas.

## Limitações

- Ibovespa vem do Yahoo Finance, que não é fonte oficial. Se falhar, o site mantém a série da publicação anterior.
- Taxas de administração e performance vêm do cadastro antigo da CVM (`cad_fi.csv`) e podem faltar em fundos novos.
- FIIs, FIDCs e ETFs não estão no informe diário da CVM.

Rentabilidade passada não garante resultados futuros. Este site não é recomendação de investimento.
