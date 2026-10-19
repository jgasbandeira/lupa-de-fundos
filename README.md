# Lupa de Fundos

Terminal de fundos de investimento brasileiros com dados públicos da CVM:

- **F1 Painel**: favoritos (estrela ☆ em qualquer tela) e cestas de fundos personalizadas, salvos no navegador de cada pessoa. Cestas têm link para compartilhar; o painel inteiro pode ser exportado e importado. O site abre no Painel para quem já tem favoritos ou cestas, e no Ranking para os demais.
- **F2 Ranking**: todos os fundos com filtros por universo, classe, situação, série e PL; colunas ordenáveis; seleção vai para a comparação ou para uma cesta.
- **F3 Compara**: até 8 fundos lado a lado, janelas de 1M a MAX ou datas livres, mais de 30 indicadores (Sharpe, Sortino, VaR, beta, alfa de Jensen…), matriz de correlação e exportação em CSV.
- **F4 Gestores e administradores**: número de fundos e PL total; ao escolher um, a lista dos fundos dele.
- **F5 Estruturados**: FII, FIDC, FIP, Fiagro e afins, com PL, cotistas e rentabilidade mensal (informes mensais da CVM).
- **Ficha do fundo**: desempenho, PL e cotistas, retorno mês a mês contra o CDI, carteira (últimas 12 competências da CDA), cadastro completo e exportação do histórico de cotas em CSV.

Atalhos: F1 a F5 trocam de tela. Tema claro e escuro.

**Site:** https://jgasbandeira.github.io/lupa-de-fundos/

## Como funciona

Tudo roda no GitHub, sem servidor e sem nada no seu computador.

1. Duas vezes por dia (7h30 e 20h30, horário de Brasília) o GitHub Actions roda `scripts/coleta.mjs`.
2. O script baixa os dados públicos:
   - CVM: informe diário (cota, PL, cotistas), cadastro de fundos, classes e subclasses, e extrato de informações (taxas, aplicação mínima, prazos)
   - CDI diário: Banco Central (SGS 12) ou, quando o BC bloqueia, Selic over do Ipeadata calibrada pelo CDI mensal oficial
   - Ibovespa (Yahoo Finance)
3. Gera arquivos estáticos em `site/dados/` e publica o site no GitHub Pages.

Os arquivos já baixados da CVM ficam em cache, então só os meses recentes são baixados de novo.

## Rodar a atualização na hora

Aba **Actions** → **Atualizar dados e publicar site** → **Run workflow**.

## O que entra no site

- Todos os fundos que já enviaram informe diário à CVM, ativos e encerrados. Fundos cancelados ficam ocultos; aparecem ao marcar "Incluir fundos cancelados" no topo.
- Histórico completo, desde o início do informe diário da CVM. As séries ficam compactadas (gzip) para caber no limite de 1 GB do GitHub Pages.
- Subclasses criadas na adaptação à RCVM 175 têm o histórico anterior emendado a partir da classe.

## Métricas

- Rentabilidade pela variação da cota (líquida de taxas). Anualização e volatilidade com 252 dias úteis.
- Sharpe: (retorno anualizado − CDI anualizado no mesmo período) ÷ volatilidade.
- % do CDI: retorno do fundo ÷ retorno do CDI entre as mesmas datas.

## Limitações

- Ibovespa vem do Yahoo Finance, que não é fonte oficial. Se falhar, o site mantém a série da publicação anterior.
- Taxas vêm do extrato de informações da CVM (ou do cadastro antigo quando o extrato não tem). Podem faltar em fundos que não enviaram extrato.
- FIIs, FIDCs e Fiagros têm só dados mensais (informe mensal). FIPs aparecem com cadastro e PL declarado.
- Na carteira, a CVM permite omitir posições temporariamente; nesses meses só o agregado por categoria é publicado.

Rentabilidade passada não garante resultados futuros. Este site não é recomendação de investimento.

## Log das execuções

O resultado da última coleta fica no ramo [`logs`](../../tree/logs): `coleta.log`, `meta.json` e `status.txt`.
