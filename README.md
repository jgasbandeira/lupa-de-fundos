# Comparador de Fundos

Terminal de fundos de investimento brasileiros com dados públicos da CVM:

- **F1 Painel**: favoritos (estrela ☆ em qualquer tela) e peer groups de fundos personalizados, salvos no navegador de cada pessoa. Peer groups têm link para compartilhar; o painel inteiro pode ser exportado e importado. O site abre no Painel para quem já tem favoritos ou peer groups, e no Ranking para os demais.
- **F2 Ranking**: só fundos líquidos (com cota diária; FII, FIDC, FIP, Fiagro e afins ficam fora e aparecem em F5), com janelas de 1M, no ano, 12M, 24M e 36M (retorno, % do CDI, volatilidade e Sharpe), filtros por universo, classe, situação e PL e a opção "Só fundos para investidor" (ligada por padrão: tira exclusivos, masters e fundos de um cotista só); colunas ordenáveis, inclusive captação líquida em 12 meses; seleção vai para a comparação ou para um peer group.
- **F3 Compara**: até 8 fundos lado a lado, janelas de 1M a MAX ou datas livres, mais de 30 indicadores (Sharpe, Sortino, VaR, beta, alfa de Jensen…), excesso, tracking error e information ratio contra os benchmarks escolhidos, matriz de correlação e exportação em CSV.
- **F4 Gestores e administradores**: PL, market share e captação líquida (último mês fechado e 12 meses) somando os fundos líquidos para investidor; ao escolher um, captação mês a mês e PL dos últimos 36 meses (com CSV); ao escolher um, a lista dos fundos dele.
- **F5 Estruturados**: FII, FIDC, FIP, Fiagro e afins, com PL, cotistas e rentabilidade mensal (informes mensais da CVM).
- **Ficha do fundo**: desempenho contra até 6 benchmarks à escolha (padrão: CDI e o indicador de desempenho do cadastro), tabela fundo × benchmark, PL e cotistas, captação e resgates mês a mês (com exportação em CSV), retorno mês a mês, carteira (últimas 12 competências da CDA), cadastro completo e exportação do histórico de cotas em CSV.

Atalhos: F1 a F5 trocam de tela. Tema claro e escuro.

**Site:** https://comparadorfundos.com.br (o endereço antigo, jgasbandeira.github.io/lupa-de-fundos, redireciona para ele)

## Como funciona

Tudo roda no GitHub, sem servidor e sem nada no seu computador.

1. Duas vezes por dia (7h30 e 20h30, horário de Brasília) o GitHub Actions roda `scripts/coleta.mjs`.
2. O script baixa os dados públicos:
   - CVM: informe diário (cota, PL, cotistas, captação e resgates do dia), cadastro de fundos, classes e subclasses, e extrato de informações (taxas, aplicação mínima, prazos)
   - CDI diário: Banco Central (SGS 12) ou, quando o BC bloqueia, Selic over do Ipeadata calibrada pelo CDI mensal oficial
   - Benchmarks: Ibovespa, Small Caps, IDIV e IFIX (B3); IMA-B, IMA-B 5, IMA-B 5+, IRF-M, IMA-S, IMA-Geral e IHFA (ANBIMA, histórico oficial); IPCA, IGP-M e dólar PTAX (Ipeadata); S&P 500 em reais (Yahoo Finance × PTAX). IPCA + X% é montado no navegador com a taxa que a pessoa digitar.
3. Gera arquivos estáticos em `site/dados/` e publica o site no GitHub Pages.

Os arquivos já baixados da CVM ficam em cache, então só os meses recentes são baixados de novo.

## Senha de acesso

O site pede uma senha quando existe o segredo `SITE_SENHA` no repositório (**Settings → Secrets and variables → Actions → New repository secret**). Com ele, todos os arquivos de dados são publicados criptografados (AES-256-GCM, chave derivada da senha com PBKDF2), então sem a senha não dá para ver nem os dados nem as telas.

- Para trocar a senha, edite o segredo e rode a atualização. Quem tinha marcado "Lembrar neste computador" vai precisar digitar a nova.
- Para tirar a senha, apague o segredo e rode a atualização.
- O botão **Sair** no topo esquece a senha naquele navegador.

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

- Se uma fonte de benchmark falhar, o site mantém a série da publicação anterior. O Ibovespa usa o Yahoo Finance como reserva da B3.
- IPCA e IGP-M são mensais: a variação do mês é distribuída nos dias úteis, e a série termina no último mês divulgado.
- Taxas vêm do extrato de informações da CVM (ou do cadastro antigo quando o extrato não tem). Podem faltar em fundos que não enviaram extrato.
- Captação e resgates ficam agregados por mês. Num período que começa no meio de um mês, esse mês só entra se o período começar na primeira semana dele; assim a janela de 12M soma 12 meses.
- FIIs, FIDCs e Fiagros têm só dados mensais (informe mensal). FIPs aparecem com cadastro e PL declarado.
- Na carteira, a CVM permite omitir posições temporariamente; nesses meses só o agregado por categoria é publicado.

Rentabilidade passada não garante resultados futuros. Este site não é recomendação de investimento.

## Log das execuções

O resultado da última coleta fica no ramo [`logs`](../../tree/logs): `coleta.log`, `meta.json` e `status.txt`.
