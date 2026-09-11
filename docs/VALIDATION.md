# Escopo de validação

## Referências

- [Guia Prático EFD ICMS/IPI 3.2.2](https://www.gov.br/sped/pt-br/assuntos/escrituracoes-digitais/efd-icms-ipi/manuais-e-documentos-tecnicos/guia-pratico-da-efd-icms-ipi-3-2.2): datas em DDMMAAAA; registros e relações são específicos por escrituração.
- [Manuais de EFD Contribuições](https://sped.rfb.gov.br/item/show/1989): referência para o cabeçalho e estrutura dessa escrituração.
- [Cálculo do dígito verificador do CNPJ](https://www.gov.br/receitafederal/pt-br/centrais-de-conteudo/publicacoes/documentos-tecnicos/cnpj/manual-dv-cnpj.pdf): algoritmo numérico e alfanumérico (ASCII menos 48).

Consultados em 10–11/09/2026. Os catálogos existentes foram mantidos como base de extração. Foram conferidos e corrigidos C170 (38 campos), C176 (27), C500 (40), K200 (6) e 0221 (3) no catálogo fiscal; D700/D730/D731 foram incluídos conforme o guia 3.2.2. Não se presume cobertura completa de todas as versões oficiais; versões históricas com outra estrutura permanecem preservadas para revisão.

## Regras implementadas

O tipo é detectado pela posição das datas e demais campos no registro 0000, individualmente por arquivo. Cabeçalhos inválidos e tipo manual incompatível bloqueiam o arquivo.

Registros conhecidos com quantidade diferente de campos são preservados em ORIG_* e rejeitados da interpretação. Campos adicionais recebem nomes EXTRA_NNN; não há truncamento. Registros desconhecidos são preservados com campos posicionais, fora dos indicadores. Valores obrigatórios básicos, datas reais, números, CNPJ e formatos de CFOP e chaves são verificados. O formato de chave é validado; não há consulta à SEFAZ nem garantia de autorização do documento.

As relações explicitamente mapeadas têm identificador de pai e escopo de ancestralidade. O processamento não tenta adivinhar vínculos através de registros desconhecidos. Relações não mapeadas permanecem sem pai; os dados brutos continuam disponíveis. É necessário ampliar o catálogo para interpretar versões ou registros adicionais.

Valores são Decimal no processamento. A comparação entre VL_MERC do C100 e soma VL_ITEM do C170 usa tolerância de R$ 0,01 e gera aviso; não trata toda divergência como erro fiscal. O modo estrito bloqueia um arquivo com qualquer ocorrência, incluindo avisos.

Indicadores abrangem os registros de documentos/operações definidos em schema.DOCUMENTS. Não somam apuração, estoque, registros agregados ou detalhes como se fossem novas notas. Tributos vêm dos cabeçalhos e são rotulados como informados; não são débito, crédito, imposto a recolher nem rateio por CFOP. Cancelados e denegados ficam fora dos indicadores.

## Verificação automatizada

Os testes usam somente arquivos fictícios e cobrem o caso 1 nota / 2 itens / 2 analíticos, filtros, datas, precisão decimal, quarentena, duplicatas, períodos sobrepostos, hierarquia, exportação completa, conteúdo textual que começa com igual, campos longos, CNPJ e reexecuções da interface.

Antes de homologar, executar arquivos reais anonimizados de cada tipo e versão utilizada, comparar contagens e valores com referência conhecida e PVA, testar grandes volumes e conferir a autenticação e o dimensionamento do ambiente de hospedagem. A presença de CI não substitui essa homologação.
