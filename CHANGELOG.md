# Histórico

## 6.0.0

- Documentos únicos e detalhes em tabelas separadas, sem cruzamento multiplicativo entre registros filhos.
- Chaves por hash do arquivo e linha, vínculo pai-filho explícito e cadastro de participantes por estabelecimento.
- Identificação de tipo por cabeçalho de cada arquivo, duplicatas exatas ignoradas com aviso e bloqueio de indicadores para arquivos sobrepostos.
- Validação estrutural, CNPJ numérico/alfanumérico, datas, números, CFOP, formato de chaves e reconciliação de mercadorias.
- Quarentena com conteúdo original e métricas de linhas aceitas, rejeitadas, ignoradas e não suportadas.
- Valores decimais exatos no processamento; códigos preservados e rótulos de operação separados.
- Nova interface com demonstração, filtros reativos, paginação, inspetor de documentos, qualidade e exportação explícita.
- Excel completo ou filtrado, células textuais protegidas contra fórmulas e divisão de abas grandes.
- Limites de recursos, dependências fixadas, testes de regressão e interface, CI e imagem Docker sem usuário root.

### Migração

O consolidado achatado v5 foi substituído pela aba DOCUMENTOS e pelas abas REG_* / ORIG_*.
Scripts externos que importam as classes legadas de sped_parser devem migrar para process_content/process_batch.
O caminho CLI passa a ser `python sped_parser.py arquivo.txt --out resultado.xlsx`.
Não há banco de dados para migrar. Não há validação fiscal integral nem conversão automática de arquivos entre layouts.
