# Extrator SPED · v6.0

Workspace web para conferir EFD ICMS/IPI e EFD Contribuições e exportar dados para Excel com origem preservada.

**Documentos aparecem uma vez. Detalhes ficam em tabelas separadas.** Linhas rejeitadas e registros fora do catálogo são preservados para revisão e não entram nos indicadores. Consulte [o escopo de validação](docs/VALIDATION.md) antes de usar os resultados.

## Executar localmente

Requer Python 3.11 ou superior. Recomenda-se ambiente virtual dedicado.

```sh
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\Activate.ps1
# Linux/macOS: source .venv/bin/activate
python -m pip install -r requirements.txt
streamlit run app.py --server.address 127.0.0.1
```

Abra o endereço exibido no terminal. Use **Explorar demonstração** para conhecer o fluxo com dados fictícios. A demonstração é uma amostra de extração e não uma escrituração pronta para transmissão.

## Fluxo de trabalho

1. **Importar:** selecione TXT/SPED. O tipo é identificado pelo cabeçalho de cada arquivo, incluindo lotes mistos. A codificação automática tenta UTF-8, Windows-1252 e Latin-1; pode ser escolhida manualmente.
2. **Conferir qualidade:** revise arquivos rejeitados, campos inválidos, diferenças de estrutura e registros não suportados. O modo estrito bloqueia um arquivo com qualquer ocorrência; os demais continuam.
3. **Explorar documentos:** filtre por escrituração, arquivo, estabelecimento, período, operação, CFOP, número ou participante. Os filtros são imediatos. Limpar filtros restaura a seleção completa.
4. **Inspecionar:** abra um documento para ver seus detalhes e vínculos ou consulte qualquer registro na área Registros.
5. **Exportar:** prepare o Excel explicitamente. O download é invalidado quando a seleção muda, evitando baixar resultados de filtros anteriores.

Arquivos idênticos são reconhecidos pelo SHA-256 e não são adicionados novamente. Arquivos distintos da mesma empresa, tipo e período sobreposto são mantidos para conferência, mas bloqueiam indicadores conjuntos. Selecione apenas a versão desejada em Arquivos. O sistema não escolhe automaticamente entre original e retificadora.

Cada escrituração tem indicadores separados. O filtro CFOP seleciona documentos que contêm o código; **não rateia o valor da nota por CFOP**. A contagem inclui documentos/operações aceitos dos códigos definidos em `schema.DOCUMENTS`; não inclui apuração ou registros agregados como novas notas. Cancelados e denegados podem ser consultados, mas ficam fora dos indicadores. Tributos são valores informados nos cabeçalhos, não imposto apurado ou a recolher.

## Conteúdo do Excel

| Aba | Conteúdo |
|---|---|
| LEIA_ME | Escopo e regras de interpretação |
| DOCUMENTOS | Uma linha por documento/operação de origem |
| ARQUIVOS | Empresa, período, hash, tipo, codificação e métricas |
| OCORRENCIAS | Arquivo, linha, campo e motivo |
| ARQUIVOS_REJEITADOS | Motivo e conteúdo em Base64 dos arquivos bloqueados durante o processamento |
| REG_* | Registros interpretados e aceitos, com vínculos |
| ORIG_* | Campos originais e texto da linha, inclusive rejeitados/não suportados |

**Lote completo** inclui todas essas tabelas. **Documentos filtrados** inclui apenas documentos visíveis e a explicação do escopo. Registros completos não são filtrados pela seleção de documentos, para preservar cadastros e apuração sem vínculo a uma nota.

`REGISTRO_ID` combina SHA-256 e linha. `PAI_ID` contém o pai explicitamente mapeado. `DOCUMENTO_ID` identifica a nota/operação de origem. Cada linha contém arquivo, tipo e estabelecimento.

Campos textuais são gravados como texto, inclusive quando começam com `=`. Abas são divididas ao atingir o limite do Excel; campos longos são divididos em colunas numeradas para não truncar. Valores são Decimal no processamento; o Excel tem precisão numérica limitada, por isso as abas originais preservam o texto recebido.

## Linha de comando

```sh
python sped_parser.py arquivo.txt outro.txt --out resultado.xlsx
python sped_parser.py arquivo.txt --out resultado.xlsx --strict
```

A exportação completa pode incluir arquivos com ocorrências. No modo estrito, arquivos com qualquer ocorrência ficam rejeitados. Falhas de arquivo resultam em saída não zero, mantendo o relatório dos demais e os motivos. O limite de tamanho é verificado antes da leitura na CLI.

## Limites e operação

`config.yaml` centraliza os limites: 100 MB por arquivo, 200 MB por lote, 20 arquivos e 500 mil linhas por arquivo. A exportação tem limite de 5 milhões de células para evitar consumo descontrolado. Esses limites não constituem garantia de capacidade; dimensione o servidor com dados representativos.

O processamento usa memória e é sequencial. Não há produto cartesiano entre tabelas filhas, banco de dados nem cache global de dados fiscais. O Excel é escrito em modo sequencial e preparado somente por solicitação. A sessão mantém o lote até ser limpa ou encerrada; o processo hospedeiro e seus administradores têm acesso à memória do servidor.

Os dados não são enviados a APIs externas pela aplicação. Não inclua arquivos fiscais reais, planilhas ou segredos no Git; as pastas `data/`, `exports/` e `.streamlit/secrets.toml` são ignoradas.

### Docker

```sh
docker build -t extrator-sped .
docker run --rm -p 127.0.0.1:8501:8501 extrator-sped
```

A imagem executa como usuário sem privilégios e oferece healthcheck. O Dockerfile deve ser construído e testado no ambiente de destino. Para disponibilizar a outras pessoas, configure autenticação no proxy/plataforma, HTTPS, restrição de rede, limites de memória e monitoramento. O aplicativo não implementa uma base própria de usuários. Não desative CORS ou proteção XSRF.

## Desenvolvimento e testes

```sh
python -m pip install -r requirements-dev.txt
ruff check .
ruff format --check .
pytest -q
```

A CI executa os mesmos comandos em Python 3.11 e 3.14. As dependências diretas têm versões fixas. Os testes cobrem parsing, integridade, filtros, exportação, arquivos inválidos e interações Streamlit. Leia [CHANGELOG.md](CHANGELOG.md) para migração da v5.

| Arquivo | Responsabilidade |
|---|---|
| app.py | Interface, estado da sessão e apresentação |
| sped_parser.py | Parsing, validação aplicada, lote, filtros e indicadores |
| schema.py | Seleção de layout, campos numéricos e relações |
| export.py | Excel seguro e preservação de conteúdo |
| layouts_*.py | Catálogos de campos por escrituração |
| validators.py | Validadores auxiliares e CNPJ numérico/alfanumérico |
| demo.py | Amostra fictícia usada na demonstração e nos testes |
| tests/ | Regressões e testes da interface |

## Limites de validade

Não é um PVA, não consulta a SEFAZ e não assegura conformidade fiscal integral. A contagem de campos é comparada ao catálogo local: versões diferentes ficam visíveis como rejeições, sem descarte silencioso. Homologue os layouts e arquivos usados pela sua operação, inclusive arquivos retificadores, e compare os resultados com uma referência confiável. A atualização não foi testada com dados fiscais reais.

Licenciamento mantido: uso interno, todos os direitos reservados.
