# Extrator SPED V3.0

Sistema avançado para extração e consolidação de dados de arquivos SPED (Sistema Público de Escrituração Digital) para formato Excel.

## 📋 Características

- **Processamento robusto**: Tratamento de erros aprimorado com exceções customizadas
- **Validação de dados**: Validação de CNPJs, datas, campos obrigatórios e integridade referencial
- **Performance otimizada**: Operações vetorizadas e processamento eficiente
- **Métricas detalhadas**: Rastreamento completo do processamento com estatísticas
- **Interface gráfica moderna**: GUI com barra de progresso e processamento assíncrono
- **Configurável**: Arquivo YAML para personalização de parâmetros
- **Testado**: Suite de testes unitários incluída

## 🚀 Instalação

### Dependências

```bash
pip install pandas openpyxl pyyaml charset-normalizer pytest
```

### Estrutura de Arquivos

```
V3.0/
├── Extrat_V3.py          # Arquivo principal
├── exceptions.py         # Exceções customizadas
├── validators.py         # Validadores de dados
├── metrics.py            # Sistema de métricas
├── config.yaml           # Configurações
├── test_extrat_v3.py     # Testes unitários
└── README.md             # Este arquivo
```

## 💻 Uso

### Interface Gráfica (Recomendado)

```bash
python Extrat_V3.py
```

Isso abrirá uma janela onde você pode:
1. Selecionar um ou mais arquivos SPED (.txt)
2. Escolher o local para salvar o Excel
3. Clicar em "Processar Arquivos"
4. Acompanhar o progresso na barra de status

### Linha de Comando

```bash
# Arquivo único
python Extrat_V3.py arquivo.txt --out saida.xlsx

# Múltiplos arquivos
python Extrat_V3.py arquivo1.txt arquivo2.txt --out consolidado.xlsx

# Com nível de log customizado
python Extrat_V3.py arquivo.txt --out saida.xlsx --log-level DEBUG
```

## 📊 Registros Suportados

### Bloco C - Documentos Fiscais I
- **C100**: Nota Fiscal (modelo 1/1A)
- **C170**: Itens do documento
- **C190**: Registro analítico
- **C500**: Nota Fiscal de Energia Elétrica
- **C501/C505**: Detalhamento PIS/COFINS

### Bloco D - Documentos Fiscais II
- **D100**: Conhecimento de Transporte
- **D170**: Itens do documento
- **D500**: Nota Fiscal de Serviço de Comunicação
- **D501/D505**: Detalhamento PIS/COFINS
- **D700**: NFCom (Nota Fiscal Fatura Eletrônica)

### Bloco A - Documentos Fiscais III
- **A100**: Documento de Serviços

### Bloco F - Demais Documentos
- **F100**: Demais documentos e operações

### Bloco M - Apuração de Contribuições
- **M100/M105/M110/M115**: Créditos e ajustes

### Bloco E - Apuração ICMS/IPI
- **E100/E110**: Período e apuração
- **E111/E112/E113/E115/E116**: Ajustes e detalhamentos

## ⚙️ Configuração

Edite o arquivo `config.yaml` para personalizar:

```yaml
processing:
  max_file_size_mb: 100        # Tamanho máximo de arquivo
  chunk_size: 10000            # Tamanho do chunk de processamento
  validation_tolerance: 0.01   # Tolerância para validações

validation:
  validate_cnpj: true          # Validar CNPJs
  validate_dates: true         # Validar datas
  strict_mode: false           # Modo estrito (interrompe em erros)

gui:
  window_title: 'SPED → Excel - Extrator v3.0'
  show_progress_bar: true      # Mostrar barra de progresso
```

## 🧪 Testes

Execute a suite de testes:

```bash
# Todos os testes
pytest test_extrat_v3.py -v

# Testes específicos
pytest test_extrat_v3.py::TestValidators -v
pytest test_extrat_v3.py::TestMetrics -v
```

## 📈 Métricas de Processamento

O sistema rastreia automaticamente:
- Total de linhas processadas
- Taxa de sucesso
- Erros por tipo
- Registros por tipo
- Tempo de processamento
- Velocidade (linhas/segundo)

Exemplo de saída:

```
============================================================
RESUMO DO PROCESSAMENTO
============================================================
Arquivo: sped_exemplo.txt
Total de linhas: 15,234
Processadas com sucesso: 15,180
Linhas com erro: 54
Taxa de sucesso: 99.65%
Tempo de processamento: 3.45s
Velocidade: 4,400 linhas/segundo

Top 10 Registros Processados:
  C100: 1,234
  C170: 5,678
  D100: 234
  ...
============================================================
```

## 🔍 Validações Implementadas

### Validação de Dados
- **CNPJ**: Validação completa com dígitos verificadores
- **Datas**: Verificação de formatos e valores válidos
- **Campos numéricos**: Validação de formato brasileiro
- **Chaves NFe**: Verificação de 44 dígitos
- **CFOPs**: Validação de 4 dígitos

### Validação de Integridade
- Campos obrigatórios por tipo de registro
- Validação cruzada de totais (soma de itens vs total do documento)
- Verificação de referências entre registros

## 🐛 Tratamento de Erros

O sistema utiliza exceções customizadas para melhor diagnóstico:

- `SpedParseError`: Erros de parsing de linhas
- `SpedValidationError`: Erros de validação de dados
- `SpedFileError`: Problemas com arquivos
- `SpedEncodingError`: Erros de encoding
- `SpedIntegrityError`: Inconsistências de integridade

## 📝 Formato de Saída

O Excel gerado contém múltiplas planilhas:

### Planilhas Consolidadas
- `C_CONSOLIDADO`: Notas fiscais com itens agregados
- `D_CONSOLIDADO`: CTes com itens agregados
- `A_CONSOLIDADO`: Documentos de serviços
- `F_CONSOLIDADO`: Demais documentos
- `E_CONSOLIDADO`: Apuração ICMS/IPI

### Planilhas Detalhadas
- Registros principais (C100, D100, etc.)
- Registros filhos (C170, D170, etc.)
- Blocos de apuração (M100, M105, etc.)

### Formatação
- Valores monetários em formato R$ brasileiro
- Datas convertidas para formato legível
- Indicadores traduzidos (Entrada/Saída, etc.)

## 🔧 Troubleshooting

### Erro de encoding
```
SpedEncodingError: Falha ao detectar encoding
```
**Solução**: Verifique o encoding do arquivo ou ajuste `fallback_encodings` no config.yaml

### Arquivo muito grande
```
SpedFileError: Arquivo muito grande: 150.00 MB (máximo: 100 MB)
```
**Solução**: Aumente `max_file_size_mb` no config.yaml

### Validação falhou
```
SpedValidationError: Campos obrigatórios vazios: IND_EMIT, NUM_DOC
```
**Solução**: Corrija os dados ou desabilite `strict_mode` no config.yaml

## 📄 Licença

Este projeto é de uso interno. Todos os direitos reservados.

## 👥 Suporte

Para dúvidas ou problemas, consulte os logs gerados durante o processamento ou execute com `--log-level DEBUG` para mais detalhes.

## 🔄 Changelog

### v3.0 (Atual)
- ✨ Exceções customizadas para melhor tratamento de erros
- ✨ Sistema de validação completo (CNPJ, datas, campos)
- ✨ Métricas detalhadas de processamento
- ✨ Configuração via arquivo YAML
- ✨ GUI com barra de progresso e processamento assíncrono
- ✨ Suite de testes unitários
- ⚡ Otimizações de performance
- 📝 Documentação aprimorada

### v2.0
- Suporte a múltiplos blocos SPED
- Consolidação de registros pai-filho
- Interface gráfica básica

### v1.0
- Versão inicial com funcionalidades básicas
