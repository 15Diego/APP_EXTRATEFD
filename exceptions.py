"""
Exceções customizadas para o processamento de arquivos SPED.

Este módulo define exceções específicas do domínio SPED para melhorar
o tratamento de erros e facilitar o debugging.
"""


class SpedError(Exception):
    """Classe base para todas as exceções SPED."""
    pass


class SpedParseError(SpedError):
    """
    Erro ao fazer parsing de linhas SPED.
    
    Levantada quando uma linha do arquivo SPED está malformada ou
    não pode ser processada corretamente.
    """
    
    def __init__(self, message: str, line_number: int = None, line_content: str = None):
        self.line_number = line_number
        self.line_content = line_content
        
        if line_number:
            message = f"Linha {line_number}: {message}"
        if line_content:
            preview = line_content[:100] + "..." if len(line_content) > 100 else line_content
            message = f"{message}\nConteúdo: {preview}"
        
        super().__init__(message)


class SpedValidationError(SpedError):
    """
    Erro de validação de dados SPED.
    
    Levantada quando os dados do SPED não passam nas validações
    de integridade, formato ou regras de negócio.
    """
    
    def __init__(self, message: str, registro: str = None, campo: str = None, valor: str = None):
        self.registro = registro
        self.campo = campo
        self.valor = valor
        
        details = []
        if registro:
            details.append(f"Registro: {registro}")
        if campo:
            details.append(f"Campo: {campo}")
        if valor:
            details.append(f"Valor: {valor}")
        
        if details:
            message = f"{message} ({', '.join(details)})"
        
        super().__init__(message)


class SpedFileError(SpedError):
    """
    Erro relacionado a arquivos SPED.
    
    Levantada quando há problemas ao abrir, ler ou validar
    o arquivo SPED (tamanho, encoding, permissões, etc).
    """
    
    def __init__(self, message: str, file_path: str = None):
        self.file_path = file_path
        
        if file_path:
            message = f"{message}: {file_path}"
        
        super().__init__(message)


class SpedEncodingError(SpedFileError):
    """
    Erro ao detectar ou decodificar o encoding do arquivo SPED.
    
    Levantada quando não é possível determinar o encoding correto
    ou quando há erros de decodificação.
    """
    pass


class SpedIntegrityError(SpedValidationError):
    """
    Erro de integridade referencial nos dados SPED.
    
    Levantada quando há inconsistências entre registros relacionados,
    como totais que não batem ou referências inválidas.
    """
    
    def __init__(self, message: str, parent_registro: str = None, child_registro: str = None):
        self.parent_registro = parent_registro
        self.child_registro = child_registro
        
        if parent_registro and child_registro:
            message = f"{message} (Pai: {parent_registro}, Filho: {child_registro})"
        
        super().__init__(message)
