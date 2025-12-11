"""
Módulo de validação de dados SPED.

Contém funções para validar CNPJs, datas, campos obrigatórios e
integridade referencial dos dados SPED.
"""

import re
from datetime import datetime
from typing import List, Optional, Dict, Any
import pandas as pd

from exceptions import SpedValidationError, SpedIntegrityError


def validate_cnpj(cnpj: str) -> bool:
    """
    Valida um CNPJ brasileiro.
    
    Verifica formato e dígitos verificadores do CNPJ.
    
    Args:
        cnpj: String contendo o CNPJ (com ou sem formatação)
        
    Returns:
        True se o CNPJ é válido, False caso contrário
        
    Example:
        >>> validate_cnpj('11.222.333/0001-81')
        True
        >>> validate_cnpj('00.000.000/0000-00')
        False
    """
    if not cnpj:
        return False
    
    # Remove caracteres não numéricos
    cnpj = re.sub(r'[^0-9]', '', cnpj)
    
    # Verifica se tem 14 dígitos
    if len(cnpj) != 14:
        return False
    
    # Verifica se não é uma sequência de números iguais
    if cnpj == cnpj[0] * 14:
        return False
    
    # Calcula primeiro dígito verificador
    soma = 0
    peso = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    for i in range(12):
        soma += int(cnpj[i]) * peso[i]
    
    resto = soma % 11
    digito1 = 0 if resto < 2 else 11 - resto
    
    if int(cnpj[12]) != digito1:
        return False
    
    # Calcula segundo dígito verificador
    soma = 0
    peso = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    for i in range(13):
        soma += int(cnpj[i]) * peso[i]
    
    resto = soma % 11
    digito2 = 0 if resto < 2 else 11 - resto
    
    return int(cnpj[13]) == digito2


def validate_date_format(date_str: str, format_str: str) -> bool:
    """
    Valida se uma string está em um formato de data específico.
    
    Args:
        date_str: String contendo a data
        format_str: Formato esperado (ex: '%Y%m%d', '%d%m%Y')
        
    Returns:
        True se a data é válida no formato especificado
        
    Example:
        >>> validate_date_format('20231225', '%Y%m%d')
        True
        >>> validate_date_format('32012023', '%d%m%Y')
        False
    """
    if not date_str or not date_str.strip():
        return True  # Datas vazias são permitidas
    
    try:
        datetime.strptime(date_str, format_str)
        return True
    except (ValueError, TypeError):
        return False


def validate_numeric_field(value: str, allow_empty: bool = True) -> bool:
    """
    Valida se um campo contém um valor numérico válido no formato brasileiro.
    
    Args:
        value: Valor a validar
        allow_empty: Se True, permite valores vazios
        
    Returns:
        True se o valor é numérico válido ou vazio (se permitido)
        
    Example:
        >>> validate_numeric_field('1.234,56')
        True
        >>> validate_numeric_field('abc')
        False
    """
    if not value or not value.strip():
        return allow_empty
    
    # Formato brasileiro: 1.234,56 ou 1234,56
    pattern = r'^-?\d{1,3}(\.\d{3})*,\d{2}$|^-?\d+,\d{2}$|^-?\d+$'
    return bool(re.match(pattern, value.strip()))


# Campos obrigatórios por tipo de registro
REQUIRED_FIELDS = {
    'C100': ['IND_OPER', 'IND_EMIT', 'COD_PART', 'COD_MOD', 'NUM_DOC', 'DT_DOC'],
    'C170': ['NUM_ITEM', 'COD_ITEM', 'QTD', 'VL_ITEM'],
    'D100': ['IND_OPER', 'IND_EMIT', 'COD_PART', 'COD_MOD', 'NUM_DOC', 'DT_DOC'],
    'A100': ['IND_OPER', 'IND_EMIT', 'COD_PART', 'NUM_DOC', 'DT_DOC'],
    'F100': ['IND_OPER', 'COD_PART', 'DT_OPER'],
    'C500': ['COD_PART', 'COD_MOD', 'NUM_DOC', 'DT_DOC'],
    'D500': ['IND_OPER', 'IND_EMIT', 'COD_PART', 'COD_MOD', 'NUM_DOC', 'DT_DOC'],
    'D700': ['IND_OPER', 'IND_EMIT', 'COD_PART', 'COD_MOD', 'NUM_DOC', 'DT_DOC'],
}


def validate_registro(registro: str, fields: Dict[str, str], strict: bool = False) -> List[str]:
    """
    Valida se um registro possui todos os campos obrigatórios preenchidos.
    
    Args:
        registro: Código do registro (ex: 'C100')
        fields: Dicionário com nome do campo e valor
        strict: Se True, levanta exceção em caso de erro
        
    Returns:
        Lista de campos obrigatórios que estão vazios
        
    Raises:
        SpedValidationError: Se strict=True e houver campos vazios
        
    Example:
        >>> fields = {'IND_OPER': '0', 'IND_EMIT': '', 'NUM_DOC': '123'}
        >>> validate_registro('C100', fields)
        ['IND_EMIT', 'COD_PART', 'COD_MOD', 'DT_DOC']
    """
    if registro not in REQUIRED_FIELDS:
        return []
    
    missing_fields = []
    for field_name in REQUIRED_FIELDS[registro]:
        value = fields.get(field_name, '')
        if not value or not str(value).strip():
            missing_fields.append(field_name)
    
    if strict and missing_fields:
        raise SpedValidationError(
            f"Campos obrigatórios vazios: {', '.join(missing_fields)}",
            registro=registro
        )
    
    return missing_fields


def validate_cross_reference_totals(
    parent_df: pd.DataFrame,
    child_df: pd.DataFrame,
    parent_index: str,
    parent_total_col: str,
    child_value_col: str,
    tolerance: float = 0.01
) -> List[Dict[str, Any]]:
    """
    Valida se a soma dos valores dos registros filhos bate com o total do pai.
    
    Args:
        parent_df: DataFrame com registros pai
        child_df: DataFrame com registros filhos
        parent_index: Nome da coluna de índice do pai
        parent_total_col: Nome da coluna com o total no registro pai
        child_value_col: Nome da coluna com valores no registro filho
        tolerance: Tolerância para diferenças (padrão: R$ 0,01)
        
    Returns:
        Lista de dicionários com divergências encontradas
        
    Example:
        >>> divergencias = validate_cross_reference_totals(
        ...     df_c100, df_c170, 'C100_INDEX', 'VL_MERC', 'VL_ITEM'
        ... )
        >>> if divergencias:
        ...     print(f"Encontradas {len(divergencias)} divergências")
    """
    if parent_df.empty or child_df.empty:
        return []
    
    if parent_total_col not in parent_df.columns:
        return []
    
    if child_value_col not in child_df.columns:
        return []
    
    # Agrupa filhos por índice do pai
    child_totals = child_df.groupby(parent_index)[child_value_col].sum()
    
    divergencias = []
    for idx, row in parent_df.iterrows():
        parent_idx_value = row[parent_index]
        parent_total = row[parent_total_col]
        
        if pd.isna(parent_total):
            continue
        
        child_total = child_totals.get(parent_idx_value, 0)
        
        diff = abs(parent_total - child_total)
        if diff > tolerance:
            divergencias.append({
                'index': parent_idx_value,
                'parent_total': parent_total,
                'child_total': child_total,
                'difference': diff,
                'registro_pai': row.get('REG', 'Unknown'),
                'num_doc': row.get('NUM_DOC', 'Unknown')
            })
    
    return divergencias


def validate_chave_nfe(chave: str) -> bool:
    """
    Valida formato de chave de NFe (44 dígitos).
    
    Args:
        chave: Chave da NFe
        
    Returns:
        True se a chave tem formato válido
        
    Example:
        >>> validate_chave_nfe('35231234567890123456789012345678901234567890')
        True
        >>> validate_chave_nfe('123')
        False
    """
    if not chave or not chave.strip():
        return True  # Chaves vazias são permitidas
    
    # Remove espaços e caracteres não numéricos
    chave = re.sub(r'[^0-9]', '', chave)
    
    # Deve ter exatamente 44 dígitos
    return len(chave) == 44


def validate_cfop(cfop: str) -> bool:
    """
    Valida código CFOP (4 dígitos).
    
    Args:
        cfop: Código CFOP
        
    Returns:
        True se o CFOP tem formato válido
        
    Example:
        >>> validate_cfop('5102')
        True
        >>> validate_cfop('999')
        False
    """
    if not cfop or not cfop.strip():
        return True
    
    # Remove espaços
    cfop = cfop.strip()
    
    # Deve ter 4 dígitos numéricos
    return bool(re.match(r'^\d{4}$', cfop))
