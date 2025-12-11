"""
Módulo de métricas de processamento SPED.

Rastreia estatísticas e métricas durante o processamento de arquivos SPED
para monitoramento, debugging e otimização.
"""

import time
from dataclasses import dataclass, field
from typing import Dict, List
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


@dataclass
class ProcessingMetrics:
    """
    Métricas de processamento de arquivos SPED.
    
    Attributes:
        total_lines: Total de linhas no arquivo
        processed_lines: Linhas processadas com sucesso
        error_lines: Linhas com erro
        skipped_lines: Linhas ignoradas (vazias, comentários)
        registros_por_tipo: Contador de registros por tipo
        erros_por_tipo: Contador de erros por tipo
        tempo_inicio: Timestamp de início do processamento
        tempo_fim: Timestamp de fim do processamento
        arquivo_processado: Nome do arquivo sendo processado
    """
    
    total_lines: int = 0
    processed_lines: int = 0
    error_lines: int = 0
    skipped_lines: int = 0
    registros_por_tipo: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    erros_por_tipo: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    warnings: List[str] = field(default_factory=list)
    tempo_inicio: float = field(default_factory=time.time)
    tempo_fim: float = 0.0
    arquivo_processado: str = ""
    
    def increment_registro(self, tipo_registro: str) -> None:
        """
        Incrementa contador de um tipo de registro.
        
        Args:
            tipo_registro: Código do registro (ex: 'C100')
        """
        self.registros_por_tipo[tipo_registro] += 1
        self.processed_lines += 1
    
    def increment_erro(self, tipo_erro: str = "Genérico") -> None:
        """
        Incrementa contador de erros.
        
        Args:
            tipo_erro: Tipo ou descrição do erro
        """
        self.erros_por_tipo[tipo_erro] += 1
        self.error_lines += 1
    
    def add_warning(self, warning: str) -> None:
        """
        Adiciona um aviso à lista de warnings.
        
        Args:
            warning: Mensagem de aviso
        """
        self.warnings.append(warning)
        if len(self.warnings) > 100:  # Limita a 100 warnings
            self.warnings.pop(0)
    
    def finalizar(self) -> None:
        """Marca o fim do processamento e registra timestamp."""
        self.tempo_fim = time.time()
    
    @property
    def tempo_processamento(self) -> float:
        """
        Retorna tempo de processamento em segundos.
        
        Returns:
            Tempo decorrido em segundos
        """
        fim = self.tempo_fim if self.tempo_fim > 0 else time.time()
        return fim - self.tempo_inicio
    
    @property
    def taxa_sucesso(self) -> float:
        """
        Retorna taxa de sucesso do processamento.
        
        Returns:
            Percentual de linhas processadas com sucesso (0-100)
        """
        if self.total_lines == 0:
            return 0.0
        return (self.processed_lines / self.total_lines) * 100
    
    @property
    def linhas_por_segundo(self) -> float:
        """
        Retorna taxa de processamento em linhas por segundo.
        
        Returns:
            Número de linhas processadas por segundo
        """
        tempo = self.tempo_processamento
        if tempo == 0:
            return 0.0
        return self.processed_lines / tempo
    
    def get_top_registros(self, n: int = 10) -> List[tuple]:
        """
        Retorna os N tipos de registro mais frequentes.
        
        Args:
            n: Número de registros a retornar
            
        Returns:
            Lista de tuplas (tipo_registro, quantidade) ordenada por quantidade
        """
        return sorted(
            self.registros_por_tipo.items(),
            key=lambda x: x[1],
            reverse=True
        )[:n]
    
    def log_summary(self) -> None:
        """Registra resumo das métricas no log."""
        logger.info("=" * 60)
        logger.info("RESUMO DO PROCESSAMENTO")
        logger.info("=" * 60)
        
        if self.arquivo_processado:
            logger.info(f"Arquivo: {self.arquivo_processado}")
        
        logger.info(f"Total de linhas: {self.total_lines:,}")
        logger.info(f"Processadas com sucesso: {self.processed_lines:,}")
        logger.info(f"Linhas com erro: {self.error_lines:,}")
        logger.info(f"Linhas ignoradas: {self.skipped_lines:,}")
        logger.info(f"Taxa de sucesso: {self.taxa_sucesso:.2f}%")
        logger.info(f"Tempo de processamento: {self.tempo_processamento:.2f}s")
        logger.info(f"Velocidade: {self.linhas_por_segundo:.0f} linhas/segundo")
        
        if self.registros_por_tipo:
            logger.info("\nTop 10 Registros Processados:")
            for tipo, qtd in self.get_top_registros(10):
                logger.info(f"  {tipo}: {qtd:,}")
        
        if self.erros_por_tipo:
            logger.info("\nErros por Tipo:")
            for tipo, qtd in sorted(self.erros_por_tipo.items(), key=lambda x: x[1], reverse=True):
                logger.info(f"  {tipo}: {qtd:,}")
        
        if self.warnings:
            logger.info(f"\nTotal de avisos: {len(self.warnings)}")
            if len(self.warnings) <= 10:
                for warning in self.warnings:
                    logger.info(f"  - {warning}")
            else:
                logger.info("  (Mostrando últimos 5 avisos)")
                for warning in self.warnings[-5:]:
                    logger.info(f"  - {warning}")
        
        logger.info("=" * 60)
    
    def to_dict(self) -> Dict:
        """
        Converte métricas para dicionário.
        
        Returns:
            Dicionário com todas as métricas
        """
        return {
            'arquivo': self.arquivo_processado,
            'total_linhas': self.total_lines,
            'processadas': self.processed_lines,
            'erros': self.error_lines,
            'ignoradas': self.skipped_lines,
            'taxa_sucesso': f"{self.taxa_sucesso:.2f}%",
            'tempo_segundos': round(self.tempo_processamento, 2),
            'linhas_por_segundo': round(self.linhas_por_segundo, 0),
            'registros_por_tipo': dict(self.registros_por_tipo),
            'erros_por_tipo': dict(self.erros_por_tipo),
            'total_warnings': len(self.warnings)
        }
    
    def __str__(self) -> str:
        """Representação em string das métricas."""
        return (
            f"ProcessingMetrics("
            f"linhas={self.processed_lines}/{self.total_lines}, "
            f"erros={self.error_lines}, "
            f"tempo={self.tempo_processamento:.2f}s, "
            f"taxa={self.taxa_sucesso:.1f}%)"
        )
