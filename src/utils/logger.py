"""
Módulo de configuração de logging.
"""

import logging
import sys
from pathlib import Path
from typing import Optional


def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    log_format: Optional[str] = None,
) -> None:
    """
    Configura o sistema de logging.

    Args:
        log_level: Nível de logging (DEBUG, INFO, WARNING, ERROR)
        log_file: Arquivo de log (opcional)
        log_format: Formato das mensagens de log
    """
    if log_format is None:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Configurar nível de logging
    level = getattr(logging, log_level.upper(), logging.INFO)

    # Configurar handlers
    handlers = [logging.StreamHandler(sys.stdout)]

    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    # Configurar logging básico
    logging.basicConfig(level=level, format=log_format, handlers=handlers, force=True)

    # Configurar loggers de bibliotecas externas
    logging.getLogger("oci").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """
    Retorna logger configurado.

    Args:
        name: Nome do logger

    Returns:
        Logger configurado
    """
    return logging.getLogger(name)
