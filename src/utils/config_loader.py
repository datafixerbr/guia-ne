"""
Módulo para carregamento de configurações.
"""

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from src.utils.exceptions import ConfigurationError


@dataclass
class OCIConfig:
    """Configuração para OCI."""

    config_path: str
    namespace: str
    bucket_name: str
    compartment_id: str


@dataclass
class ProcessingConfig:
    """Configuração para processamento."""

    ssd_path: str
    temp_directory: str
    # Tornando os campos opcionais com um valor padrão
    batch_size: Optional[int] = None
    max_workers: Optional[int] = None


@dataclass
class LoggingConfig:
    """Configuração de logging."""

    level: str
    file: str


@dataclass
class AppConfig:
    """Configuração principal da aplicação."""

    oci: OCIConfig
    processing: ProcessingConfig
    logging: LoggingConfig


class ConfigLoader:
    """Carregador de configurações."""

    @staticmethod
    def load_from_yaml(config_path: str) -> AppConfig:
        """
        Carrega configuração de arquivo YAML.

        Args:
            config_path: Caminho do arquivo de configuração

        Returns:
            AppConfig com configurações carregadas
        """
        try:
            config_file = Path(config_path)
            if not config_file.exists():
                raise ConfigurationError(
                    f"Arquivo de configuração não encontrado: {config_path}"
                )

            with open(config_file, "r", encoding="utf-8") as f:
                config_data = yaml.safe_load(f)

            # Expandir variáveis de ambiente
            config_data = ConfigLoader._expand_env_variables(config_data)

            # Criar objetos de configuração
            oci_config = OCIConfig(**config_data["oci"])
            processing_config = ProcessingConfig(**config_data["processing"])
            logging_config = LoggingConfig(
                **config_data.get("logging", {"level": "INFO", "file": "app.log"})
            )

            app_config = AppConfig(
                oci=oci_config, processing=processing_config, logging=logging_config
            )

            return app_config
        except Exception as e:
            # A mensagem de erro original será mais informativa aqui
            raise ConfigurationError(f"Erro ao carregar configuração: {str(e)}")

    @staticmethod
    def _expand_env_variables(config_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Expande variáveis de ambiente nas configurações.
        Suporta os formatos:
        - ${VAR_NAME}
        - $VAR_NAME
        """

        def expand_value(value):
            if isinstance(value, str):
                # Padrão ${VAR_NAME}
                if value.startswith("${") and value.endswith("}"):
                    env_var = value[2:-1]
                    return os.getenv(env_var, value)
                # Padrão $VAR_NAME (compatibilidade)
                elif value.startswith("$"):
                    env_var = value[1:]
                    return os.getenv(env_var, value)
                # Expansão inline múltipla: "path/${VAR}/subdir"
                else:
                    pattern = r"\\$\\{([^}]+)\\}"
                    matches = re.findall(pattern, value)
                    result = value
                    for var in matches:
                        result = result.replace(
                            f"${{{var}}}", os.getenv(var, f"${{{var}}}")
                        )
                    return result
            elif isinstance(value, dict):
                return {k: expand_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [expand_value(item) for item in value]
            return value

        return expand_value(config_data)

    @staticmethod
    def validate_config(config: AppConfig) -> None:
        """
        Valida configurações carregadas.

        Args:
            config: Configuração a ser validada
        """
        # Validar caminhos
        ssd_path = Path(config.processing.ssd_path)
        if not ssd_path.exists():
            raise ConfigurationError(
                f"Diretório SSD não encontrado: {config.processing.ssd_path}"
            )

        oci_config_path = Path(config.oci.config_path).expanduser()
        if not oci_config_path.exists():
            raise ConfigurationError(
                f"Arquivo de configuração OCI não encontrado: {config.oci.config_path}"
            )

        # Validar valores numéricos apenas se existirem (para modo batch)
        if (
            config.processing.batch_size is not None
            and config.processing.batch_size <= 0
        ):
            raise ConfigurationError("batch_size deve ser maior que zero")

        if (
            config.processing.max_workers is not None
            and config.processing.max_workers <= 0
        ):
            raise ConfigurationError("max_workers deve ser maior que zero")
