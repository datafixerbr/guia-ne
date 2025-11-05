"""
Módulo de validadores.
"""

import re
from pathlib import Path
from typing import List, Optional


class FileValidator:
    """Validador de arquivos."""

    @staticmethod
    def validate_zip_filename(filename: str) -> bool:
        """
        Valida o nome de um arquivo ZIP de forma flexível.

        Args:
            filename: O nome do arquivo a ser validado.

        Returns:
            True se o nome do arquivo for um .zip válido, False caso contrário.
        """
        # Garante que estamos lidando apenas com o nome do arquivo, não o caminho completo
        file_name_only = Path(filename).name

        # A validação agora é simples: o nome não pode ser vazio e deve terminar com .zip
        if not file_name_only:
            return False

        return file_name_only.lower().endswith(".zip")

    @staticmethod
    def validate_xml_filename(filename: str) -> bool:
        """
        Valida nome de arquivo XML.

        Args:
            filename: Nome do arquivo

        Returns:
            True se válido, False caso contrário
        """
        if not filename.endswith(".xml"):
            return False

        # Validar padrão de nome XML baseado no notebook
        pattern = r"^\d+\.xml$"
        return bool(re.match(pattern, filename))

    @staticmethod
    def filter_valid_zip_files(filenames: List[str]) -> List[str]:
        """
        Filtra apenas arquivos ZIP válidos.

        Args:
            filenames: Lista de nomes de arquivos

        Returns:
            Lista filtrada de arquivos ZIP válidos
        """
        return [
            filename
            for filename in filenames
            if FileValidator.validate_zip_filename(filename)
        ]


class OCIValidator:
    """Validador de configurações OCI."""

    @staticmethod
    def validate_object_name(object_name: str) -> bool:
        """
        Valida nome de objeto OCI.

        Args:
            object_name: Nome do objeto

        Returns:
            True se válido, False caso contrário
        """
        # OCI object names têm algumas restrições
        if not object_name:
            return False

        # Não pode começar ou terminar com '/'
        if object_name.startswith("/") or object_name.endswith("/"):
            return False

        # Caracteres permitidos (simplificado)
        allowed_chars = re.compile(r"^[a-zA-Z0-9._\-/]+$")
        return bool(allowed_chars.match(object_name))

    @staticmethod
    def sanitize_object_name(object_name: str) -> str:
        """
        Sanitiza nome de objeto para OCI.

        Args:
            object_name: Nome original

        Returns:
            Nome sanitizado
        """
        # Remover caracteres especiais
        sanitized = re.sub(r"[^a-zA-Z0-9._\-/]", "_", object_name)

        # Remover barras duplas
        sanitized = re.sub(r"/+", "/", sanitized)

        # Remover barras no início e fim
        sanitized = sanitized.strip("/")

        return sanitized
