"""
Exceções customizadas do projeto.
"""


class CNPQProcessingError(Exception):
    """Exceção base para erros de processamento."""

    pass


class ConfigurationError(CNPQProcessingError):
    """Erro de configuração."""

    pass


class ZipProcessingError(CNPQProcessingError):
    """Erro no processamento de arquivos ZIP."""

    pass


class FileNotFoundError(CNPQProcessingError):
    """Arquivo não encontrado."""

    pass


class OCIConnectionError(CNPQProcessingError):
    """Erro de conexão com OCI."""

    pass


class OCIUploadError(CNPQProcessingError):
    """Erro no upload para OCI."""

    pass


class MetadataError(CNPQProcessingError):
    """Erro no processamento de metadados."""

    pass


class BatchProcessingError(CNPQProcessingError):
    """Erro no processamento em lote."""

    pass
