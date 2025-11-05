"""
Módulo responsável pela comunicação streaming com Oracle Cloud Infrastructure (OCI).
"""

from dataclasses import dataclass
from io import BytesIO
from typing import Dict, List, Optional

import oci
from oci.exceptions import ServiceError
from oci.object_storage import ObjectStorageClient

from src.utils.exceptions import OCIConnectionError, OCIUploadError
from src.utils.logger import get_logger


@dataclass
class StreamUploadResult:
    """Resultado do upload streaming para OCI."""

    object_name: str
    success: bool
    size_bytes: int = 0
    error_message: Optional[str] = None


class StreamingOCIClient:
    """Cliente streaming para interação com OCI Object Storage."""

    def __init__(self, config_path: str, namespace: str, bucket_name: str):
        """
        Inicializa o cliente OCI streaming.

        Args:
            config_path: Caminho do arquivo de configuração OCI
            namespace: Namespace da tenancy OCI
            bucket_name: Nome do bucket
        """
        self.config_path = config_path
        self.namespace = namespace
        self.bucket_name = bucket_name
        self.logger = get_logger(__name__)

        try:
            self.config = oci.config.from_file(config_path)
            self.client = ObjectStorageClient(self.config)
            self.logger.info(
                f"Cliente OCI Streaming inicializado - Bucket: {bucket_name}"
            )

            # Verificar conectividade
            self._verify_connectivity()

        except Exception as e:
            raise OCIConnectionError(f"Erro ao inicializar cliente OCI: {str(e)}")

    def _verify_connectivity(self) -> None:
        """Verifica conectividade com OCI."""
        try:
            self.client.get_bucket(
                namespace_name=self.namespace, bucket_name=self.bucket_name
            )
            self.logger.info("Conectividade com OCI verificada com sucesso")
        except ServiceError as e:
            raise OCIConnectionError(f"Erro de conectividade OCI: {e.message}")

    def upload_from_bytes(self, content: bytes, object_name: str) -> StreamUploadResult:
        """
        Faz upload de conteúdo em bytes diretamente para OCI.

        Args:
            content: Conteúdo em bytes
            object_name: Nome do objeto no bucket

        Returns:
            StreamUploadResult com informações do upload
        """
        result = StreamUploadResult(object_name=object_name, success=False)

        try:
            if not content:
                result.error_message = "Conteúdo vazio"
                return result

            content_size = len(content)
            self.logger.debug(
                f"Iniciando upload streaming: {object_name} ({content_size} bytes)"
            )

            # Criar stream em memória
            content_stream = BytesIO(content)

            # Upload usando stream
            self.client.put_object(
                namespace_name=self.namespace,
                bucket_name=self.bucket_name,
                object_name=object_name,
                put_object_body=content_stream,
            )

            result.success = True
            result.size_bytes = content_size

            self.logger.info(f"Upload streaming realizado com sucesso: {object_name}")

        except ServiceError as e:
            result.error_message = f"Erro OCI: {e.message}"
            self.logger.error(f"Erro no upload streaming de {object_name}: {e.message}")
        except Exception as e:
            result.error_message = str(e)
            self.logger.error(
                f"Erro geral no upload streaming de {object_name}: {str(e)}"
            )

        return result

    def upload_from_stream(
        self, content_stream: BytesIO, object_name: str
    ) -> StreamUploadResult:
        """
        Faz upload de um stream diretamente para OCI.

        Args:
            content_stream: Stream de conteúdo
            object_name: Nome do objeto no bucket

        Returns:
            StreamUploadResult com informações do upload
        """
        result = StreamUploadResult(object_name=object_name, success=False)

        try:
            # Obter posição atual para calcular tamanho
            current_pos = content_stream.tell()
            content_stream.seek(0, 2)  # Ir para o final
            content_size = content_stream.tell()
            content_stream.seek(current_pos)  # Voltar para posição original

            self.logger.debug(
                f"Iniciando upload de stream: {object_name} ({content_size} bytes)"
            )

            # Upload usando stream
            self.client.put_object(
                namespace_name=self.namespace,
                bucket_name=self.bucket_name,
                object_name=object_name,
                put_object_body=content_stream,
            )

            result.success = True
            result.size_bytes = content_size

            self.logger.info(f"Upload de stream realizado com sucesso: {object_name}")

        except ServiceError as e:
            result.error_message = f"Erro OCI: {e.message}"
            self.logger.error(f"Erro no upload de stream de {object_name}: {e.message}")
        except Exception as e:
            result.error_message = str(e)
            self.logger.error(
                f"Erro geral no upload de stream de {object_name}: {str(e)}"
            )

        return result

    def batch_upload_from_generator(
        self, xml_files_generator, object_prefix: str, zip_base_name: str
    ) -> List[StreamUploadResult]:
        """
        Faz upload em lote de arquivos XML de um generator.

        Args:
            xml_files_generator: Generator de XMLFileStream
            object_prefix: Prefixo para objetos no OCI
            zip_base_name: Nome base do arquivo ZIP

        Returns:
            Lista de StreamUploadResult
        """
        results = []

        try:
            for xml_file in xml_files_generator:
                object_name = f"{object_prefix}/{zip_base_name}/{xml_file.filename}"

                result = self.upload_from_bytes(xml_file.content, object_name)
                results.append(result)

                if result.success:
                    self.logger.debug(
                        f"Upload concluído: {xml_file.filename} -> {object_name}"
                    )
                else:
                    self.logger.warning(
                        f"Falha no upload: {xml_file.filename} - {result.error_message}"
                    )

        except Exception as e:
            self.logger.error(f"Erro no upload em lote: {str(e)}")
            # Adicionar resultado de erro se não houver resultados ainda
            if not results:
                results.append(
                    StreamUploadResult(
                        object_name="batch_error", success=False, error_message=str(e)
                    )
                )

        return results

    def list_objects(self, prefix: str = "") -> List[Dict]:
        """
        Lista objetos no bucket.

        Args:
            prefix: Prefixo para filtrar objetos

        Returns:
            Lista de objetos
        """
        try:
            response = self.client.list_objects(
                namespace_name=self.namespace,
                bucket_name=self.bucket_name,
                prefix=prefix,
            )

            objects = []
            for obj in response.data.objects:
                objects.append(
                    {
                        "name": obj.name,
                        "size": obj.size,
                        "time_created": obj.time_created,
                    }
                )

            self.logger.info(f"Listados {len(objects)} objetos com prefixo '{prefix}'")
            return objects

        except ServiceError as e:
            self.logger.error(f"Erro ao listar objetos: {e.message}")
            return []

    def get_bucket_stats(self) -> Dict:
        """
        Retorna estatísticas do bucket.

        Returns:
            Dicionário com estatísticas
        """
        try:
            objects = self.list_objects()
            total_objects = len(objects)
            total_size = sum(obj["size"] for obj in objects)

            stats = {
                "total_objects": total_objects,
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
            }

            self.logger.info(f"Estatísticas do bucket: {stats}")
            return stats

        except Exception as e:
            self.logger.error(f"Erro ao obter estatísticas: {str(e)}")
            return {}
