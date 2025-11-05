"""
Módulo principal para processamento streaming de arquivos ZIP.
"""

import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

from src.core.file_manager import FileManager
from src.core.lattes_zip_processor import (
    StreamExtractionResult,
    StreamingZipProcessor,
    XMLFileStream,
)
from src.core.oci_client import StreamingOCIClient, StreamUploadResult
from src.utils.exceptions import BatchProcessingError
from src.utils.logger import get_logger


@dataclass
class StreamingBatchResult:
    """Resultado do processamento streaming em lote."""

    zip_filename: str
    status: str
    xml_files_processed: int
    uploaded_objects: List[str]
    total_bytes_uploaded: int
    processing_time_seconds: float
    error_message: Optional[str] = None


class StreamingBatchProcessor:
    """Processador em lote streaming para arquivos ZIP e upload para OCI."""

    def __init__(
        self,
        zip_processor: StreamingZipProcessor,
        oci_client: StreamingOCIClient,
        file_manager: FileManager,
    ):
        """
        Inicializa o processador streaming.

        Args:
            zip_processor: Processador de ZIP streaming
            oci_client: Cliente OCI streaming
            file_manager: Gerenciador de arquivos
        """
        self.zip_processor = zip_processor
        self.oci_client = oci_client
        self.file_manager = file_manager
        self.logger = get_logger(__name__)

    def process_single_file_streaming(
        self, zip_filename: str, object_prefix: str = "cnpq_lattes"
    ) -> StreamingBatchResult:
        """
        Processa um único arquivo ZIP usando streaming.

        Args:
            zip_filename: Nome do arquivo ZIP
            object_prefix: Prefixo para objetos no OCI

        Returns:
            StreamingBatchResult com resultado do processamento
        """
        start_time = time.time()
        result = StreamingBatchResult(
            zip_filename=zip_filename,
            status="ERRO",
            xml_files_processed=0,
            uploaded_objects=[],
            total_bytes_uploaded=0,
            processing_time_seconds=0.0,
        )

        try:
            self.logger.info(f"Iniciando processamento streaming de: {zip_filename}")

            # Verificar se há arquivos XML no ZIP
            extraction_info = self.zip_processor.extract_xml_files_streaming(
                zip_filename
            )

            if not extraction_info.success:
                result.error_message = extraction_info.error_message
                return result

            # Processar arquivos XML via streaming
            zip_base_name = zip_filename.replace(".zip", "")
            xml_files_generator = self.zip_processor.stream_xml_files(zip_filename)

            upload_results = []
            for xml_file in xml_files_generator:
                object_name = f"{object_prefix}/{zip_base_name}/{xml_file.filename}"

                upload_result = self.oci_client.upload_from_bytes(
                    xml_file.content, object_name
                )
                upload_results.append(upload_result)

                if upload_result.success:
                    result.uploaded_objects.append(object_name)
                    result.xml_files_processed += 1
                    result.total_bytes_uploaded += upload_result.size_bytes

                    self.logger.debug(
                        f"Upload streaming concluído: {xml_file.filename}"
                    )
                else:
                    self.logger.warning(
                        f"Falha no upload: {xml_file.filename} - {upload_result.error_message}"
                    )

            # Determinar status final
            total_uploads = len(upload_results)
            successful_uploads = len([r for r in upload_results if r.success])

            if successful_uploads == total_uploads and successful_uploads > 0:
                result.status = "SUCESSO"
            elif successful_uploads > 0:
                result.status = "SUCESSO_PARCIAL"
                failed_uploads = [
                    r.object_name for r in upload_results if not r.success
                ]
                result.error_message = (
                    f"Falhas em {len(failed_uploads)} uploads: {failed_uploads[:3]}"
                )
            else:
                result.status = "ERRO"
                result.error_message = "Nenhum arquivo foi enviado com sucesso"

            result.processing_time_seconds = time.time() - start_time

            self.logger.info(
                f"Processamento streaming concluído: {zip_filename} - "
                f"Status: {result.status} - "
                f"XMLs: {result.xml_files_processed} - "
                f"Bytes: {result.total_bytes_uploaded} - "
                f"Tempo: {result.processing_time_seconds:.2f}s"
            )

        except Exception as e:
            result.error_message = str(e)
            result.processing_time_seconds = time.time() - start_time
            self.logger.error(
                f"Erro no processamento streaming de {zip_filename}: {str(e)}"
            )

        return result

    def process_batch_streaming(
        self,
        zip_files: List[str],
        object_prefix: str = "cnpq_lattes",
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> List[StreamingBatchResult]:
        """
        Processa lote de arquivos usando streaming (sem paralelização para economizar memória).

        Args:
            zip_files: Lista de arquivos ZIP
            object_prefix: Prefixo para objetos no OCI
            progress_callback: Função de callback para progresso

        Returns:
            Lista de StreamingBatchResult
        """
        results = []
        total_files = len(zip_files)

        self.logger.info(
            f"Iniciando processamento streaming em lote de {total_files} arquivos"
        )

        # Processar sequencialmente para minimizar uso de memória
        for i, zip_file in enumerate(zip_files, 1):
            try:
                self.logger.info(
                    f"[{i}/{total_files}] Processando streaming: {zip_file}"
                )

                result = self.process_single_file_streaming(zip_file, object_prefix)
                results.append(result)

                if progress_callback:
                    progress_callback(i, total_files)

            except Exception as e:
                error_result = StreamingBatchResult(
                    zip_filename=zip_file,
                    status="ERRO",
                    xml_files_processed=0,
                    uploaded_objects=[],
                    total_bytes_uploaded=0,
                    processing_time_seconds=0.0,
                    error_message=str(e),
                )
                results.append(error_result)
                self.logger.error(
                    f"Erro fatal no processamento streaming de {zip_file}: {str(e)}"
                )

        # Salvar resultados
        results_dict = [self._streaming_result_to_dict(result) for result in results]
        self.file_manager.save_processing_results(results_dict)

        self.logger.info(
            f"Processamento streaming concluído: {len(results)} arquivos processados"
        )
        return results

    def _streaming_result_to_dict(self, result: StreamingBatchResult) -> Dict:
        """Converte StreamingBatchResult para dicionário."""
        return {
            "zip_filename": result.zip_filename,
            "status": result.status,
            "xml_files_processed": result.xml_files_processed,
            "uploaded_objects_count": len(result.uploaded_objects),
            "uploaded_objects": ",".join(result.uploaded_objects),
            "total_bytes_uploaded": result.total_bytes_uploaded,
            "processing_time_seconds": result.processing_time_seconds,
            "error_message": result.error_message,
        }

    def get_streaming_batch_summary(self, results: List[StreamingBatchResult]) -> Dict:
        """
        Gera resumo dos resultados do lote streaming.

        Args:
            results: Lista de resultados

        Returns:
            Dicionário com resumo
        """
        if not results:
            return {"total_files": 0}

        successful = [r for r in results if r.status in ["SUCESSO", "SUCESSO_PARCIAL"]]
        failed = [r for r in results if r.status == "ERRO"]

        total_xml_files = sum(r.xml_files_processed for r in results)
        total_bytes = sum(r.total_bytes_uploaded for r in results)
        total_processing_time = sum(r.processing_time_seconds for r in results)

        summary = {
            "total_files": len(results),
            "successful": len(successful),
            "failed": len(failed),
            "total_xml_files_processed": total_xml_files,
            "total_bytes_uploaded": total_bytes,
            "total_bytes_uploaded_mb": round(total_bytes / (1024 * 1024), 2),
            "total_processing_time_seconds": total_processing_time,
            "average_processing_time_per_file": total_processing_time / len(results),
            "success_rate_percent": (len(successful) / len(results)) * 100,
            "average_bytes_per_file": (
                total_bytes / len(results) if len(results) > 0 else 0
            ),
        }

        self.logger.info(f"Resumo do lote streaming: {summary}")
        return summary
