"""
Módulo responsável pelo processamento em lote de arquivos ZIP e upload direto para OCI.
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

from src.core.file_manager import FileManager
from src.core.lattes_zip_processor import StreamingZipProcessor, XMLFileStream
from src.core.oci_client import StreamingOCIClient, StreamUploadResult
from src.utils.exceptions import BatchProcessingError
from src.utils.logger import get_logger


@dataclass
class BatchResult:
    """Resultado do processamento em lote."""

    zip_filename: str
    status: str
    xml_files_processed: int
    uploaded_objects: List[str]
    processing_time_seconds: float
    error_message: Optional[str] = None


class BatchProcessor:
    """Processador em lote para arquivos ZIP e upload streaming para OCI."""

    def __init__(
        self,
        zip_processor: StreamingZipProcessor,
        oci_client: StreamingOCIClient,
        file_manager: FileManager,
        max_workers: int = 4,
    ):
        """
        Inicializa o processador em lote.

        Args:
            zip_processor: Instância do processador streaming ZIP
            oci_client: Cliente OCI streaming
            file_manager: Gerenciador de arquivos
            max_workers: Número máximo de threads paralelas
        """
        self.zip_processor = zip_processor
        self.oci_client = oci_client
        self.file_manager = file_manager
        self.max_workers = max_workers
        self.logger = get_logger(__name__)

    def process_single_file_streaming(
        self, zip_filename: str, object_prefix: str = "cnpq_lattes"
    ) -> BatchResult:
        """
        Processa um único arquivo ZIP via streaming.

        Args:
            zip_filename: Nome do arquivo ZIP
            object_prefix: Prefixo para objetos no OCI

        Returns:
            BatchResult com resultado do processamento
        """
        start_time = time.time()
        result = BatchResult(
            zip_filename=zip_filename,
            status="ERRO",
            xml_files_processed=0,
            uploaded_objects=[],
            processing_time_seconds=0.0,
        )

        try:
            self.logger.info(f"Iniciando processamento streaming de: {zip_filename}")

            # Gera os XMLs diretamente da memória do ZIP
            xml_generator = self.zip_processor.stream_xml_files(zip_filename)
            zip_base_name = zip_filename.replace(".zip", "")
            uploaded_objects = []
            upload_failures = []

            for xml_file in xml_generator:
                object_name = f"{object_prefix}/{zip_base_name}/{xml_file.filename}"
                upload_result = self.oci_client.upload_from_bytes(
                    xml_file.content, object_name
                )

                if upload_result.success:
                    uploaded_objects.append(object_name)
                    result.xml_files_processed += 1
                else:
                    upload_failures.append(
                        f"{xml_file.filename}: {upload_result.error_message}"
                    )

            # Determina status final
            if result.xml_files_processed > 0:
                result.status = "SUCESSO" if not upload_failures else "SUCESSO_PARCIAL"
                result.uploaded_objects = uploaded_objects
                if upload_failures:
                    result.error_message = (
                        f"Falhas em uploads: {'; '.join(upload_failures)}"
                    )
            else:
                result.status = "ERRO"
                result.error_message = "Nenhum arquivo foi processado com sucesso"

            result.processing_time_seconds = time.time() - start_time

            self.logger.info(
                f"Processamento streaming concluído: {zip_filename} - Status: {result.status} - "
                f"XMLs: {result.xml_files_processed} - Tempo: {result.processing_time_seconds:.2f}s"
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
    ) -> List[BatchResult]:
        """
        Processa um lote de arquivos ZIP via streaming.

        Args:
            zip_files: Lista de arquivos ZIP
            object_prefix: Prefixo para objetos no OCI
            progress_callback: Função de callback para progresso (atual, total)

        Returns:
            Lista de BatchResult
        """
        results = []
        total_files = len(zip_files)

        self.logger.info(
            f"Iniciando processamento streaming em lote de {total_files} arquivos"
        )

        # Ajuste: para processamento massivo e baixo consumo, pode usar sequencial ou ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_filename = {
                executor.submit(
                    self.process_single_file_streaming, zip_file, object_prefix
                ): zip_file
                for zip_file in zip_files
            }

            processed_count = 0
            for future in as_completed(future_to_filename):
                zip_filename = future_to_filename[future]

                try:
                    result = future.result()
                    results.append(result)
                    processed_count += 1

                    if progress_callback:
                        progress_callback(processed_count, total_files)

                    self.logger.info(
                        f"[{processed_count}/{total_files}] Processado: {zip_filename}"
                    )

                except Exception as e:
                    error_result = BatchResult(
                        zip_filename=zip_filename,
                        status="ERRO",
                        xml_files_processed=0,
                        uploaded_objects=[],
                        processing_time_seconds=0.0,
                        error_message=str(e),
                    )
                    results.append(error_result)
                    processed_count += 1

                    self.logger.error(
                        f"Erro fatal no processamento de {zip_filename}: {str(e)}"
                    )

        results_dict = [self._batch_result_to_dict(result) for result in results]
        self.file_manager.save_processing_results(results_dict)

        self.logger.info(
            f"Processamento streaming concluído: {len(results)} arquivos processados"
        )
        return results

    def _batch_result_to_dict(self, result: BatchResult) -> Dict:
        """Converte BatchResult para dicionário."""
        return {
            "zip_filename": result.zip_filename,
            "status": result.status,
            "xml_files_processed": result.xml_files_processed,
            "uploaded_objects_count": len(result.uploaded_objects),
            "uploaded_objects": ",".join(result.uploaded_objects),
            "processing_time_seconds": result.processing_time_seconds,
            "error_message": result.error_message,
        }

    def get_batch_summary(self, results: List[BatchResult]) -> Dict:
        """
        Gera resumo dos resultados do lote.

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
        total_processing_time = sum(r.processing_time_seconds for r in results)

        summary = {
            "total_files": len(results),
            "successful": len(successful),
            "failed": len(failed),
            "total_xml_files_processed": total_xml_files,
            "total_processing_time_seconds": total_processing_time,
            "average_processing_time_per_file": (
                total_processing_time / len(results) if len(results) else 0.0
            ),
            "success_rate_percent": (
                (len(successful) / len(results)) * 100 if len(results) else 0.0
            ),
        }

        self.logger.info(f"Resumo do lote: {summary}")
        return summary
