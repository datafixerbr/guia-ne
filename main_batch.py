#!/usr/bin/env python3
"""
Script principal para processamento em LOTE de arquivos CNPq Lattes.
"""
import os
import sys
import time
from typing import Generator, List

from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

from src.core.batch_processor import BatchProcessor
from src.core.file_manager import FileManager
from src.core.lattes_zip_processor import StreamingZipProcessor
from src.core.oci_client import StreamingOCIClient
from src.utils.config_loader import ConfigLoader
from src.utils.logger import get_logger, setup_logging
from src.utils.validators import FileValidator


def create_batches(items: List, batch_size: int) -> Generator[List, None, None]:
    """Cria um gerador de lotes a partir de uma lista."""
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]


def save_report(report_text: str, filename: str = "batch_processing_report.txt") -> str:
    """Salva relatório na raiz do projeto para análise posterior."""
    filepath = os.path.join(os.getcwd(), filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(report_text)
    return filepath


def get_results_summary_text(summary_dict: dict) -> str:
    """Gera texto resumido a partir do dicionário de resumo."""
    lines = []
    lines.append("RELATÓRIO FINAL DO PROCESSAMENTO EM LOTE")
    lines.append("=" * 50)
    lines.append(f"Total de arquivos processados: {summary_dict.get('total_files', 0)}")
    lines.append(f"Arquivos com sucesso: {summary_dict.get('successful', 0)}")
    lines.append(f"Arquivos com erro: {summary_dict.get('failed', 0)}")
    lines.append(
        f"Total de XMLs enviados para a nuvem: {summary_dict.get('total_xml_files_processed', 0)}"
    )
    lines.append(
        f"Tempo total de processamento (s): {summary_dict.get('total_processing_time_seconds', 0.0):.2f}"
    )
    lines.append(
        f"Tempo médio por arquivo (s): {summary_dict.get('average_processing_time_per_file', 0.0):.2f}"
    )
    lines.append(
        f"Taxa de sucesso (%): {summary_dict.get('success_rate_percent', 0.0):.2f}"
    )
    lines.append("=" * 50)
    return "\n".join(lines)


def main():
    """Função principal para o processamento em lote."""
    try:
        # 1. Carregar Configurações
        config = ConfigLoader.load_from_yaml("config.yaml")
        ConfigLoader.validate_config(config)

        # 2. Configurar Logging
        setup_logging(log_level=config.logging.level, log_file=config.logging.file)
        logger = get_logger(__name__)
        logger.info("=" * 50)
        logger.info("Iniciando Aplicação de Processamento em LOTE")
        logger.info("=" * 50)

        # 3. Inicializar Componentes
        zip_processor = StreamingZipProcessor(ssd_path=config.processing.ssd_path)
        oci_client = StreamingOCIClient(
            config_path=config.oci.config_path,
            namespace=config.oci.namespace,
            bucket_name=config.oci.bucket_name,
        )
        file_manager = FileManager(base_dir=config.processing.ssd_path)

        batch_processor = BatchProcessor(
            zip_processor=zip_processor,
            oci_client=oci_client,
            file_manager=file_manager,
            max_workers=config.processing.max_workers,
        )
        logger.info(
            f"Processador configurado com max_workers={config.processing.max_workers}"
        )

        # 4. Carregar e Validar Arquivos
        all_files = file_manager.load_all_zip_files()
        valid_files = FileValidator.filter_valid_zip_files(all_files)

        if not valid_files:
            logger.warning("Nenhum arquivo .zip válido encontrado. Encerrando.")
            sys.exit(0)

        batch_size = config.processing.batch_size
        total_batches = (len(valid_files) + batch_size - 1) // batch_size
        logger.info(f"Total de {len(valid_files)} arquivos válidos encontrados.")
        logger.info(
            f"Arquivos serão processados em {total_batches} lotes de até {batch_size} arquivos cada."
        )

        # --- INÍCIO DA ALTERAÇÃO: Confirmação do Usuário ---
        proceed = input("Deseja continuar com o processamento? (s/n): ").strip().lower()
        if proceed != "s":
            logger.info("Processamento abortado pelo usuário.")
            sys.exit(0)
        # --- FIM DA ALTERAÇÃO ---

        # 5. Criar e Processar Lotes
        batches = create_batches(valid_files, batch_size)
        all_results = []
        start_time = time.time()

        for i, batch in enumerate(batches):
            logger.info(f"--- Processando Lote {i + 1}/{total_batches} ---")
            batch_results = batch_processor.process_batch_streaming(
                zip_files=batch,
                object_prefix="cnpq_lattes",
            )
            all_results.extend(batch_results)
            logger.info(f"--- Fim do Lote {i + 1}/{total_batches} ---")

        total_time = time.time() - start_time
        logger.info("=" * 50)
        logger.info("Processamento de todos os lotes concluído!")
        logger.info(f"Tempo total: {total_time:.2f} segundos")

        # --- INÍCIO DA ALTERAÇÃO: Gerar e Salvar Relatório ---
        final_summary = batch_processor.get_batch_summary(all_results)
        summary_text = get_results_summary_text(final_summary)

        report_path = save_report(summary_text)

        logger.info(f"Relatório final salvo em: {report_path}")
        print("\n" + summary_text)  # Mostra o relatório no console também
        # --- FIM DA ALTERAÇÃO ---

    except Exception as e:
        print(f"Erro fatal na aplicação de lote: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
