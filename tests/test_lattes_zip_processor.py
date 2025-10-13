"""
Testes para StreamingZipProcessor, focando no método stream_xml_files.
"""

import zipfile
from io import BytesIO
from pathlib import Path
from unittest.mock import Mock, mock_open, patch

import pytest

from core.lattes_zip_processor import (
    StreamExtractionResult,
    StreamingZipProcessor,
    XMLFileStream,
)


class TestStreamingZipProcessor:
    """Testes para a classe StreamingZipProcessor."""

    @pytest.fixture
    def processor(self, temp_ssd_dir):
        """Fixture que cria uma instância do StreamingZipProcessor."""
        return StreamingZipProcessor(ssd_path=str(temp_ssd_dir))

    def test_init_creates_processor_with_correct_path(self, temp_ssd_dir, mock_logger):
        """Testa se o processador é inicializado corretamente."""
        processor = StreamingZipProcessor(ssd_path=str(temp_ssd_dir))

        assert processor.ssd_path == temp_ssd_dir
        assert processor.logger is not None
        mock_logger.info.assert_called_with(
            f"StreamingZipProcessor inicializado - SSD: {temp_ssd_dir}"
        )


class TestSstreamXMLFiles:
    """Testes específicos para o método stream_xml_files"""

    @pytest.fixture
    def processor(self, temp_ssd_dir, mock_logger):
        """Fixture que cria uma instância do StreamingZipProcessor."""
        return StreamingZipProcessor(ssd_path=str(temp_ssd_dir))

    def test_stream_xml_files_success_single_file(
        self,
        processor,
        mock_logger,
        temp_ssd_dir,
        sample_xml_content,
        create_test_zip,
    ):
        """Testa extração bem-sucedida de um único arquivo XML"""
        # Arranjo

        zip_filename = "12345678910.zip"
        zip_path = temp_ssd_dir / zip_filename

        files_data = {"12345678910.xml": sample_xml_content}

        create_test_zip(zip_path, files_data)

        # Ação
        xml_files = list(processor.stream_xml_files(zip_filename))

        # Assert
        assert len(xml_files) == 1

        xml_file = xml_files[0]
        assert isinstance(xml_file, XMLFileStream)
        assert xml_file.filename == "12345678910.xml"
        assert xml_file.content == sample_xml_content
        assert xml_file.size == len(sample_xml_content)

        # Verificar logs
        mock_logger.info.assert_any_call(
            f"Iniciando a extração streaming de: {zip_filename}"
        )
