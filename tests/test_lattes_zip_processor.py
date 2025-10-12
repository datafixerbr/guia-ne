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
