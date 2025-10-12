"""
Módulo responsável pela extração de arquivos ZIP a partir de um dispositivo fisico extrerno (ssd).
"""

import zipfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Dict, Iterator, List, Optional

from utils.exceptions import ZipProcessingError
from utils.logger import get_logger


@dataclass
class XMLFileStream:
    """Representa um arquivo XML extraído em memória"""

    filename: str
    content: bytes
    size: int


@dataclass
class StreamExtractionResult:
    """Resultado da extração streaming de um arquivo ZIP"""

    zip_filename: str
    xml_files_found: int
    success: bool
    error_message: Optional[str] = None


class StreamingZipProcessor:
    """Processador de ZIP que trabalha apenas em memória"""

    def __init__(self, ssd_path: str):
        """
        Inicializa o processador streaming

        Args:
            ssd_path (str): Caminho do diretório do dispositivo SSD
        """
        self.ssd_path = ssd_path
        self.logger = get_logger(__name__)

        self.logger.info(f"StreamingZipProcessor inicializado = SSD {self.ssd_path}")

    def stream_xml_files(self, zip_filename: str) -> Iterator[XMLFileStream]:
        """
        Gerador que extrai arquivos XML de um ZIP diretamente em memória.

        Args:
            zip_filename (str): Nome do arquivo Zip

        Yields:
            XMLFileStream: Arquivo XML extraído em memória
        """
        zip_path = Path(self.ssd_path) / zip_filename

        if not zip_path.exists():
            raise FileNotFoundError(f"Arquivo Zip não encontrado: {zip_filename}")

        self.logger.info(f"Iniciando a extração streaming de: {zip_filename}")

        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                # Filtrar apenas arquivos XML
                xml_files = [
                    info
                    for info in zip_ref.infolist()
                    if info.filename.endswith(".xml") and not info.is_dir()
                ]
                if not xml_files:
                    self.logger.warning(
                        f"Nenhum arquivo XML encontrado em {zip_filename}"
                    )
                    return

                self.logger.info(
                    f"Encontrados {len(xml_files)} arquivos XML em {zip_filename}"
                )

                for xml_info in xml_files:
                    try:
                        # Ler arquivo diretamente em memória
                        xml_content = zip_ref.read(xml_info.filename)

                        yield XMLFileStream(
                            filename=xml_info.filename,
                            content=xml_content,
                            size=len(xml_content),
                        )

                        self.logger.debug(
                            f"Extração em memória: {xml_info.filename} {len(xml_content)} bytes"
                        )

                    except Exception as e:
                        self.logger.warning(
                            f"Erro ao extrair {xml_info.filename}: {str(e)}"
                        )
                        continue

        except Exception as e:
            self.logger.error(f"Erro ao processar ZIP {zip_filename}: {str(e)}")
            raise ZipProcessingError(f"Erro na extração streaming: {str(e)}")

    # def extract_xml_files_streaming(self, zip_filename: str) -> StreamExtractionResult:
    #     """
    #     Extrai informações sobre arquivos XML sem carregar o conteúdo.

    #     Args:
    #         zip_filename (str): Nomo do arquivo ZIP

    #     Returns:
    #         StreamExtractionResult com informações da extração
    #     """

    #     reusult = StreamExtractionResult(
    #         zip_filename=zip_filename, xml_files_found=0, success=False
    #     )

    #     try:
    #         zip_path = Path(f"{self.ssd_path}/{zip_filename}")

    #         if not zip_path.exists():
    #             raise FileNotFoundError(f"Arquivo ZIP não encontrado: {zip_filename}")

    #         with zipfile.ZipFile(zip_path, "r") as zip_ref:
    #             xml_files = [
    #                 info
    #                 for info in zip_ref.infolist()
    #                 if info.filename.endswith(".xml") and not info.is_dir()
    #             ]

    #     except:
    #         pass
