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
        self.ssd_path = Path(ssd_path)
        self.logger = get_logger(__name__)

        self.logger.info(f"StreamingZipProcessor inicializado - SSD: {self.ssd_path}")

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

    def extract_xml_files_streaming(self, zip_filename: str) -> StreamExtractionResult:
        """
        Extrai informações sobre arquivos XML sem carregar o conteúdo.

        Args:
            zip_filename (str): Nomo do arquivo ZIP

        Returns:
            StreamExtractionResult com informações da extração
        """

        result = StreamExtractionResult(
            zip_filename=zip_filename, xml_files_found=0, success=False
        )

        try:
            zip_path = Path(self.ssd_path) / zip_filename

            if not zip_path.exists():
                raise FileNotFoundError(f"Arquivo ZIP não encontrado: {zip_filename}")

            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                # Contar arquivos XML
                xml_files = [
                    info
                    for info in zip_ref.infolist()
                    if info.filename.endswith(".xml") and not info.is_dir()
                ]

            result.xml_files_found = len(xml_files)
            result.success = len(xml_files) > 0

            if not result.success:
                result.error_message = (
                    f"Nenhum arquivo XML encontrado em {zip_filename}"
                )

        except Exception as e:
            result.error_message = str(e)
            self.logger.error(f"Erro na verificação de {zip_filename}: {str(e)}")

        return result

    def get_zip_metadata(self, zip_filename: str) -> Dict:
        """
        Obtém metadados do arquivo ZIP sem extrair conteúdo.

        Args:
            zip_filename: Nome do arquivo ZIP

        Returns:
            Dicionário com metadados
        """
        zip_path = self.ssd_path / zip_filename

        if not zip_path.exists():
            return {"error": f"Arquivo não encontrado: {zip_filename}"}

        try:
            metadata = {
                "zip_filename": zip_filename,
                "file_size": zip_path.stat().st_size,
                "xml_files": [],
                "total_xml_files": 0,
                "total_uncompressed_size": 0,
            }

            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                for info in zip_ref.infolist():
                    if info.filename.endswith(".xml") and not info.is_dir():
                        metadata["xml_files"].append(
                            {
                                "filename": info.filename,
                                "compressed_size": info.compress_size,
                                "uncompressed_size": info.file_size,
                                "compression_ratio": (
                                    (1 - info.compress_size / info.file_size) * 100
                                    if info.file_size > 0
                                    else 0
                                ),
                            }
                        )
                        metadata["total_uncompressed_size"] += info.file_size

                metadata["total_xml_files"] = len(metadata["xml_files"])

            return metadata

        except Exception as e:
            return {"error": str(e)}

    def get_zip_files_list(self) -> List[str]:
        """
        Retorna lista de arquivos ZIP disponíveis no diretório SSD.

        Returns:
            Lista de nomes de arquivos ZIP
        """
        try:
            zip_files = [f.name for f in self.ssd_path.glob("*.zip")]
            self.logger.info(f"Encontrados {len(zip_files)} arquivos ZIP")
            return zip_files
        except Exception as e:
            self.logger.error(f"Erro ao listar arquivos ZIP: {str(e)}")
            return []
