"""
Módulo responsável pelo gerenciamento de arquivos de metadados e resultados do processamento em lote.
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from src.utils.exceptions import MetadataError
from src.utils.logger import get_logger


class FileManager:
    """Gerenciador de arquivos de processamento e metainformação do pipeline."""

    def __init__(
        self,
        base_dir: str = "./data",
        metadata_filename: str = "processing_metadata.csv",
    ):
        """
        Inicializa o FileManager.

        Args:
            base_dir: Diretório base para dados e metadados.
            metadata_filename: Nome do arquivo de metadados em CSV.
        """
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_file = self.base_dir / metadata_filename
        self.logger = get_logger(__name__)
        self.logger.debug(f"FileManager inicializado em {self.base_dir}")

    def load_statistical_sample(
        self, metadata_csv_path: Optional[str] = None
    ) -> List[str]:
        """
        Carrega lista de arquivos ZIP estatísticos a processar.

        Args:
            metadata_csv_path: Caminho opcional para CSV de amostra/seleção.

        Returns:
            Lista de nomes de arquivos ZIP.
        """
        if metadata_csv_path and Path(metadata_csv_path).exists():
            try:
                df = pd.read_csv(metadata_csv_path)
                if "arquivo_zip" in df.columns:
                    # Critérios de filtro opcionais (status/amostra)
                    if {"status", "amostra_estatistica"}.issubset(df.columns):
                        sample_files = df[
                            (df["status"].str.upper() == "PROCESSADO")
                            & (df["amostra_estatistica"] == True)
                        ]["arquivo_zip"].tolist()
                    else:
                        sample_files = df["arquivo_zip"].tolist()
                    self.logger.info(
                        f"Carregados {len(sample_files)} arquivos do CSV estatístico {metadata_csv_path}"
                    )
                    return sample_files
                else:
                    self.logger.warning(
                        f"Coluna 'arquivo_zip' não encontrada em {metadata_csv_path}"
                    )
            except Exception as e:
                self.logger.error(f"Erro ao carregar CSV de amostra: {e}")

        # Fallback minimalista
        default_sample = [
            "2289791037340878.zip",
            "7736757198910884.zip",
            "4750415979101539.zip",
            "7953183731885121.zip",
            "8154062550507046.zip",
        ]
        self.logger.warning(
            f"Usando amostra padrão de desenvolvimento ({len(default_sample)} arquivos)."
        )
        return default_sample

    def save_processing_results(self, results: List[Dict]):
        """
        Salva resultados do processamento em arquivo CSV incrementalmente.

        Args:
            results: Lista de dicionários de resultados de processamento.
        """
        try:
            new_df = pd.DataFrame(results)
            # Adiciona timestamp
            new_df["timestamp"] = datetime.now().isoformat()

            if self.metadata_file.exists():
                existing_df = pd.read_csv(self.metadata_file)
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                combined_df = new_df

            combined_df.to_csv(self.metadata_file, index=False)
            self.logger.info(
                f"Salvo relatório de processamento em {self.metadata_file} ({combined_df.shape[0]} linhas)."
            )
        except Exception as e:
            self.logger.error(f"Erro ao salvar metadados: {e}")
            raise MetadataError(f"Não foi possível salvar resultados: {e}")

    def load_processing_history(self) -> pd.DataFrame:
        """
        Carrega todo o histórico de processamento em DataFrame.

        Returns:
            DataFrame com histórico, ou vazio se não houver.
        """
        try:
            if self.metadata_file.exists():
                df = pd.read_csv(self.metadata_file)
                self.logger.info(f"Histórico carregado com {len(df)} registros.")
                return df
            else:
                self.logger.warning("Nenhum histórico de processamento encontrado.")
                return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Erro ao carregar histórico: {e}")
            return pd.DataFrame()

    def get_processing_stats(self) -> Dict:
        """
        Consolida estatísticas do processamento conforme histórico.

        Returns:
            Dicionário com métricas principais.
        """
        try:
            df = self.load_processing_history()
            if df.empty:
                return {"total_processed": 0}

            stats = {
                "total_processed": len(df),
                "successful": len(df[df["status"].str.upper() == "SUCESSO"]),
                "partial_success": len(
                    df[df["status"].str.upper() == "SUCESSO_PARCIAL"]
                ),
                "failed": len(df[df["status"].str.upper() == "ERRO"]),
                "total_xml_files": (
                    df["xml_files_processed"].sum()
                    if "xml_files_processed" in df.columns
                    else None
                ),
                "last_processed": (
                    df["timestamp"].max() if "timestamp" in df.columns else None
                ),
            }
            self.logger.info(f"Estatísticas de processamento: {stats}")
            return stats
        except Exception as e:
            self.logger.error(f"Erro ao calcular estatísticas: {e}")
            return {"error": str(e)}

    def backup_metadata(self, backup_dir: Optional[str] = None) -> Optional[Path]:
        """
        Cria backup do arquivo de metadata.

        Args:
            backup_dir: Diretório opcional para salvar backup

        Returns:
            Caminho do backup realizado
        """
        try:
            if not self.metadata_file.exists():
                self.logger.warning("Nenhum arquivo de metadados para backup.")
                return None
            if backup_dir:
                backup_path = (
                    Path(backup_dir)
                    / f"{self.metadata_file.stem}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                )
            else:
                backup_path = self.metadata_file.with_name(
                    f"{self.metadata_file.stem}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                )
            self.metadata_file.replace(backup_path)
            self.logger.info(f"Backup realizado: {backup_path}")
            return backup_path
        except Exception as e:
            self.logger.error(f"Erro ao criar backup de metadados: {e}")
            return None

    def load_all_zip_files(self) -> List[str]:
        """
        Carrega a lista de todos os arquivos .zip do diretório base.

        Returns:
            Lista com os caminhos completos de todos os arquivos .zip.
        """
        self.logger.info(f"Carregando todos os arquivos .zip de: {self.base_dir}")
        zip_files = [str(p) for p in self.base_dir.glob("**/*.zip") if p.is_file()]
        self.logger.info(f"Total de {len(zip_files)} arquivos .zip encontrados.")
        return zip_files


# ... (resto do arquivo)
