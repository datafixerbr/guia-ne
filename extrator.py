import logging
import os
import random
from pathlib import Path
from zipfile import ZipFile

# Configuração de logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# Configurações de diretório
base_dir = os.path.dirname(os.path.abspath(__name__))
data_dir = os.path.join(base_dir, "data")
ssd_path = "/media/datafixer/f57a7a83-c2e6-48e4-a82c-bdfa502ac0bf/cvs"
sample_list_file = os.path.join(data_dir, "arquivos_amostra.txt")
output_file = os.path.join(data_dir, "xml_files")  # Diretório específico para XMLs

# Parâmetros de processamento
POPULACAO_TOTAL = 7391139
AMOSTRA_TAMANHO = 500
RANDOM_SEED = 42


def load_sample_list():
    """Versão com debug melhorado"""
    if not os.path.exists(sample_list_file):
        logger.warning(f"Arquivo de amostra não encontrado: {sample_list_file}")
        return []

    try:
        sample_files = []
        with open(sample_list_file, "r") as f:
            lines = f.readlines()
            logger.info(f"Lendo {len(lines)} linhas do arquivo de amostra")

            for i, line in enumerate(lines):
                # line = line.strip()
                if line and not line.startswith("#"):
                    parts = line.split("\t")
                    if len(parts) == 2:
                        sample_files.append(parts[1])
                    else:
                        logger.warning(f"Linha {i+1} mal formatada: {line}")

        logger.info(f"✓ {len(sample_files)} arquivos carregados da lista")

        # Debug: mostrar primeiros arquivos
        if sample_files:
            logger.info(f"Primeiros arquivos: {sample_files[:3]}")

        return sample_files

    except Exception as e:
        logger.error(f"Erro ao carregar lista da amostra: {e}")
        return []


def generate_sample_files(ssd_path, sample_size=500, seed=42):
    """Gera nova amostra se não existir lista prévia"""
    logger.info("Gerando nova amostra de arquivos...")

    try:
        # Listar todos os arquivos ZIP
        path = Path(ssd_path)
        all_zip_files = [f.name for f in path.glob("*.zip")]

        if len(all_zip_files) == 0:
            logger.error("Nenhum arquivo ZIP encontrado!")
            return []

        logger.info(f"Total de arquivos encontrados: {len(all_zip_files):,}")

        # Gerar amostra sistemática
        random.seed(seed)
        interval = len(all_zip_files) // sample_size
        start = random.randint(0, interval - 1)

        selected_files = []
        for i in range(sample_size):
            index = start + (i * interval)
            if index < len(all_zip_files):
                selected_files.append(all_zip_files[index])

        logger.info(f"Amostra gerada: {len(selected_files)} arquivos")
        return selected_files

    except Exception as e:
        logger.error(f"Erro ao gerar amostra: {e}")
        return []


def extract_xml_from_zip(zip_filename, ssd_path, extract_dir):
    """Extrai arquivos XML de um único ZIP"""
    zip_file_path = os.path.join(ssd_path, zip_filename)

    if not os.path.exists(zip_file_path):
        logger.warning(f"Arquivo não encontrado: {zip_filename}")
        return 0

    try:
        with ZipFile(zip_file_path, "r") as zip_ref:
            # Filtrar apenas arquivos XML
            xml_files = [item for item in zip_ref.namelist() if item.endswith(".xml")]

            if not xml_files:
                logger.info(f"Nenhum XML encontrado em: {zip_filename}")
                return 0

            # Criar subdiretório com nome do ZIP (sem extensão)
            zip_name = os.path.splitext(zip_filename)[0]
            zip_extract_dir = os.path.join(extract_dir, zip_name)
            os.makedirs(zip_extract_dir, exist_ok=True)

            # Extrair apenas os XMLs
            zip_ref.extractall(path=zip_extract_dir, members=xml_files)

            logger.info(
                f"✓ {zip_filename}: {len(xml_files)} XMLs extraídos para {zip_extract_dir}"
            )
            return len(xml_files)

    except Exception as e:
        logger.error(f"Erro ao processar {zip_filename}: {e}")
        return 0


def extract_sample_xmls():
    """Função principal para extrair XMLs da amostra"""
    logger.info("=== INICIANDO EXTRAÇÃO DE XMLs DA AMOSTRA ===")

    # Criar diretório de destino
    os.makedirs(output_file, exist_ok=True)

    # Obter lista de arquivos da amostra
    sample_files = load_sample_list()

    # Se não existe lista, gerar nova amostra
    if not sample_files:
        sample_files = generate_sample_files(ssd_path, AMOSTRA_TAMANHO, RANDOM_SEED)

        if not sample_files:
            logger.error("Falha ao obter lista de arquivos da amostra!")
            return

    logger.info(f"Processando {len(sample_files)} arquivos da amostra...")
    logger.info(f"Diretório de destino: {output_file}")

    # Extrair XMLs de cada arquivo da amostra
    total_xmls = 0
    processed_files = 0

    for i, zip_filename in enumerate(sample_files, 1):
        logger.info(f"[{i}/{len(sample_files)}] Processando: {zip_filename}")

        xml_count = extract_xml_from_zip(zip_filename, ssd_path, output_file)
        total_xmls += xml_count

        if xml_count > 0:
            processed_files += 1

    # Relatório final
    logger.info("=== EXTRAÇÃO CONCLUÍDA ===")
    logger.info(f"Arquivos ZIP processados: {len(sample_files)}")
    logger.info(f"Arquivos com XMLs: {processed_files}")
    logger.info(f"Total de XMLs extraídos: {total_xmls}")
    logger.info(f"XMLs salvos em: {output_file}")

    # Verificar estrutura criada
    if os.path.exists(output_file):
        subdirs = [
            d
            for d in os.listdir(output_file)
            if os.path.isdir(os.path.join(output_file, d))
        ]
        logger.info(f"Subdiretórios criados: {len(subdirs)}")

        # Exemplo de estrutura
        if subdirs:
            example_dir = os.path.join(output_file, subdirs[0])
            example_files = os.listdir(example_dir)
            logger.info(f"Exemplo - {subdirs[0]}: {len(example_files)} arquivos")


if __name__ == "__main__":
    extract_sample_xmls()
