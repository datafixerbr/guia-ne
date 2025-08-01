import gc
import logging
import math
import os
import random
import signal
import sys
import time
import xml.etree.ElementTree as ET
import zipfile
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime
from multiprocessing import cpu_count
from pathlib import Path

import pandas as pd
import psutil

# Configurações de diretório
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__name__)))
data_dir = os.path.join(base_dir, "data")
ssd_path = "/media/datafixer/f57a7a83-c2e6-48e4-a82c-bdfa502ac0bf/cvs"
output_file = os.path.join(data_dir, "lattes_amostra_metadata.csv")
sample_list_file = os.path.join(data_dir, "arquivos_amostra.txt")

# MODIFICADO: Arquivo de log agora na pasta data
log_file = os.path.join(data_dir, "amostragem_lattes.log")


# Configuração de logging
def setup_logging():
    """Configurar sistema de logging com rotação"""
    os.makedirs(data_dir, exist_ok=True)

    log_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - [PID:%(process)d] - %(message)s"
    )

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    from logging.handlers import RotatingFileHandler

    file_handler = RotatingFileHandler(
        log_file,  # Agora aponta para data/amostragem_lattes.log
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=3,
    )
    file_handler.setFormatter(log_formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"Log configurado em: {log_file}")

    return logger


logger = setup_logging()

# Parâmetros da amostragem estatística
POPULACAO_TOTAL = 7391139
NIVEL_CONFIANCA = 0.95
MARGEM_ERRO = 0.05
PROPORCAO_CONSERVADORA = 0.5
AMOSTRA_SEGURANCA = 500  # Adicionamos margem de segurança aos 385 calculados

# Configurações otimizadas para amostragem
MAX_WORKERS = 4  # Reduzido para amostra menor
CHUNK_SIZE = 25  # Chunks menores
RANDOM_SEED = 42  # Para reprodutibilidade


def calculate_sample_size(
    population_size, confidence_level=0.95, margin_error=0.05, proportion=0.5
):
    """
    Calcula o tamanho da amostra estatisticamente representativa

    Args:
        population_size (int): Tamanho da população
        confidence_level (float): Nível de confiança (0.90, 0.95, 0.99)
        margin_error (float): Margem de erro (0.01 a 0.10)
        proportion (float): Proporção estimada (0.5 para conservador)

    Returns:
        int: Tamanho da amostra recomendado
    """

    # Valor Z baseado no nível de confiança
    z_values = {0.90: 1.645, 0.95: 1.96, 0.99: 2.576}
    z = z_values.get(confidence_level, 1.96)

    # Cálculo inicial (população infinita)
    n_initial = (z**2 * proportion * (1 - proportion)) / (margin_error**2)

    # Ajuste para população finita
    n_adjusted = n_initial / (1 + ((n_initial - 1) / population_size))

    logger.info(f"Cálculo da Amostra Estatística:")
    logger.info(f"  População total: {population_size:,}")
    logger.info(f"  Nível de confiança: {confidence_level*100}%")
    logger.info(f"  Margem de erro: ±{margin_error*100}%")
    logger.info(f"  Amostra calculada: {n_adjusted:.0f}")
    logger.info(f"  Representatividade: {n_adjusted/population_size*100:.4f}%")

    return math.ceil(n_adjusted)


def generate_systematic_sample(total_files, sample_size, seed=42):
    """
    Gera amostra sistemática com ponto de partida aleatório

    Args:
        total_files (int): Total de arquivos na população
        sample_size (int): Tamanho da amostra desejada
        seed (int): Semente para reprodutibilidade

    Returns:
        list: Índices dos arquivos selecionados
    """
    random.seed(seed)

    # Calcular intervalo
    interval = total_files // sample_size

    # Ponto de partida aleatório
    start = random.randint(0, interval - 1)

    # Gerar índices sistemáticos
    selected_indices = []
    for i in range(sample_size):
        index = start + (i * interval)
        if index < total_files:
            selected_indices.append(index)

    logger.info(f"Amostragem Sistemática:")
    logger.info(f"  Intervalo: {interval}")
    logger.info(f"  Ponto inicial: {start}")
    logger.info(f"  Arquivos selecionados: {len(selected_indices)}")

    return selected_indices


def get_sample_files(directory, sample_size):
    """Obtém amostra representativa de arquivos ZIP"""
    logger.info("=== INICIANDO PROCESSO DE AMOSTRAGEM ===")

    # Listar todos os arquivos
    logger.info("Listando todos os arquivos ZIP...")
    try:
        path = Path(directory)
        all_zip_files = [f.name for f in path.glob("*.zip")]
        logger.info(f"Total de arquivos encontrados: {len(all_zip_files):,}")

        if len(all_zip_files) == 0:
            logger.error("Nenhum arquivo ZIP encontrado!")
            return []

        # Atualizar população real se diferente da estimada
        if len(all_zip_files) != POPULACAO_TOTAL:
            logger.warning(
                f"População real ({len(all_zip_files):,}) difere da estimada ({POPULACAO_TOTAL:,})"
            )
            # Recalcular amostra baseada na população real
            sample_size = calculate_sample_size(len(all_zip_files))
            sample_size = min(
                sample_size + 100, len(all_zip_files)
            )  # Margem de segurança

        # Gerar amostra sistemática
        selected_indices = generate_systematic_sample(len(all_zip_files), sample_size)
        sample_files = [all_zip_files[i] for i in selected_indices]

        # Salvar lista da amostra para auditoria
        save_sample_list(sample_files, selected_indices)

        logger.info(f"Amostra final: {len(sample_files)} arquivos")
        logger.info("=== AMOSTRAGEM CONCLUÍDA ===")

        return sample_files

    except Exception as e:
        logger.error(f"Erro ao gerar amostra: {e}")
        return []


def save_sample_list(sample_files, indices):
    """Salva lista de arquivos da amostra para auditoria"""
    try:
        os.makedirs(data_dir, exist_ok=True)

        with open(sample_list_file, "w") as f:
            f.write(f"# Amostra Estatística Gerada em {datetime.now()}\n")
            f.write(f"# Seed: {RANDOM_SEED}\n")
            f.write(f"# Total de arquivos: {len(sample_files)}\n")
            f.write(
                f"# Parâmetros: Confiança {NIVEL_CONFIANCA*100}%, Erro ±{MARGEM_ERRO*100}%\n\n"
            )

            for i, (idx, filename) in enumerate(zip(indices, sample_files)):
                f.write(f"{idx:08d}\t{filename}\n")

        logger.info(f"Lista da amostra salva em: {sample_list_file}")

    except Exception as e:
        logger.error(f"Erro ao salvar lista da amostra: {e}")


def monitor_system_resources():
    """Monitora recursos do sistema"""
    memory = psutil.virtual_memory()
    cpu_percent = psutil.cpu_percent(interval=1)

    return {
        "memory_percent": memory.percent,
        "memory_available_gb": memory.available / (1024**3),
        "cpu_percent": cpu_percent,
    }


def process_single_zip(args):
    """Processa um único arquivo ZIP da amostra"""
    zip_filename, ssd_path, temp_dir = args

    metadata_list = []
    zip_file_path = os.path.join(ssd_path, zip_filename)

    try:
        if not os.path.exists(zip_file_path):
            logger.warning(f"Arquivo da amostra não encontrado: {zip_filename}")
            return []

        # Obter informações do arquivo ZIP
        zip_stat = os.stat(zip_file_path)
        zip_size_mb = zip_stat.st_size / (1024 * 1024)
        zip_modified = datetime.fromtimestamp(zip_stat.st_mtime)

        # Processar ZIP
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            xml_files = [item for item in zip_ref.namelist() if item.endswith(".xml")]

            if not xml_files:
                metadata_list.append(
                    {
                        "arquivo_zip": zip_filename,
                        "xml_arquivo": None,
                        "status": "SEM_XML",
                        "zip_tamanho_mb": round(zip_size_mb, 2),
                        "zip_data_modificacao": zip_modified,
                        "linhas": 0,
                        "colunas": 0,
                        "xml_tamanho_kb": 0,
                        "encoding_detectado": None,
                        "elementos_xml_raiz": None,
                        "tem_namespaces": False,
                        "erro": None,
                        "amostra_estatistica": True,  # Identificador de amostra
                    }
                )
                return metadata_list

            # Processar cada XML do ZIP
            for xml_file in xml_files:
                xml_metadata = {
                    "arquivo_zip": zip_filename,
                    "xml_arquivo": xml_file,
                    "zip_tamanho_mb": round(zip_size_mb, 2),
                    "zip_data_modificacao": zip_modified,
                    "status": "PROCESSADO",
                    "amostra_estatistica": True,
                }

                try:
                    # Extrair para memória
                    xml_data = zip_ref.read(xml_file)
                    xml_size_kb = len(xml_data) / 1024
                    xml_metadata["xml_tamanho_kb"] = round(xml_size_kb, 2)

                    # Detectar encoding
                    try:
                        first_line = xml_data[:500].decode("utf-8", errors="ignore")
                        if "encoding=" in first_line:
                            encoding_start = first_line.find("encoding=") + 10
                            encoding_end = first_line.find('"', encoding_start)
                            if encoding_end > encoding_start:
                                encoding = first_line[encoding_start:encoding_end]
                                xml_metadata["encoding_detectado"] = encoding
                            else:
                                xml_metadata["encoding_detectado"] = "UTF-8"
                        else:
                            xml_metadata["encoding_detectado"] = "UTF-8"
                    except:
                        xml_metadata["encoding_detectado"] = "UNKNOWN"

                    # Parse XML básico
                    try:
                        import io

                        xml_io = io.BytesIO(xml_data)
                        tree = ET.parse(xml_io)
                        root = tree.getroot()
                        xml_metadata["elementos_xml_raiz"] = root.tag[:100]
                        xml_metadata["tem_namespaces"] = "{" in root.tag
                        del tree, root
                    except:
                        xml_metadata["elementos_xml_raiz"] = "ERRO_PARSE"
                        xml_metadata["tem_namespaces"] = False

                    # Ler com pandas
                    try:
                        xml_string = xml_data.decode("ISO-8859-1", errors="ignore")
                        df_temp = pd.read_xml(io.StringIO(xml_string))

                        xml_metadata["linhas"] = len(df_temp)
                        xml_metadata["colunas"] = len(df_temp.columns)
                        xml_metadata["erro"] = None

                        del df_temp, xml_string

                    except Exception as e:
                        xml_metadata["linhas"] = 0
                        xml_metadata["colunas"] = 0
                        xml_metadata["erro"] = str(e)[:100]
                        xml_metadata["status"] = "ERRO_PANDAS"

                    del xml_data

                except Exception as e:
                    xml_metadata.update(
                        {
                            "linhas": 0,
                            "colunas": 0,
                            "xml_tamanho_kb": 0,
                            "encoding_detectado": None,
                            "elementos_xml_raiz": None,
                            "tem_namespaces": False,
                            "erro": str(e)[:100],
                            "status": "ERRO_EXTRACAO",
                            "amostra_estatistica": True,
                        }
                    )

                metadata_list.append(xml_metadata)

        gc.collect()

    except Exception as e:
        logger.error(f"Erro crítico ao processar amostra {zip_filename}: {e}")
        return []

    return metadata_list


def process_sample(sample_files, ssd_path, output_file):
    """Processa a amostra estatística com paralelismo otimizado"""

    os.makedirs(data_dir, exist_ok=True)

    total_files = len(sample_files)
    logger.info(f"Processando amostra de {total_files} arquivos...")

    start_time = time.time()
    all_results = []

    # Preparar argumentos
    args_list = [(zip_file, ssd_path, None) for zip_file in sample_files]

    # Processar com paralelismo
    try:
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Dividir em chunks
            chunks = [
                args_list[i : i + CHUNK_SIZE]
                for i in range(0, len(args_list), CHUNK_SIZE)
            ]

            # Submeter jobs
            future_to_chunk = {
                executor.submit(process_chunk_sample, chunk): i
                for i, chunk in enumerate(chunks)
            }

            # Coletar resultados
            completed = 0
            for future in as_completed(future_to_chunk):
                try:
                    chunk_results = future.result(timeout=300)
                    all_results.extend(chunk_results)
                    completed += len(chunks[future_to_chunk[future]])

                    # Log de progresso
                    progress = completed / total_files * 100
                    logger.info(
                        f"Progresso da amostra: {completed}/{total_files} ({progress:.1f}%)"
                    )

                except Exception as e:
                    chunk_num = future_to_chunk[future]
                    logger.error(f"Erro no chunk {chunk_num}: {e}")

    except Exception as e:
        logger.error(f"Erro no processamento paralelo da amostra: {e}")
        return

    # Salvar resultados
    if all_results:
        try:
            df_amostra = pd.DataFrame(all_results)
            df_amostra.to_csv(output_file, index=False)

            # Estatísticas da amostra
            total_time = time.time() - start_time
            throughput = len(all_results) / total_time

            logger.info(f"=== RESULTADOS DA AMOSTRA ESTATÍSTICA ===")
            logger.info(f"Arquivos processados: {len(sample_files)}")
            logger.info(f"XMLs analisados: {len(all_results)}")
            logger.info(f"Tempo total: {total_time:.2f} segundos")
            logger.info(f"Throughput: {throughput:.1f} XMLs/segundo")
            logger.info(f"Dados salvos em: {output_file}")

            # Análise estatística básica
            df_sucesso = df_amostra[df_amostra["status"] == "PROCESSADO"]
            if len(df_sucesso) > 0:
                logger.info(f"\n=== ESTATÍSTICAS DA AMOSTRA ===")
                logger.info(f"XMLs processados com sucesso: {len(df_sucesso)}")
                logger.info(f"Total de linhas: {df_sucesso['linhas'].sum():,}")
                logger.info(
                    f"Média de linhas por XML: {df_sucesso['linhas'].mean():.1f}"
                )
                logger.info(f"Mediana de linhas: {df_sucesso['linhas'].median():.1f}")
                logger.info(
                    f"Máximo de colunas encontradas: {df_sucesso['colunas'].max()}"
                )
                logger.info(f"Média de colunas: {df_sucesso['colunas'].mean():.1f}")

                # Projeções para população total
                linha_total_estimada = df_sucesso["linhas"].mean() * POPULACAO_TOTAL
                logger.info(f"\n=== PROJEÇÕES PARA POPULAÇÃO TOTAL ===")
                logger.info(f"Estimativa de linhas totais: {linha_total_estimada:,.0f}")
                logger.info(
                    f"Confiança da estimativa: {NIVEL_CONFIANCA*100}% ± {MARGEM_ERRO*100}%"
                )

        except Exception as e:
            logger.error(f"Erro ao salvar resultados da amostra: {e}")

    else:
        logger.warning("Nenhum resultado obtido da amostra!")


def process_chunk_sample(chunk_args):
    """Processa um chunk da amostra"""
    results = []
    for args in chunk_args:
        try:
            file_results = process_single_zip(args)
            results.extend(file_results)
        except Exception as e:
            logger.error(f"Erro ao processar arquivo da amostra {args[0]}: {e}")
    return results


def main():
    """Função principal para processamento da amostra estatística"""
    try:
        logger.info("=== SISTEMA DE AMOSTRAGEM ESTATÍSTICA LATTES ===")
        logger.info(f"SSD Path: {ssd_path}")
        logger.info(f"Output: {output_file}")
        # ADICIONADO: Log da localização do arquivo de log
        logger.info(f"Log salvo em: {log_file}")

        # Verificar diretórios
        if not os.path.exists(ssd_path):
            logger.error(f"Diretório SSD não encontrado: {ssd_path}")
            return 1

        # Calcular tamanho da amostra
        sample_size = calculate_sample_size(
            POPULACAO_TOTAL, NIVEL_CONFIANCA, MARGEM_ERRO, PROPORCAO_CONSERVADORA
        )

        # Adicionar margem de segurança
        final_sample_size = min(AMOSTRA_SEGURANCA, sample_size + 100)
        logger.info(f"Tamanho final da amostra (com margem): {final_sample_size}")

        # Gerar amostra
        sample_files = get_sample_files(ssd_path, final_sample_size)

        if not sample_files:
            logger.error("Falha ao gerar amostra representativa!")
            return 1

        # Processar amostra
        process_sample(sample_files, ssd_path, output_file)

        logger.info("=== PROCESSAMENTO DA AMOSTRA CONCLUÍDO ===")
        return 0

    except Exception as e:
        logger.critical(f"Erro crítico: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
