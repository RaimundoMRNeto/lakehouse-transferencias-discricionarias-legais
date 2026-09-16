"""
Módulo de extração restrita e validação de integridade estrutural do CSV de entrada.
"""
import os
import zipfile
import csv
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

TECHNICAL_COLUMNS = [
    "__ingested_at_utc",
    "__ingestion_run_id",
    "__source_file",
    "__source_sha256"
]

def extract_expected_member(zip_path: str, expected_member_name: str, target_dir: str) -> str:
    """
    Extrai EXCLUSIVAMENTE o membro CSV esperado do arquivo ZIP para um caminho controlado.
    Impede path traversal e extrações irrestritas.
    """
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"Arquivo ZIP não encontrado: {zip_path}")

    os.makedirs(target_dir, exist_ok=True)
    target_csv_path = os.path.join(target_dir, expected_member_name)

    # Validar que o caminho final está dentro do diretório de destino (anti-Zip Slip)
    real_target_dir = os.path.realpath(target_dir)
    real_dest = os.path.realpath(target_csv_path)
    if not real_dest.startswith(real_target_dir):
        raise ValueError(f"Tentativa de path traversal detectada no ZIP {zip_path} com membro {expected_member_name}")

    with zipfile.ZipFile(zip_path, "r") as zf:
        members = zf.namelist()
        if expected_member_name not in members:
            raise ValueError(
                f"Membro esperado '{expected_member_name}' não encontrado no ZIP '{zip_path}'. "
                f"Membros presentes: {members}"
            )

        info = zf.getinfo(expected_member_name)
        logger.info(f"Extraindo {expected_member_name} ({info.file_size} bytes descompactados)...")

        # Extração em streaming por blocos para não alocar o arquivo inteiro em memória
        with zf.open(expected_member_name) as source, open(target_csv_path, "wb") as dest:
            while True:
                chunk = source.read(65536)
                if not chunk:
                    break
                dest.write(chunk)

    if not os.path.exists(target_csv_path) or os.path.getsize(target_csv_path) == 0:
        raise ValueError(f"Falha na extração: arquivo CSV extraído está vazio ou inexistente em {target_csv_path}")

    logger.info(f"Membro {expected_member_name} extraído com sucesso para {target_csv_path}")
    return target_csv_path

def validate_and_count_csv(
    csv_path: str,
    delimiter: str = ";",
    encoding: str = "utf-8-sig"
) -> Dict[str, Any]:
    """
    Realiza validação estrutural streaming do arquivo CSV:
    - Decodificação estrita (falha imediatamente se houver byte inválido para o encoding especificado)
    - Validação de cabeçalho: sem vazios, sem duplicados, sem colisão com metadados técnicos
    - Validação de largura uniforme (quantidade de campos por registro)
    - Contagem de registros lógicos (respeita quebras de linha internas e aspas)
    - Não carrega o arquivo inteiro em memória (O(1) em memória)
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Arquivo CSV não encontrado: {csv_path}")

    logger.info(f"Validando estrutura e contando registros de {csv_path} (encoding={encoding}, delim={delimiter})...")

    logical_row_count = 0
    header_columns: List[str] = []

    # Uso de errors='strict' para impedir conversão ou substituição silenciosa de caracteres inválidos
    with open(csv_path, mode="r", encoding=encoding, errors="strict", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter, quotechar='"', doublequote=True)

        try:
            raw_header = next(reader)
        except StopIteration:
            raise ValueError(f"Arquivo CSV está vazio: {csv_path}")
        except UnicodeDecodeError as e:
            raise UnicodeDecodeError(
                e.encoding, e.object, e.start, e.end,
                f"Erro de decodificação no cabeçalho de {csv_path} com {encoding}: {e}"
            )

        # Validação do cabeçalho (removendo espaços e eventuais caracteres BOM residuais)
        header_columns = [col.strip().lstrip("\ufeff") for col in raw_header]
        num_columns = len(header_columns)

        if num_columns == 0:
            raise ValueError(f"Cabeçalho vazio encontrado em {csv_path}")

        # Checar se há colunas com nome vazio
        empty_indices = [i for i, col in enumerate(header_columns) if not col]
        if empty_indices:
            raise ValueError(f"Cabeçalho contém colunas sem nome nos índices {empty_indices} em {csv_path}")

        # Checar duplicidades no cabeçalho
        seen_cols = set()
        duplicates = set()
        for col in header_columns:
            if col in seen_cols:
                duplicates.add(col)
            seen_cols.add(col)
        if duplicates:
            raise ValueError(f"Cabeçalho contém colunas duplicadas {duplicates} em {csv_path}")

        # Checar colisão com campos técnicos reservados
        collisions = [col for col in header_columns if col in TECHNICAL_COLUMNS]
        if collisions:
            raise ValueError(f"Cabeçalho colide com campos técnicos reservados {collisions} em {csv_path}")

        # Percorrer registros em streaming para validar largura e contar registros lógicos
        line_num = 1
        for row in reader:
            line_num += 1
            if len(row) != num_columns:
                raise ValueError(
                    f"Registro malformado no registro lógico {line_num} de {csv_path}: "
                    f"esperados {num_columns} campos, encontrados {len(row)}."
                )
            logical_row_count += 1

    logger.info(
        f"Validação concluída com sucesso para {csv_path}: "
        f"{num_columns} colunas, {logical_row_count} registros lógicos."
    )

    return {
        "csv_path": csv_path,
        "header_columns": header_columns,
        "num_columns": num_columns,
        "logical_row_count": logical_row_count,
        "delimiter": delimiter,
        "encoding": encoding
    }
