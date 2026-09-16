"""
Módulo de controle de carga oficial por data_carga_siconv.
"""
import os
import csv
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Tuple

from transferegov.config import AppConfig
from transferegov.http_downloader import download_file
from transferegov.s3_storage import S3StorageManager
from transferegov.csv_processor import extract_expected_member

logger = logging.getLogger(__name__)

def parse_data_carga(raw_text: str, expected_format: str = "%d/%m/%Y %H:%M:%S") -> Tuple[str, datetime]:
    """
    Parseia e valida estritamente a data_carga como data calendarizável real.
    Não atribui fuso horário (preserva representação ingênua/oficial da fonte).
    """
    cleaned = raw_text.strip()
    if not cleaned:
        raise ValueError("Valor de data_carga está vazio.")

    try:
        dt = datetime.strptime(cleaned, expected_format)
    except ValueError as e:
        raise ValueError(
            f"Valor de data_carga '{cleaned}' inválido ou não calendarizável "
            f"para o formato esperado '{expected_format}': {e}"
        )

    # Retorna o texto original limpo e o datetime parseado
    return cleaned, dt

def validate_and_parse_control_csv(
    csv_path: str,
    expected_column: str = "data_carga",
    delimiter: str = ";",
    encoding: str = "utf-8-sig",
    date_format: str = "%d/%m/%Y %H:%M:%S"
) -> Tuple[str, datetime]:
    """
    Valida estritamente o contrato do CSV de controle:
    - Exatamente 1 coluna com nome idêntico a expected_column (sem colunas extras, ausentes ou duplicadas).
    - Exatamente 1 registro de dados (rejeita zero linhas e rejeita duas ou mais linhas).
    - Registro com largura estritamente compatível (1 valor).
    - Data calendarizável no formato especificado.
    - Encoding estrito.
    """
    with open(csv_path, mode="r", encoding=encoding, errors="strict", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError(f"CSV de controle {csv_path} está vazio (zero linhas).")

        header_cols = [c.strip().lstrip("\ufeff") for c in header]

        if len(header_cols) == 0 or (len(header_cols) == 1 and not header_cols[0]):
            raise ValueError(f"Cabeçalho do CSV de controle {csv_path} está vazio.")

        if len(header_cols) > 1:
            if header_cols.count(expected_column) > 1:
                raise ValueError(
                    f"Cabeçalho do controle contém colunas duplicadas: {header_cols}"
                )
            if expected_column in header_cols:
                raise ValueError(
                    f"Cabeçalho do controle contém coluna adicional inesperada: {header_cols}"
                )
            raise ValueError(
                f"Coluna esperada '{expected_column}' ausente no cabeçalho do controle: {header_cols}"
            )

        if header_cols[0] != expected_column:
            raise ValueError(
                f"Coluna esperada '{expected_column}' ausente no cabeçalho do controle (encontrada: '{header_cols[0]}')."
            )

        try:
            row = next(reader)
        except StopIteration:
            raise ValueError(f"CSV de controle {csv_path} não contém linhas de dados (zero registros).")

        if len(row) != 1:
            raise ValueError(
                f"Registro de controle possui largura incompatível: esperada 1 coluna, encontrada {len(row)} colunas ({row})."
            )

        raw_val = row[0]
        raw_text, parsed_dt = parse_data_carga(raw_val, date_format)

        try:
            extra_row = next(reader)
            raise ValueError(
                f"CSV de controle {csv_path} contém duas ou mais linhas de dados (esperado apenas 1 registro). Linha excedente: {extra_row}"
            )
        except StopIteration:
            pass

    return raw_text, parsed_dt

def fetch_and_validate_control(
    config: AppConfig,
    run_id: str,
    s3_mgr: S3StorageManager
) -> Dict[str, Any]:
    """
    Baixa o arquivo oficial de controle data_carga_siconv.zip, armazena o ZIP original
    em s3://bronze/raw/transferegov/data_carga_siconv/sha256=<hash>/..., extrai o CSV
    e parseia o marcador oficial data_carga com validação estrita de contrato.
    """
    ctrl_cfg = config.control
    control_url = config.base_url + ctrl_cfg.zip_file

    landing_dir = os.path.join(config.storage.local_landing_dir, run_id, "control")
    staging_dir = os.path.join(config.storage.local_staging_dir, run_id, "control")
    zip_dest_path = os.path.join(landing_dir, ctrl_cfg.zip_file)

    # Download seguro com cálculo de hash
    dl_meta = download_file(
        url=control_url,
        destination_path=zip_dest_path,
        chunk_size=4096
    )

    # Upload RAW do ZIP original
    upload_meta = s3_mgr.upload_raw_zip(
        local_file_path=zip_dest_path,
        dataset_id=ctrl_cfg.id,
        calculated_sha256=dl_meta["sha256"]
    )

    # Extração controlada do membro CSV
    csv_path = extract_expected_member(
        zip_path=zip_dest_path,
        expected_member_name=ctrl_cfg.member_file,
        target_dir=staging_dir
    )

    # Leitura e validação estrita do CSV de controle
    raw_text, parsed_dt = validate_and_parse_control_csv(
        csv_path=csv_path,
        expected_column=ctrl_cfg.expected_column,
        delimiter=ctrl_cfg.delimiter,
        encoding=ctrl_cfg.encoding,
        date_format=ctrl_cfg.date_format
    )

    logger.info(
        f"Controle data_carga validado com sucesso: raw='{raw_text}', "
        f"parsed='{parsed_dt.isoformat()}', sha256='{dl_meta['sha256']}'"
    )

    return {
        "dataset_id": ctrl_cfg.id,
        "source_url": control_url,
        "source_file": ctrl_cfg.zip_file,
        "source_member": ctrl_cfg.member_file,
        "source_data_carga_raw": raw_text,
        "source_data_carga": parsed_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "sha256": dl_meta["sha256"],
        "file_size_bytes": dl_meta["file_size_bytes"],
        "raw_s3_key": upload_meta["s3_key"],
        "raw_s3_uri": upload_meta["s3_uri"],
        "retrieved_at_utc": dl_meta["retrieved_at_utc"],
        "etag": dl_meta.get("etag", ""),
        "last_modified": dl_meta.get("last_modified", "")
    }

def check_if_no_change_eligible(
    last_successful_run: Optional[Dict[str, Any]],
    current_control: Dict[str, Any],
    integrity_verifier_func,
    force: bool = False
) -> Tuple[bool, str]:
    """
    Avalia se a execução pode ser classificada como NO_CHANGE.

    Regra:
    - Se force=True -> não dispensa, processa.
    - Se não houver última execução com sucesso -> processa.
    - Se source_data_carga_raw for diferente -> processa.
    - Se o estado local esperado não estiver íntegro -> processa.
    - Apenas quando data_carga igual E integridade confirmada -> retorna True (NO_CHANGE).
    """
    if force:
        return False, "Execução com parâmetro force=True; processamento forçado solicitado."

    if not last_successful_run:
        return False, "Primeira execução do pipeline ou nenhuma execução SUCCESS anterior localizada."

    last_raw = last_successful_run.get("source_data_carga_raw_final") or last_successful_run.get("source_data_carga_raw_initial")
    curr_raw = current_control["source_data_carga_raw"]

    if last_raw != curr_raw:
        return False, f"data_carga publicada foi alterada de '{last_raw}' para '{curr_raw}'."

    # Validar integridade do estado local
    is_valid, reason = integrity_verifier_func()
    if not is_valid:
        return False, f"data_carga é idêntica mas estado local está incompleto: {reason}."

    return True, f"data_carga '{curr_raw}' idêntica à última execução com sucesso e estado local íntegro."
