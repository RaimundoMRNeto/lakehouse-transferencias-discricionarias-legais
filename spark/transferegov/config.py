"""
Módulo de configuração e carregamento de fontes Transferegov para a camada Bronze.
"""
import os
import yaml
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

@dataclass
class DatasetConfig:
    id: str
    zip_file: str
    member_file: str
    table_name: str
    delta_path: str
    raw_s3_prefix: str

@dataclass
class ControlConfig:
    id: str
    zip_file: str
    member_file: str
    expected_column: str
    date_format: str
    encoding: str
    delimiter: str

@dataclass
class StorageConfig:
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_bucket_bronze: str
    s3_region: str
    raw_prefix: str
    warehouse_prefix: str
    local_landing_dir: str
    local_staging_dir: str

@dataclass
class SparkConfig:
    master: str
    app_name: str
    thrift_host: str
    thrift_port: int
    executor_memory: str
    executor_cores: int
    cores_max: int

class AppConfig:
    def __init__(self, config_path: Optional[str] = None):
        if config_path is None:
            # Padrão: mesmo diretório raiz de spark ou /app
            candidates = [
                os.path.join(os.path.dirname(os.path.dirname(__file__)), "transferegov_sources.yml"),
                "/app/transferegov_sources.yml",
                "c:/Dev/lakehouse-transferencias-discricionarias-legais/spark/transferegov_sources.yml"
            ]
            for cand in candidates:
                if os.path.exists(cand):
                    config_path = cand
                    break

        if not config_path or not os.path.exists(config_path):
            raise FileNotFoundError(f"Arquivo de fontes transferegov_sources.yml não encontrado nos locais esperados: {candidates}")

        with open(config_path, "r", encoding="utf-8") as f:
            self.raw_data = yaml.safe_load(f)

        self.base_url = self.raw_data.get("base_url")

        ctrl = self.raw_data.get("control_source", {})
        self.control = ControlConfig(
            id=ctrl.get("id", "data_carga_siconv"),
            zip_file=ctrl.get("zip_file", "data_carga_siconv.zip"),
            member_file=ctrl.get("member_file", "data_carga_siconv.csv"),
            expected_column=ctrl.get("expected_column", "data_carga"),
            date_format=ctrl.get("date_format", "%d/%m/%Y %H:%M:%S"),
            encoding=ctrl.get("encoding", "utf-8-sig"),
            delimiter=ctrl.get("delimiter", ";")
        )

        self.datasets: Dict[str, DatasetConfig] = {}
        for ds in self.raw_data.get("analytical_datasets", []):
            self.datasets[ds["id"]] = DatasetConfig(
                id=ds["id"],
                zip_file=ds["zip_file"],
                member_file=ds["member_file"],
                table_name=ds["table_name"],
                delta_path=ds["delta_path"],
                raw_s3_prefix=ds["raw_s3_prefix"]
            )

        self.csv_options = self.raw_data.get("csv_options", {})

        st = self.raw_data.get("storage", {})
        self.storage = StorageConfig(
            s3_endpoint=os.environ.get("S3_ENDPOINT", st.get("s3_endpoint", "http://minio:9000")),
            s3_access_key=os.environ.get("AWS_ACCESS_KEY_ID", os.environ.get("MINIO_ROOT_USER", st.get("s3_access_key", "minio"))),
            s3_secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY", os.environ.get("MINIO_ROOT_PASSWORD", st.get("s3_secret_key", "minio123"))),
            s3_bucket_bronze=os.environ.get("BRONZE_BUCKET", st.get("s3_bucket_bronze", "bronze")),
            s3_region=st.get("s3_region", "us-east-1"),
            raw_prefix=st.get("raw_prefix", "raw/transferegov"),
            warehouse_prefix=st.get("warehouse_prefix", "warehouse"),
            local_landing_dir=st.get("local_landing_dir", "/data/landing"),
            local_staging_dir=st.get("local_staging_dir", "/data/staging")
        )

        sp = self.raw_data.get("spark", {})
        self.spark = SparkConfig(
            master=os.environ.get("SPARK_MASTER", sp.get("master", "spark://spark-master:7077")),
            app_name=sp.get("app_name", "R2_Ingestao_Bronze_Transferegov"),
            thrift_host=os.environ.get("SPARK_THRIFT_HOST", sp.get("thrift_host", "spark-thrift-server")),
            thrift_port=int(os.environ.get("SPARK_THRIFT_PORT", sp.get("thrift_port", 10000))),
            executor_memory=sp.get("executor_memory", "1G"),
            executor_cores=int(sp.get("executor_cores", 1)),
            cores_max=int(sp.get("cores_max", 2))
        )

        ret = self.raw_data.get("retention", {})
        self.raw_versions_per_dataset = int(ret.get("raw_versions_per_dataset", 2))
