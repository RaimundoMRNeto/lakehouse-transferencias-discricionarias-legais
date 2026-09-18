import re
import unittest
from pathlib import Path


class TestR7GovernanceContract(unittest.TestCase):
    """
    Contrato automatizado de governança, catálogo de metadados, integridade
    documental e proteção contra vazamento de credenciais locais (R7).
    
    Este teste é estritamente leve, dependendo apenas da biblioteca padrão do
    Python, podendo executar tanto em CI quanto em ambientes locais sem containers.
    """

    @classmethod
    def setUpClass(cls):
        cls.repo_root = Path(__file__).resolve().parent.parent.parent
        cls.docs_gov = cls.repo_root / "docs" / "governanca"
        cls.dbt_models = cls.repo_root / "dbt_lakehouse" / "models"

    def test_governance_docs_exist(self):
        """Valida a existência de todos os documentos obrigatórios do pacote de governança."""
        expected_files = [
            "README.md",
            "governanca_dados.md",
            "catalogo_contratos.md",
            "qualidade_linhagem.md",
            "glossario.md",
            "operacao_reprodutibilidade.md",
            "r7_relatorio_entrega.md",
        ]
        for filename in expected_files:
            file_path = self.docs_gov / filename
            self.assertTrue(
                file_path.is_file(),
                f"Documento de governança obrigatório não encontrado: {file_path}",
            )
            self.assertGreater(
                file_path.stat().st_size,
                200,
                f"Documento {filename} está vazio ou com conteúdo insuficiente.",
            )

    def test_no_credential_leak_in_governance_docs(self):
        """
        Proteção contra vazamento documental:
        Garante que os arquivos de governança não contenham literais conhecidos
        de credenciais locais do ambiente Docker.
        """
        forbidden_patterns = [
            r"minio123",
            r"SUPERSET_SECRET_KEY\s*=\s*['\"].+['\"]",
            r"MINIO_ROOT_PASSWORD\s*=\s*['\"].+['\"]",
            r"airflow_secret",
        ]
        md_files = list(self.docs_gov.glob("*.md"))
        self.assertGreater(len(md_files), 0, "Nenhum arquivo markdown em docs/governanca/")

        for md_file in md_files:
            content = md_file.read_text(encoding="utf-8")
            for pattern in forbidden_patterns:
                match = re.search(pattern, content, re.IGNORECASE)
                self.assertIsNone(
                    match,
                    f"Vazamento potencial de credencial detectado em {md_file.name} com o padrão '{pattern}'",
                )

    def test_bronze_sources_documented(self):
        """Valida que todas as fontes Bronze em sources_bronze.yml possuem descrição não vazia."""
        sources_file = self.dbt_models / "staging" / "sources_bronze.yml"
        self.assertTrue(sources_file.is_file(), "Arquivo sources_bronze.yml não encontrado.")

        content = sources_file.read_text(encoding="utf-8")

        # Verifica descrição da source bronze
        self.assertRegex(
            content,
            r"name:\s*bronze\b[\s\S]*?description:\s*[\"']?[^\n\"']+",
            "Source 'bronze' não possui descrição válida.",
        )

        expected_tables = [
            "siconv_proposta",
            "siconv_programa_proposta",
            "siconv_programa",
            "siconv_convenio",
        ]
        for table in expected_tables:
            # Garante que cada tabela está presente e seguida de description
            pattern = rf"name:\s*{table}\b[\s\S]*?description:\s*[|>\"']?\s*(\S+)"
            match = re.search(pattern, content)
            self.assertIsNotNone(
                match,
                f"Tabela source '{table}' não possui descrição documentada em sources_bronze.yml.",
            )

    def test_all_sql_models_documented_in_yaml(self):
        """
        Garante que 100% dos modelos SQL do projeto estejam declarados
        com descrição não vazia em arquivos YAML de schema.
        """
        # Coleta todos os modelos SQL (excluindo macros ou testes)
        sql_files = list(self.dbt_models.rglob("*.sql"))
        model_names = {f.stem for f in sql_files}
        self.assertEqual(
            len(model_names),
            20,
            f"Esperava exatamente 20 modelos SQL dbt, encontrados {len(model_names)}: {model_names}",
        )

        # Coleta todos os arquivos YAML de schema de modelos
        yaml_files = list(self.dbt_models.rglob("*.yml"))
        all_yaml_content = "\n".join(f.read_text(encoding="utf-8") for f in yaml_files)

        # Verifica para cada modelo SQL se ele está documentado em YAML
        for model in sorted(model_names):
            # Procura por "- name: <model>" seguido de "description:"
            pattern = rf"-\s*name:\s*{model}\b[\s\S]*?description:\s*[|>\"']?\s*(\S+)"
            match = re.search(pattern, all_yaml_content)
            self.assertIsNotNone(
                match,
                f"Modelo SQL '{model}' não possui descrição válida nos arquivos YAML de modelos.",
            )

    def test_semantic_and_serving_schemas_exist_and_documented(self):
        """Valida que os schemas de Semantic e Serving existem e documentam suas entidades."""
        semantic_schema = self.dbt_models / "gold" / "semantic" / "schema.yml"
        serving_schema = self.dbt_models / "gold" / "serving" / "schema.yml"

        self.assertTrue(semantic_schema.is_file(), "Arquivo gold/semantic/schema.yml não encontrado.")
        self.assertTrue(serving_schema.is_file(), "Arquivo gold/serving/schema.yml não encontrado.")

        semantic_content = semantic_schema.read_text(encoding="utf-8")
        serving_content = serving_schema.read_text(encoding="utf-8")

        self.assertIn("vw_superset_proposta_convenio", semantic_content)
        self.assertIn("mart_superset_proposta_convenio", serving_content)

        # Valida que as 29 colunas do contrato analítico estão documentadas em ambos
        key_columns = [
            "id_proposta",
            "numero_proposta",
            "data_proposta",
            "identificacao_proponente",
            "nome_proponente",
            "municipio",
            "uf",
            "orgao_superior",
            "orgao_concedente",
            "modalidade",
            "situacao_proposta",
            "valor_global_proposta",
            "valor_repasse_proposta",
            "tem_convenio",
            "numero_convenio",
            "data_assinatura",
            "situacao_convenio",
            "is_instrumento_ativo",
            "valor_global_convenio",
            "valor_repasse_convenio",
            "valor_empenhado_convenio",
            "valor_desembolsado_convenio",
        ]
        for col in key_columns:
            self.assertIn(f"name: {col}", semantic_content, f"Coluna '{col}' ausente em semantic/schema.yml")
            self.assertIn(f"name: {col}", serving_content, f"Coluna '{col}' ausente em serving/schema.yml")

    def test_no_volatile_snapshot_counts_in_model_descriptions(self):
        """
        Finding R7-CAT-01:
        Valida que descrições de catálogo não contêm valores numéricos voláteis
        de snapshots históricos passados que induzam a erro em atualizações futuras.
        """
        volatile_numbers = [
            r"\b53\.018\b",
            r"\b1\.257\.350\b",
            r"\b287\.584\b",
            r"\b287\.586\b",
            r"\b1\.717\b",
            r"\b949286\b",
            r"\b956078\b",
        ]
        yaml_files = list(self.dbt_models.rglob("*.yml"))
        for yml in yaml_files:
            content = yml.read_text(encoding="utf-8")
            for num in volatile_numbers:
                match = re.search(num, content)
                self.assertIsNone(
                    match,
                    f"Número volátil de snapshot '{num}' encontrado no catálogo em {yml.name}",
                )


if __name__ == "__main__":
    unittest.main()
