import unittest
from pathlib import Path
from airflow.models import DagBag


class TestR6ATransformationDagContract(unittest.TestCase):
    """
    Testes de contrato estrutural e governança para a DAG r6_transformacoes_lakehouse (R6-A).
    """

    @classmethod
    def setUpClass(cls):
        dag_folder = Path(__file__).resolve().parent.parent.parent / "airflow" / "dags"
        cls.dagbag = DagBag(dag_folder=str(dag_folder), include_examples=False)
        cls.dag = cls.dagbag.get_dag("r6_transformacoes_lakehouse")

    def test_dag_loaded_without_errors(self):
        """Verifica se a DAG foi carregada sem erros de importação."""
        self.assertEqual(len(self.dagbag.import_errors), 0, f"Erros de importação no DagBag: {self.dagbag.import_errors}")
        self.assertIsNotNone(self.dag, "DAG 'r6_transformacoes_lakehouse' não encontrada no DagBag.")

    def test_dag_configuration(self):
        """Verifica configurações estritas de schedule, catchup, concorrência e tags."""
        self.assertIsNone(self.dag.schedule_interval)
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)
        self.assertEqual(self.dag.default_args.get("owner"), "airflow")
        self.assertFalse(self.dag.default_args.get("depends_on_past"))
        self.assertEqual(self.dag.default_args.get("retries"), 0)
        self.assertIn("r6", self.dag.tags)
        self.assertIn("transformacoes", self.dag.tags)
        self.assertIn("lakehouse", self.dag.tags)

    def test_task_count_and_task_groups(self):
        """Verifica a quantidade de tarefas e a existência dos 4 TaskGroups."""
        self.assertEqual(len(self.dag.tasks), 12)
        task_ids = {t.task_id for t in self.dag.tasks}
        expected_task_ids = {
            "start",
            "preflight_dbt",
            "silver.bootstrap_silver",
            "silver.dbt_build_silver",
            "silver.reconcile_bronze_silver",
            "gold.bootstrap_gold",
            "gold.dbt_build_gold_core",
            "gold.reconcile_silver_gold",
            "serving.dbt_build_semantic_view",
            "serving.dbt_build_serving_mart",
            "documentation.dbt_docs_generate",
            "end",
        }
        self.assertEqual(task_ids, expected_task_ids)

    def test_fail_fast_and_trigger_rules(self):
        """Garante que todas as tarefas utilizem trigger_rule all_success (fail-fast estrito)."""
        for task in self.dag.tasks:
            self.assertEqual(
                task.trigger_rule,
                "all_success",
                f"Tarefa '{task.task_id}' deve ter trigger_rule 'all_success', encontrado '{task.trigger_rule}'"
            )

    def test_linear_dependency_chain(self):
        """Valida o fluxo sequencial linear estrito de ponta a ponta sem paralelismo entre camadas."""
        expected_chain = [
            "start",
            "preflight_dbt",
            "silver.bootstrap_silver",
            "silver.dbt_build_silver",
            "silver.reconcile_bronze_silver",
            "gold.bootstrap_gold",
            "gold.dbt_build_gold_core",
            "gold.reconcile_silver_gold",
            "serving.dbt_build_semantic_view",
            "serving.dbt_build_serving_mart",
            "documentation.dbt_docs_generate",
            "end",
        ]

        task_dict = {t.task_id: t for t in self.dag.tasks}

        for i in range(len(expected_chain) - 1):
            curr_id = expected_chain[i]
            next_id = expected_chain[i + 1]

            curr_task = task_dict[curr_id]
            next_task = task_dict[next_id]

            self.assertIn(
                curr_task,
                next_task.upstream_list,
                f"'{curr_id}' deve ser upstream imediato de '{next_id}'"
            )
            self.assertIn(
                next_task,
                curr_task.downstream_list,
                f"'{next_id}' deve ser downstream imediato de '{curr_id}'"
            )


if __name__ == "__main__":
    unittest.main()
