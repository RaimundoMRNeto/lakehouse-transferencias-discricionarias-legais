import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path
from airflow.models import DagBag
from airflow.exceptions import AirflowException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


class TestR6BControllerDagContract(unittest.TestCase):
    """
    Testes de contrato estrutural e de governança para a DAG controladora R6-B
    (r6_pipeline_transferegov_e2e).
    """

    @classmethod
    def setUpClass(cls):
        dag_folder = Path(__file__).resolve().parent.parent.parent / "airflow" / "dags"
        cls.dagbag = DagBag(dag_folder=str(dag_folder), include_examples=False)
        cls.dag = cls.dagbag.get_dag("r6_pipeline_transferegov_e2e")

    def test_dag_loaded_without_errors(self):
        """Valida que a DAG foi carregada pelo DagBag sem erros de sintaxe ou importação."""
        self.assertEqual(len(self.dagbag.import_errors), 0, f"Erros de importação no DagBag: {self.dagbag.import_errors}")
        self.assertIsNotNone(self.dag, "DAG 'r6_pipeline_transferegov_e2e' não encontrada.")

    def test_dag_configuration(self):
        """Valida configurações fundamentais de schedule, concorrência e renderização de tipos nativos."""
        self.assertIsNone(self.dag.schedule_interval)
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)
        self.assertEqual(self.dag.default_args.get("owner"), "airflow")
        self.assertFalse(self.dag.default_args.get("depends_on_past"))
        self.assertEqual(self.dag.default_args.get("retries"), 0)
        self.assertTrue(self.dag.render_template_as_native_obj, "render_template_as_native_obj deve ser True para preservar booleanos")
        self.assertIn("r6", self.dag.tags)
        self.assertIn("e2e", self.dag.tags)

    def test_params_configuration(self):
        """Valida a existência e tipo do parâmetro force_bronze."""
        self.assertIn("force_bronze", self.dag.params)
        param_obj = self.dag.params.get_param("force_bronze")
        self.assertFalse(param_obj.value)
        self.assertEqual(param_obj.schema.get("type"), "boolean")

    def test_task_inventory_and_types(self):
        """Verifica a presença exata das 6 tarefas e seus respectivos operadores."""
        task_ids = {t.task_id for t in self.dag.tasks}
        expected_task_ids = {
            "start",
            "trigger_bronze",
            "decidir_pos_bronze",
            "no_change",
            "trigger_transformacoes",
            "end",
        }
        self.assertEqual(task_ids, expected_task_ids)

        task_dict = {t.task_id: t for t in self.dag.tasks}
        self.assertIsInstance(task_dict["start"], EmptyOperator)
        self.assertIsInstance(task_dict["trigger_bronze"], TriggerDagRunOperator)
        self.assertIsInstance(task_dict["decidir_pos_bronze"], BranchPythonOperator)
        self.assertIsInstance(task_dict["no_change"], EmptyOperator)
        self.assertIsInstance(task_dict["trigger_transformacoes"], TriggerDagRunOperator)
        self.assertIsInstance(task_dict["end"], EmptyOperator)

    def test_trigger_bronze_configuration(self):
        """Valida configuração do disparador da DAG R2."""
        task = self.dag.get_task("trigger_bronze")
        self.assertEqual(task.trigger_dag_id, "r2_ingestao_transferegov_bronze")
        self.assertTrue(task.wait_for_completion)
        self.assertEqual(task.allowed_states, ["success"])
        self.assertEqual(task.failed_states, ["failed"])
        self.assertEqual(task.trigger_run_id, "r6b_bronze__{{ ts_nodash }}")
        self.assertIn("force", task.conf)
        self.assertIn("ingestion_run_id", task.conf)
        self.assertIn("e2e_parent_run_id", task.conf)

    def test_trigger_transformacoes_configuration(self):
        """Valida configuração do disparador da DAG R6-A."""
        task = self.dag.get_task("trigger_transformacoes")
        self.assertEqual(task.trigger_dag_id, "r6_transformacoes_lakehouse")
        self.assertTrue(task.wait_for_completion)
        self.assertEqual(task.allowed_states, ["success"])
        self.assertEqual(task.failed_states, ["failed"])
        self.assertEqual(task.trigger_run_id, "r6b_transform__{{ ts_nodash }}")
        self.assertIn("e2e_parent_run_id", task.conf)
        self.assertIn("source_ingestion_run_id", task.conf)

    def test_end_trigger_rule(self):
        """Valida a trigger rule especial do join final para acomodar ramificações."""
        end_task = self.dag.get_task("end")
        self.assertEqual(
            end_task.trigger_rule,
            TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            "A tarefa 'end' deve usar NONE_FAILED_MIN_ONE_SUCCESS para convergir após ramificação"
        )

    def test_topology_dependencies(self):
        """Valida o grafo de dependências e ramificações."""
        task_dict = {t.task_id: t for t in self.dag.tasks}
        start = task_dict["start"]
        trigger_bronze = task_dict["trigger_bronze"]
        decidir_pos_bronze = task_dict["decidir_pos_bronze"]
        no_change = task_dict["no_change"]
        trigger_transformacoes = task_dict["trigger_transformacoes"]
        end = task_dict["end"]

        self.assertIn(start, trigger_bronze.upstream_list)
        self.assertIn(trigger_bronze, decidir_pos_bronze.upstream_list)

        self.assertIn(decidir_pos_bronze, no_change.upstream_list)
        self.assertIn(decidir_pos_bronze, trigger_transformacoes.upstream_list)

        self.assertIn(no_change, end.upstream_list)
        self.assertIn(trigger_transformacoes, end.upstream_list)

    @patch("pyhive.hive.Connection")
    def test_decidir_pos_bronze_no_change(self, mock_conn_cls):
        """Valida retorno do ramo 'no_change' quando auditoria registra NO_CHANGE."""
        callable_fn = self.dag.get_task("decidir_pos_bronze").python_callable

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("NO_CHANGE",)]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn_cls.return_value = mock_conn

        context = {"ts_nodash": "20260917T120000", "dag_run": MagicMock(conf={"ingestion_run_id": "e2e_20260917T120000"})}
        result = callable_fn(**context)
        self.assertEqual(result, "no_change")

    @patch("pyhive.hive.Connection")
    def test_decidir_pos_bronze_success(self, mock_conn_cls):
        """Valida retorno do ramo 'trigger_transformacoes' quando auditoria registra SUCCESS."""
        callable_fn = self.dag.get_task("decidir_pos_bronze").python_callable

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("SUCCESS",)]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn_cls.return_value = mock_conn

        context = {"ts_nodash": "20260917T120000", "dag_run": MagicMock(conf={"ingestion_run_id": "e2e_20260917T120000"})}
        result = callable_fn(**context)
        self.assertEqual(result, "trigger_transformacoes")

    @patch("pyhive.hive.Connection")
    def test_decidir_pos_bronze_zero_rows_raises(self, mock_conn_cls):
        """Valida lançamento de exceção quando não há registro correspondente no log de auditoria."""
        callable_fn = self.dag.get_task("decidir_pos_bronze").python_callable

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn_cls.return_value = mock_conn

        context = {"ts_nodash": "20260917T120000", "dag_run": MagicMock(conf={"ingestion_run_id": "e2e_20260917T120000"})}
        with self.assertRaises(AirflowException) as cm:
            callable_fn(**context)
        self.assertIn("Nenhum registro de auditoria encontrado", str(cm.exception))

    @patch("pyhive.hive.Connection")
    def test_decidir_pos_bronze_multiple_rows_raises(self, mock_conn_cls):
        """Valida lançamento de exceção quando há múltiplos registros para a mesma chave de auditoria."""
        callable_fn = self.dag.get_task("decidir_pos_bronze").python_callable

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("SUCCESS",), ("SUCCESS",)]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn_cls.return_value = mock_conn

        context = {"ts_nodash": "20260917T120000", "dag_run": MagicMock(conf={"ingestion_run_id": "e2e_20260917T120000"})}
        with self.assertRaises(AirflowException) as cm:
            callable_fn(**context)
        self.assertIn("Múltiplos registros", str(cm.exception))

    @patch("pyhive.hive.Connection")
    def test_decidir_pos_bronze_invalid_status_raises(self, mock_conn_cls):
        """Valida lançamento de exceção quando o status retornado não é nem NO_CHANGE nem SUCCESS."""
        callable_fn = self.dag.get_task("decidir_pos_bronze").python_callable

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("RUNNING",)]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn_cls.return_value = mock_conn

        context = {"ts_nodash": "20260917T120000", "dag_run": MagicMock(conf={"ingestion_run_id": "e2e_20260917T120000"})}
        with self.assertRaises(AirflowException) as cm:
            callable_fn(**context)
        self.assertIn("Status inesperado ou inválido", str(cm.exception))

    def test_decidir_pos_bronze_invalid_id_characters_raises(self):
        """Valida lançamento de exceção quando ingestion_run_id contém caracteres inseguros."""
        callable_fn = self.dag.get_task("decidir_pos_bronze").python_callable

        context = {"ts_nodash": "20260917T120000", "dag_run": MagicMock(conf={"ingestion_run_id": "e2e/hack;--drop"})}
        with self.assertRaises(AirflowException) as cm:
            callable_fn(**context)
        self.assertIn("caracteres inválidos", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
