import unittest
from unittest.mock import MagicMock
from pathlib import Path
from airflow.models import DagBag
from airflow.exceptions import AirflowException
from airflow.utils.state import TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule


class TestR2BronzeDagContract(unittest.TestCase):
    """
    Testes de contrato estrutural, governança e propagação de falhas da DAG R2
    (r2_ingestao_transferegov_bronze).
    """

    @classmethod
    def setUpClass(cls):
        dag_folder = Path(__file__).resolve().parent.parent.parent / "airflow" / "dags"
        cls.dagbag = DagBag(dag_folder=str(dag_folder), include_examples=False)
        cls.dag = cls.dagbag.get_dag("r2_ingestao_transferegov_bronze")

    def test_dag_loaded_without_errors(self):
        """Valida que a DAG R2 foi carregada sem erros de importação."""
        self.assertEqual(len(self.dagbag.import_errors), 0, f"Erros de importação no DagBag: {self.dagbag.import_errors}")
        self.assertIsNotNone(self.dag, "DAG 'r2_ingestao_transferegov_bronze' não encontrada.")

    def test_dag_configuration(self):
        """Valida configurações de execução manual e concorrência."""
        self.assertIsNone(self.dag.schedule_interval)
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)
        self.assertEqual(self.dag.default_args.get("owner"), "airflow")
        self.assertEqual(self.dag.default_args.get("retries"), 0)

    def test_params_contract(self):
        """Valida parâmetros da DAG: force e ingestion_run_id."""
        self.assertIn("force", self.dag.params)
        force_p = self.dag.params.get_param("force")
        self.assertFalse(force_p.value)
        self.assertEqual(force_p.schema.get("type"), "boolean")

        self.assertIn("ingestion_run_id", self.dag.params)
        run_id_p = self.dag.params.get_param("ingestion_run_id")
        self.assertEqual(run_id_p.value, "")
        self.assertEqual(run_id_p.schema.get("type"), "string")

    def test_run_id_resolution_custom(self):
        """Valida resolução de run_id quando ingestion_run_id é explicitamente fornecido."""
        callable_fn = None
        for t in self.dag.tasks:
            if t.task_id == "avaliar_necessidade":
                callable_fn = t.python_callable
                break
        self.assertIsNotNone(callable_fn)

        # Importando helper de resolução
        # Inspecionando o closure/módulo da função
        mod = __import__(callable_fn.__module__, fromlist=["get_ingestion_run_id"])
        get_id_fn = getattr(mod, "get_ingestion_run_id")

        context = {
            "ts_nodash": "20260917T120000",
            "params": {"ingestion_run_id": "e2e_custom_12345"}
        }
        self.assertEqual(get_id_fn(context), "e2e_custom_12345")

    def test_run_id_resolution_fallback(self):
        """Valida fallback retrocompatível run_{ts_nodash} quando ingestion_run_id está vazio."""
        callable_fn = self.dag.get_task("avaliar_necessidade").python_callable
        mod = __import__(callable_fn.__module__, fromlist=["get_ingestion_run_id"])
        get_id_fn = getattr(mod, "get_ingestion_run_id")

        context_empty = {
            "ts_nodash": "20260917T120000",
            "params": {"ingestion_run_id": ""}
        }
        self.assertEqual(get_id_fn(context_empty), "run_20260917T120000")

        context_none = {
            "ts_nodash": "20260917T120000",
            "params": {}
        }
        self.assertEqual(get_id_fn(context_none), "run_20260917T120000")

    def test_run_id_resolution_invalid_chars(self):
        """Valida rejeição de caracteres de path traversal ou injeção em ingestion_run_id."""
        callable_fn = self.dag.get_task("avaliar_necessidade").python_callable
        mod = __import__(callable_fn.__module__, fromlist=["get_ingestion_run_id"])
        get_id_fn = getattr(mod, "get_ingestion_run_id")

        context_invalid = {
            "ts_nodash": "20260917T120000",
            "params": {"ingestion_run_id": "../etc/passwd"}
        }
        with self.assertRaises(ValueError) as cm:
            get_id_fn(context_invalid)
        self.assertIn("caracteres inválidos", str(cm.exception))

    def test_finalizar_execucao_r2_is_the_only_leaf(self):
        """
        Finding R6-B-P0: Garante que finalizar_execucao_r2 é a única folha (leaf task) da DAG R2,
        eliminando o risco de limpeza_temporarios mascarar falhas operacionais.
        """
        leaf_tasks = [t for t in self.dag.tasks if len(t.downstream_list) == 0]
        self.assertEqual(len(leaf_tasks), 1, f"Deve haver exatamente 1 leaf task, encontrados: {[t.task_id for t in leaf_tasks]}")
        self.assertEqual(leaf_tasks[0].task_id, "finalizar_execucao_r2")

    def test_cleanup_and_finalizer_trigger_rules(self):
        """Valida que limpeza_temporarios e finalizar_execucao_r2 utilizam ALL_DONE."""
        limpeza = self.dag.get_task("limpeza_temporarios")
        finalizar = self.dag.get_task("finalizar_execucao_r2")

        self.assertEqual(limpeza.trigger_rule, TriggerRule.ALL_DONE)
        self.assertEqual(finalizar.trigger_rule, TriggerRule.ALL_DONE)
        self.assertIn(finalizar, limpeza.downstream_list)

    def test_verificar_resultado_r2_propagates_failed(self):
        """Valida que falha em qualquer tarefa upstream dispara AirflowException."""
        fn = self.dag.get_task("finalizar_execucao_r2").python_callable

        ti_mock = MagicMock()
        ti_mock.task_id = "finalizar_execucao_r2"

        ti_ok = MagicMock(task_id="preflight_check", state=TaskInstanceState.SUCCESS)
        ti_fail = MagicMock(task_id="ingestao_siconv_proposta", state=TaskInstanceState.FAILED)

        dag_run_mock = MagicMock()
        dag_run_mock.get_task_instances.return_value = [ti_ok, ti_fail, ti_mock]

        context = {"dag_run": dag_run_mock, "ti": ti_mock}
        with self.assertRaises(AirflowException) as cm:
            fn(**context)
        self.assertIn("ingestao_siconv_proposta (failed)", str(cm.exception))

    def test_verificar_resultado_r2_propagates_upstream_failed(self):
        """Valida que tarefas em upstream_failed disparam AirflowException."""
        fn = self.dag.get_task("finalizar_execucao_r2").python_callable

        ti_mock = MagicMock()
        ti_mock.task_id = "finalizar_execucao_r2"

        ti_upfail = MagicMock(task_id="controle_final", state=TaskInstanceState.UPSTREAM_FAILED)

        dag_run_mock = MagicMock()
        dag_run_mock.get_task_instances.return_value = [ti_upfail, ti_mock]

        context = {"dag_run": dag_run_mock, "ti": ti_mock}
        with self.assertRaises(AirflowException) as cm:
            fn(**context)
        self.assertIn("controle_final (upstream_failed)", str(cm.exception))

    def test_verificar_resultado_r2_detects_cleanup_failure(self):
        """Valida que falha na própria limpeza_temporarios é detectada como falha operacional."""
        fn = self.dag.get_task("finalizar_execucao_r2").python_callable

        ti_mock = MagicMock()
        ti_mock.task_id = "finalizar_execucao_r2"

        ti_cleanup_fail = MagicMock(task_id="limpeza_temporarios", state=TaskInstanceState.FAILED)

        dag_run_mock = MagicMock()
        dag_run_mock.get_task_instances.return_value = [ti_cleanup_fail, ti_mock]

        context = {"dag_run": dag_run_mock, "ti": ti_mock}
        with self.assertRaises(AirflowException) as cm:
            fn(**context)
        self.assertIn("limpeza_temporarios (failed)", str(cm.exception))

    def test_verificar_resultado_r2_accepts_skipped_and_success(self):
        """Valida que combinação de tarefas SUCCESS e SKIPPED é aprovada com sucesso."""
        fn = self.dag.get_task("finalizar_execucao_r2").python_callable

        ti_mock = MagicMock()
        ti_mock.task_id = "finalizar_execucao_r2"

        # Simula ramo NO_CHANGE: tarefas analíticas em SKIPPED
        ti_pref = MagicMock(task_id="preflight_check", state=TaskInstanceState.SUCCESS)
        ti_ctrl = MagicMock(task_id="controle_inicial", state=TaskInstanceState.SUCCESS)
        ti_branch = MagicMock(task_id="avaliar_necessidade", state=TaskInstanceState.SUCCESS)
        ti_no_chg = MagicMock(task_id="finalizar_no_change", state=TaskInstanceState.SUCCESS)
        ti_ing_prog = MagicMock(task_id="ingestao_siconv_programa", state=TaskInstanceState.SKIPPED)
        ti_ing_prop = MagicMock(task_id="ingestao_siconv_proposta", state=TaskInstanceState.SKIPPED)
        ti_clean = MagicMock(task_id="limpeza_temporarios", state=TaskInstanceState.SUCCESS)

        dag_run_mock = MagicMock()
        dag_run_mock.get_task_instances.return_value = [
            ti_pref, ti_ctrl, ti_branch, ti_no_chg, ti_ing_prog, ti_ing_prop, ti_clean, ti_mock
        ]

        context = {"dag_run": dag_run_mock, "ti": ti_mock}
        # Não deve lançar exceção
        try:
            fn(**context)
        except AirflowException as e:
            self.fail(f"verificar_resultado_r2 lançou AirflowException indevidamente para branch normal com skipped: {e}")


if __name__ == "__main__":
    unittest.main()
