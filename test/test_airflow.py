"""Tests for Airflow DAG structure and syntax validation."""

import ast
from pathlib import Path

import pytest

DAG_DIR = Path(__file__).parent.parent / "dags"
DAG_FILE = DAG_DIR / "streaming_processing_dag.py"


# ==========================================
# DAG File Validation
# ==========================================


class TestDAGFileValidation:
    def test_dag_file_exists(self):
        assert DAG_FILE.exists(), f"DAG file not found: {DAG_FILE}"

    def test_dag_file_valid_python_syntax(self):
        source = DAG_FILE.read_text()
        try:
            ast.parse(source)
        except SyntaxError as e:
            pytest.fail(f"DAG file has invalid Python syntax: {e}")

    def test_dag_file_imports_airflow(self):
        source = DAG_FILE.read_text()
        tree = ast.parse(source)
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.append(node.module)
        assert any("airflow" in imp for imp in imports)

    def test_dag_file_imports_operators(self):
        source = DAG_FILE.read_text()
        tree = ast.parse(source)
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                imports.append(node.module)
        assert any("operators" in imp for imp in imports)


# ==========================================
# DAG Structure
# ==========================================


class TestDAGStructure:
    def setup_method(self):
        self.source = DAG_FILE.read_text()

    def test_dag_id(self):
        assert 'dag_id="streaming_processing_dag"' in self.source

    def test_schedule_every_5_minutes(self):
        assert "*/5 * * * *" in self.source

    def test_catchup_disabled(self):
        assert "catchup=False" in self.source

    def test_has_health_checks_group(self):
        assert '"health_checks"' in self.source

    def test_has_redis_health_task(self):
        assert 'task_id="test_redis_health"' in self.source

    def test_has_postgres_health_task(self):
        assert 'task_id="test_postgres_health"' in self.source

    def test_no_minio_health_check(self):
        assert "test_minio_health" not in self.source

    def test_has_trade_aggregation_group(self):
        assert '"trade_aggregation"' in self.source

    def test_has_trade_aggregation_job(self):
        assert 'task_id="run_trade_aggregation_job"' in self.source

    def test_has_anomaly_detection_group(self):
        assert '"anomaly_detection"' in self.source

    def test_has_anomaly_detection_job(self):
        assert 'task_id="run_anomaly_detection_job"' in self.source

    def test_has_volatility_prediction_group(self):
        assert '"volatility_prediction"' in self.source

    def test_has_cleanup_task(self):
        assert 'task_id="cleanup_streaming"' in self.source

    def test_has_streaming_tag(self):
        assert '"streaming"' in self.source

    def test_no_technical_indicators(self):
        assert "run_technical_indicators_job" not in self.source
        assert "validate_indicators_output" not in self.source

    def test_task_dependency_health_first(self):
        assert "health_checks >> trade_aggregation" in self.source

    def test_has_task_dependencies(self):
        assert ">>" in self.source
