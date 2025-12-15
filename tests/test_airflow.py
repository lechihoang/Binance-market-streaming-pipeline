"""
Consolidated test module for Airflow DAGs and Data Quality.
Contains all tests for DAG syntax, health checks, field validation, completeness checks, and logging.

Table of Contents:
- Imports and Setup (line ~20)
- DAG Syntax Tests (line ~80)
- Health Check Tests (line ~200)
- Field Validation Tests (line ~350)
- Completeness Check Tests (line ~500)
- Logging Property Tests (line ~650)
- Integration Tests (line ~800)

Requirements: 6.5
"""

# ============================================================================
# IMPORTS AND SETUP
# ============================================================================

import pytest
import ast
import os
import sys
import tempfile
from pathlib import Path
from datetime import datetime

# Add dags directory to path
dags_path = Path(__file__).parent.parent / 'dags'
sys.path.insert(0, str(dags_path))

# Set environment variables for DAG imports
os.environ.setdefault('AIRFLOW__CORE__DAGS_FOLDER', str(dags_path))
os.environ.setdefault('AIRFLOW__CORE__UNIT_TEST_MODE', 'True')

# Check if airflow is available
try:
    from airflow import DAG
    from airflow.models import DagBag
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    DAG = None
    DagBag = None


# ============================================================================
# DAG SYNTAX TESTS
# ============================================================================

class TestStreamingPipelineDAGSyntax:
    """Test streaming_processing_dag.py syntax and structure."""
    
    @pytest.fixture
    def dag_file_path(self):
        """Get path to streaming_processing_dag.py."""
        return dags_path / 'streaming_processing_dag.py'
    
    @pytest.fixture
    def dag_ast(self, dag_file_path):
        """Parse DAG file into AST."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        return ast.parse(content)
    
    def test_dag_file_exists(self, dag_file_path):
        """Test that streaming_processing_dag.py exists."""
        assert dag_file_path.exists(), "streaming_processing_dag.py not found"
    
    def test_dag_file_has_valid_syntax(self, dag_file_path):
        """Test DAG file has valid Python syntax."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        
        try:
            ast.parse(content)
        except SyntaxError as e:
            pytest.fail(f"DAG file has syntax error: {e}")
    
    def test_dag_imports_required_modules(self, dag_ast):
        """Test DAG imports required Airflow modules."""
        imports = []
        for node in ast.walk(dag_ast):
            if isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.append(node.module)
        
        assert 'airflow' in imports or any('airflow' in imp for imp in imports)
        assert any('operators' in imp for imp in imports)
    
    def test_dag_has_health_checks(self, dag_file_path):
        """Test DAG file contains health check tasks for 3-tier storage."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        
        assert 'test_redis_health' in content
        assert 'test_postgres_health' in content
        assert 'test_minio_health' in content
    
    def test_dag_has_run_tasks(self, dag_file_path):
        """Test DAG has run tasks for streaming jobs.
        
        Note: run_technical_indicators_job was removed as part of simplify-indicators
        feature. The DAG now only has trade_aggregation and anomaly_detection jobs.
        See: .kiro/specs/simplify-indicators/requirements.md
        """
        with open(dag_file_path, 'r') as f:
            content = f.read()
        
        # Verify expected tasks exist
        assert 'run_trade_aggregation_job' in content
        assert 'run_anomaly_detection_job' in content
        
        # Verify technical_indicators tasks do NOT exist (removed in simplify-indicators)
        assert 'run_technical_indicators_job' not in content, \
            "run_technical_indicators_job should have been removed"
        assert 'validate_indicators_output' not in content, \
            "validate_indicators_output should have been removed"
    
    def test_dag_sets_dependencies(self, dag_file_path):
        """Test DAG sets dependencies between tasks."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        
        assert '>>' in content


# ============================================================================
# NOTE: Health check, field validation, completeness check, and logging tests
# were removed because they depended on the data_quality module which no longer exists.
# The actual health check functionality is now in src/storage/ modules.
# ============================================================================


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

@pytest.mark.skipif(not AIRFLOW_AVAILABLE, reason="Airflow is not installed")
class TestFullPipelineExecution:
    """Integration tests for full pipeline execution."""
    
    @pytest.fixture
    def streaming_dag(self):
        """Load streaming_processing_dag."""
        try:
            from streaming_processing_dag import dag
            return dag
        except ImportError:
            pytest.skip("streaming_processing_dag not available")
    
    def test_streaming_pipeline_structure(self, streaming_dag):
        """Test that streaming processing DAG has correct structure."""
        assert streaming_dag is not None
        assert streaming_dag.dag_id == 'streaming_processing_dag'
        
        tasks = streaming_dag.tasks
        task_ids = [t.task_id for t in tasks]
        
        assert 'test_redis_health' in task_ids or 'health_checks.test_redis_health' in task_ids
        assert 'run_trade_aggregation_job' in task_ids or 'trade_aggregation.run_trade_aggregation_job' in task_ids


@pytest.mark.skipif(not AIRFLOW_AVAILABLE, reason="Airflow is not installed")
class TestAutoDiscovery:
    """Integration tests for DAG auto-discovery."""
    
    def test_dag_bag_loads_existing_dags(self):
        """Test that DagBag can load existing DAGs from dags/ directory."""
        dagbag = DagBag(dag_folder=str(dags_path), include_examples=False)
        
        assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"
        
        dag_ids = list(dagbag.dag_ids)
        assert 'streaming_processing_dag' in dag_ids
    
    def test_new_dag_can_be_discovered(self):
        """Test that a new DAG file can be discovered by DagBag."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_dag_content = '''
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_task(**context):
    return "Test task executed"

with DAG(
    dag_id='test_new_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['test'],
) as dag:
    task = PythonOperator(
        task_id='test_task',
        python_callable=test_task,
    )
'''
            
            test_dag_path = os.path.join(temp_dir, 'test_new_dag.py')
            with open(test_dag_path, 'w') as f:
                f.write(test_dag_content)
            
            dagbag = DagBag(dag_folder=temp_dir, include_examples=False)
            
            assert len(dagbag.import_errors) == 0
            assert 'test_new_dag' in dagbag.dag_ids


@pytest.mark.skipif(not AIRFLOW_AVAILABLE, reason="Airflow is not installed")
class TestStreamingPipelineDAG:
    """Test suite for streaming_processing_dag structure."""
    
    @pytest.fixture(scope='class')
    def dagbag(self):
        """Load DAGs from dags directory."""
        return DagBag(dag_folder=str(dags_path), include_examples=False)
    
    @pytest.fixture(scope='class')
    def dag(self, dagbag):
        """Get streaming_processing_dag DAG."""
        dag_id = 'streaming_processing_dag'
        assert dag_id in dagbag.dags, f"DAG {dag_id} not found in DagBag"
        return dagbag.dags[dag_id]
    
    def test_dag_loaded(self, dagbag):
        """Test that streaming_processing_dag DAG is loaded without errors."""
        assert 'streaming_processing_dag' in dagbag.dags
        assert len(dagbag.import_errors) == 0
    
    def test_dag_has_correct_properties(self, dag):
        """Test DAG has correct configuration."""
        assert dag.dag_id == 'streaming_processing_dag'
        assert dag.schedule_interval == '* * * * *'
        assert dag.catchup is False
        assert 'streaming' in dag.tags
    
    def test_dag_has_no_cycles(self, dag):
        """Test DAG has no circular dependencies."""
        for task in dag.tasks:
            upstream_tasks = task.get_flat_relatives(upstream=True)
            assert task not in upstream_tasks, f"Task {task.task_id} has circular dependency"
    
    def test_dag_does_not_contain_technical_indicators(self, dag):
        """Test DAG does not contain technical_indicators tasks (removed in simplify-indicators).
        
        The technical_indicators TaskGroup was removed as part of the simplify-indicators
        feature to reduce complexity for regular users.
        See: .kiro/specs/simplify-indicators/requirements.md
        """
        task_ids = [t.task_id for t in dag.tasks]
        
        # Verify no technical_indicators tasks exist
        for task_id in task_ids:
            assert 'technical_indicators' not in task_id, \
                f"Found technical_indicators task '{task_id}' which should have been removed"
    
    def test_dag_has_correct_task_groups(self, dag):
        """Test DAG has the expected TaskGroups after simplify-indicators refactoring.
        
        Expected structure:
        - health_checks: test_redis_health, test_postgres_health, test_minio_health
        - trade_aggregation: run_trade_aggregation_job, validate_aggregation_output
        - anomaly_detection: run_anomaly_detection_job, validate_anomaly_output
        - cleanup_streaming (standalone task)
        """
        task_ids = [t.task_id for t in dag.tasks]
        
        # Verify health_checks tasks
        assert any('test_redis_health' in tid for tid in task_ids), "Missing test_redis_health task"
        assert any('test_postgres_health' in tid for tid in task_ids), "Missing test_postgres_health task"
        assert any('test_minio_health' in tid for tid in task_ids), "Missing test_minio_health task"
        
        # Verify trade_aggregation tasks
        assert any('run_trade_aggregation_job' in tid for tid in task_ids), "Missing run_trade_aggregation_job task"
        assert any('validate_aggregation_output' in tid for tid in task_ids), "Missing validate_aggregation_output task"
        
        # Verify anomaly_detection tasks
        assert any('run_anomaly_detection_job' in tid for tid in task_ids), "Missing run_anomaly_detection_job task"
        assert any('validate_anomaly_output' in tid for tid in task_ids), "Missing validate_anomaly_output task"
        
        # Verify cleanup task
        assert 'cleanup_streaming' in task_ids, "Missing cleanup_streaming task"
