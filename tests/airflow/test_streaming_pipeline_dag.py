"""
Unit tests for streaming_processing_dag structure.

Tests verify:
- DAG contains expected TaskGroups
- Each TaskGroup contains expected tasks
- Dependencies are set correctly between TaskGroups
- Health checks precede streaming jobs
"""

import pytest
import sys
import os
from pathlib import Path

# Add dags directory to path for imports
dags_path = Path(__file__).parent.parent.parent / 'dags'
sys.path.insert(0, str(dags_path))

# Set environment variables for DAG imports
os.environ.setdefault('AIRFLOW__CORE__DAGS_FOLDER', str(dags_path))
os.environ.setdefault('AIRFLOW__CORE__UNIT_TEST_MODE', 'True')

try:
    from airflow.models import DagBag
    from airflow.utils.task_group import TaskGroup
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    
    # Create mock test that documents the requirement
    def test_airflow_not_installed():
        """
        Airflow is not installed in the development environment.
        
        These tests are designed to run in the Airflow Docker container
        where Airflow is installed. To run these tests:
        
        1. Start Airflow with docker-compose up
        2. Execute tests inside the container:
           docker exec airflow-scheduler pytest /opt/airflow/tests/airflow/test_streaming_pipeline_dag.py
        
        The DAG structure can be verified manually by:
        1. Accessing Airflow UI at http://localhost:8080
        2. Checking that streaming_processing_dag DAG appears
        3. Verifying tasks in Graph view
        """
        pytest.skip("Airflow not installed - tests should run in Docker container")
    
    pytest.skip("Airflow not installed - tests should run in Docker container", allow_module_level=True)


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
        assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"
    
    def test_dag_has_correct_properties(self, dag):
        """Test DAG has correct configuration."""
        assert dag.dag_id == 'streaming_processing_dag'
        assert dag.schedule_interval == '* * * * *'  # Every minute
        assert dag.catchup is False
        assert 'streaming' in dag.tags
        assert 'spark' in dag.tags
        assert 'processing' in dag.tags
    
    # ==========================================================================
    # TaskGroup Structure Tests (Requirements 1.1-1.5)
    # ==========================================================================
    
    def test_dag_contains_health_checks_taskgroup(self, dag):
        """Test DAG contains health_checks TaskGroup with expected tasks."""
        task_group = dag.task_group_dict.get('health_checks')
        assert task_group is not None, "health_checks TaskGroup not found"
        
        # Get task IDs within the TaskGroup (prefixed with group name)
        task_ids = [task.task_id for task in dag.tasks]
        
        assert 'health_checks.test_redis_health' in task_ids, "test_redis_health not in health_checks"
        assert 'health_checks.test_postgres_health' in task_ids, "test_postgres_health not in health_checks"
        assert 'health_checks.test_minio_health' in task_ids, "test_minio_health not in health_checks"
    
    def test_dag_contains_trade_aggregation_taskgroup(self, dag):
        """Test DAG contains trade_aggregation TaskGroup with expected tasks."""
        task_group = dag.task_group_dict.get('trade_aggregation')
        assert task_group is not None, "trade_aggregation TaskGroup not found"
        
        task_ids = [task.task_id for task in dag.tasks]
        
        assert 'trade_aggregation.run_trade_aggregation_job' in task_ids, \
            "run_trade_aggregation_job not in trade_aggregation"
        assert 'trade_aggregation.validate_aggregation_output' in task_ids, \
            "validate_aggregation_output not in trade_aggregation"
    
    def test_dag_contains_technical_indicators_taskgroup(self, dag):
        """Test DAG contains technical_indicators TaskGroup with expected tasks."""
        task_group = dag.task_group_dict.get('technical_indicators')
        assert task_group is not None, "technical_indicators TaskGroup not found"
        
        task_ids = [task.task_id for task in dag.tasks]
        
        assert 'technical_indicators.run_technical_indicators_job' in task_ids, \
            "run_technical_indicators_job not in technical_indicators"
        assert 'technical_indicators.validate_indicators_output' in task_ids, \
            "validate_indicators_output not in technical_indicators"
    
    def test_dag_contains_anomaly_detection_taskgroup(self, dag):
        """Test DAG contains anomaly_detection TaskGroup with expected tasks."""
        task_group = dag.task_group_dict.get('anomaly_detection')
        assert task_group is not None, "anomaly_detection TaskGroup not found"
        
        task_ids = [task.task_id for task in dag.tasks]
        
        assert 'anomaly_detection.run_anomaly_detection_job' in task_ids, \
            "run_anomaly_detection_job not in anomaly_detection"
        assert 'anomaly_detection.validate_anomaly_output' in task_ids, \
            "validate_anomaly_output not in anomaly_detection"
    
    def test_dag_contains_cleanup_taskgroup(self, dag):
        """Test DAG contains cleanup TaskGroup with expected tasks."""
        task_group = dag.task_group_dict.get('cleanup')
        assert task_group is not None, "cleanup TaskGroup not found"
        
        task_ids = [task.task_id for task in dag.tasks]
        
        assert 'cleanup.cleanup_streaming' in task_ids, "cleanup_streaming not in cleanup"
    
    def test_all_taskgroups_exist(self, dag):
        """Test all expected TaskGroups exist in the DAG."""
        expected_groups = ['health_checks', 'trade_aggregation', 'technical_indicators', 
                          'anomaly_detection', 'cleanup']
        
        for group_name in expected_groups:
            assert group_name in dag.task_group_dict, f"TaskGroup '{group_name}' not found"
    
    # ==========================================================================
    # TaskGroup Dependency Tests (Requirement 1.5)
    # ==========================================================================
    
    def test_health_checks_upstream_of_trade_aggregation(self, dag):
        """Test health_checks TaskGroup is upstream of trade_aggregation."""
        # Get the first task in trade_aggregation group
        aggregation_job = dag.get_task('trade_aggregation.run_trade_aggregation_job')
        upstream_ids = [t.task_id for t in aggregation_job.upstream_list]
        
        # Health check tasks should be upstream
        assert 'health_checks.test_redis_health' in upstream_ids, \
            "Redis health check should be upstream of trade aggregation"
        assert 'health_checks.test_postgres_health' in upstream_ids, \
            "PostgreSQL health check should be upstream of trade aggregation"
        assert 'health_checks.test_minio_health' in upstream_ids, \
            "MinIO health check should be upstream of trade aggregation"
    
    def test_trade_aggregation_upstream_of_technical_indicators(self, dag):
        """Test trade_aggregation TaskGroup is upstream of technical_indicators."""
        indicators_job = dag.get_task('technical_indicators.run_technical_indicators_job')
        upstream_ids = [t.task_id for t in indicators_job.upstream_list]
        
        # Validation task from trade_aggregation should be upstream
        assert 'trade_aggregation.validate_aggregation_output' in upstream_ids, \
            "trade_aggregation validation should be upstream of technical_indicators"
    
    def test_technical_indicators_upstream_of_anomaly_detection(self, dag):
        """Test technical_indicators TaskGroup is upstream of anomaly_detection."""
        anomaly_job = dag.get_task('anomaly_detection.run_anomaly_detection_job')
        upstream_ids = [t.task_id for t in anomaly_job.upstream_list]
        
        # Validation task from technical_indicators should be upstream
        assert 'technical_indicators.validate_indicators_output' in upstream_ids, \
            "technical_indicators validation should be upstream of anomaly_detection"
    
    def test_anomaly_detection_upstream_of_cleanup(self, dag):
        """Test anomaly_detection TaskGroup is upstream of cleanup."""
        cleanup_task = dag.get_task('cleanup.cleanup_streaming')
        upstream_ids = [t.task_id for t in cleanup_task.upstream_list]
        
        # Validation task from anomaly_detection should be upstream
        assert 'anomaly_detection.validate_anomaly_output' in upstream_ids, \
            "anomaly_detection validation should be upstream of cleanup"
    
    # ==========================================================================
    # Internal TaskGroup Dependency Tests
    # ==========================================================================
    
    def test_trade_aggregation_internal_dependencies(self, dag):
        """Test internal dependencies within trade_aggregation TaskGroup."""
        validate_task = dag.get_task('trade_aggregation.validate_aggregation_output')
        upstream_ids = [t.task_id for t in validate_task.upstream_list]
        
        assert 'trade_aggregation.run_trade_aggregation_job' in upstream_ids, \
            "run_trade_aggregation_job should be upstream of validate_aggregation_output"
    
    def test_technical_indicators_internal_dependencies(self, dag):
        """Test internal dependencies within technical_indicators TaskGroup."""
        validate_task = dag.get_task('technical_indicators.validate_indicators_output')
        upstream_ids = [t.task_id for t in validate_task.upstream_list]
        
        assert 'technical_indicators.run_technical_indicators_job' in upstream_ids, \
            "run_technical_indicators_job should be upstream of validate_indicators_output"
    
    def test_anomaly_detection_internal_dependencies(self, dag):
        """Test internal dependencies within anomaly_detection TaskGroup."""
        validate_task = dag.get_task('anomaly_detection.validate_anomaly_output')
        upstream_ids = [t.task_id for t in validate_task.upstream_list]
        
        assert 'anomaly_detection.run_anomaly_detection_job' in upstream_ids, \
            "run_anomaly_detection_job should be upstream of validate_anomaly_output"
    
    # ==========================================================================
    # General DAG Tests
    # ==========================================================================
    
    def test_dag_has_no_cycles(self, dag):
        """Test DAG has no circular dependencies."""
        for task in dag.tasks:
            upstream_tasks = task.get_flat_relatives(upstream=True)
            assert task not in upstream_tasks, f"Task {task.task_id} has circular dependency"
    
    def test_default_args_configured(self, dag):
        """Test DAG has proper default_args."""
        assert dag.default_args is not None
        assert 'owner' in dag.default_args
        assert dag.default_args['owner'] == 'data-engineering'
        assert 'retries' in dag.default_args
        assert dag.default_args['retries'] == 2
    
    def test_dag_task_count(self, dag):
        """Test DAG has expected number of tasks."""
        # 3 health checks + 2 trade_aggregation + 2 technical_indicators + 
        # 2 anomaly_detection + 1 cleanup = 10 tasks
        expected_task_count = 10
        actual_task_count = len(dag.tasks)
        assert actual_task_count == expected_task_count, \
            f"Expected {expected_task_count} tasks, found {actual_task_count}"
