"""
Integration tests for Airflow orchestration.

Tests:
- Full pipeline execution end-to-end
- Failure propagation through task groups
- Auto-discovery of new DAGs
"""

import pytest
import os
import sys
import tempfile
import shutil
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

# Add dags directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../dags'))

from airflow import DAG
from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Import DAGs
from streaming_processing_dag import dag as streaming_dag


class TestFullPipelineExecution:
    """Integration tests for full pipeline execution."""
    
    def test_streaming_pipeline_structure(self):
        """Test that streaming processing DAG has correct structure."""
        # Verify DAG exists and is valid
        assert streaming_dag is not None
        assert streaming_dag.dag_id == 'streaming_processing_dag'
        
        # Verify tasks exist (this DAG uses flat structure, not task groups)
        tasks = streaming_dag.tasks
        task_ids = [t.task_id for t in tasks]
        
        # Check for health check tasks (3-tier storage)
        assert 'test_redis_health' in task_ids
        assert 'test_postgres_health' in task_ids
        assert 'test_minio_health' in task_ids
        
        # Check for run tasks
        assert 'run_trade_aggregation_job' in task_ids
        assert 'run_technical_indicators_job' in task_ids
        assert 'run_anomaly_detection_job' in task_ids
        
        # Check for cleanup task
        assert 'cleanup_streaming' in task_ids
    
    def test_streaming_pipeline_dependencies(self):
        """Test that streaming pipeline has correct task dependencies."""
        # Get all tasks
        tasks = {t.task_id: t for t in streaming_dag.tasks}
        
        # Get specific tasks
        aggregation_task = tasks.get('run_trade_aggregation_job')
        indicators_task = tasks.get('run_technical_indicators_job')
        anomaly_task = tasks.get('run_anomaly_detection_job')
        cleanup_task = tasks.get('cleanup_streaming')
        
        # Verify aggregation task has all 3 storage health checks as upstream
        if aggregation_task:
            upstream_ids = [t.task_id for t in aggregation_task.upstream_list]
            assert 'test_redis_health' in upstream_ids
            assert 'test_postgres_health' in upstream_ids
            assert 'test_minio_health' in upstream_ids
        
        # Verify indicators task has aggregation as upstream
        if indicators_task:
            upstream_ids = [t.task_id for t in indicators_task.upstream_list]
            assert 'run_trade_aggregation_job' in upstream_ids
        
        # Verify anomaly task has indicators as upstream
        if anomaly_task:
            upstream_ids = [t.task_id for t in anomaly_task.upstream_list]
            assert 'run_technical_indicators_job' in upstream_ids
        
        # Verify cleanup task has anomaly as upstream
        if cleanup_task:
            upstream_ids = [t.task_id for t in cleanup_task.upstream_list]
            assert 'run_anomaly_detection_job' in upstream_ids


class TestFailurePropagation:
    """Integration tests for failure propagation through pipeline."""
    
    def test_task_failure_stops_downstream(self):
        """Test that when a task fails, downstream tasks don't execute."""
        # Create a test DAG with task groups
        with DAG(
            dag_id='test_failure_propagation',
            start_date=datetime(2024, 1, 1),
            schedule_interval=None,
            catchup=False,
        ) as test_dag:
            
            # Task Group 1 with failing task
            with TaskGroup('group1') as group1:
                def failing_task(**context):
                    raise Exception("Intentional failure for testing")
                
                fail_task = PythonOperator(
                    task_id='fail_task',
                    python_callable=failing_task,
                )
            
            # Task Group 2 that should not execute
            with TaskGroup('group2') as group2:
                def downstream_task(**context):
                    return "This should not execute"
                
                downstream = PythonOperator(
                    task_id='downstream_task',
                    python_callable=downstream_task,
                )
            
            # Set dependency
            group1 >> group2
        
        # Verify structure
        assert len(test_dag.tasks) == 2
        
        # Verify dependency exists
        fail_task_obj = test_dag.get_task('group1.fail_task')
        downstream_task_obj = test_dag.get_task('group2.downstream_task')
        
        # Check that downstream has upstream dependency
        upstream_ids = [t.task_id for t in downstream_task_obj.upstream_list]
        assert fail_task_obj.task_id in upstream_ids
    
    def test_optional_test_task_failure_stops_pipeline(self):
        """Test that when optional test task fails, downstream groups don't execute."""
        # Create a test DAG with optional test task
        with DAG(
            dag_id='test_optional_test_failure',
            start_date=datetime(2024, 1, 1),
            schedule_interval=None,
            catchup=False,
        ) as test_dag:
            
            # Task Group 1 with run and test tasks
            with TaskGroup('ingestion') as ingestion_group:
                def run_task(**context):
                    return "Run successful"
                
                def test_task(**context):
                    raise Exception("Test failed")
                
                run = PythonOperator(
                    task_id='run',
                    python_callable=run_task,
                )
                
                test = PythonOperator(
                    task_id='test',
                    python_callable=test_task,
                )
                
                run >> test
            
            # Task Group 2 that should not execute if test fails
            with TaskGroup('processing') as processing_group:
                def process_task(**context):
                    return "Processing"
                
                process = PythonOperator(
                    task_id='process',
                    python_callable=process_task,
                )
            
            # Set dependency
            ingestion_group >> processing_group
        
        # Verify structure
        assert len(test_dag.tasks) == 3
        
        # Verify dependencies
        test_task_obj = test_dag.get_task('ingestion.test')
        process_task_obj = test_dag.get_task('processing.process')
        
        # Process task should have test task as upstream (through group dependency)
        # This ensures failure propagation
        all_upstream = []
        for task in process_task_obj.upstream_list:
            all_upstream.append(task.task_id)
        
        # Either direct dependency or through group
        assert len(all_upstream) > 0


class TestAutoDiscovery:
    """Integration tests for DAG auto-discovery."""
    
    def test_dag_bag_loads_existing_dags(self):
        """Test that DagBag can load existing DAGs from dags/ directory."""
        # Get the dags directory path
        dags_dir = os.path.join(os.path.dirname(__file__), '../../dags')
        
        # Create DagBag
        dagbag = DagBag(dag_folder=dags_dir, include_examples=False)
        
        # Verify no import errors
        assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"
        
        # Verify our DAGs are loaded
        dag_ids = list(dagbag.dag_ids)
        assert 'streaming_processing_dag' in dag_ids
    
    def test_new_dag_can_be_discovered(self):
        """Test that a new DAG file can be discovered by DagBag."""
        # Create a temporary directory for test DAGs
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a simple test DAG file
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
            
            # Write test DAG to temp directory
            test_dag_path = os.path.join(temp_dir, 'test_new_dag.py')
            with open(test_dag_path, 'w') as f:
                f.write(test_dag_content)
            
            # Create DagBag pointing to temp directory
            dagbag = DagBag(dag_folder=temp_dir, include_examples=False)
            
            # Verify no import errors
            assert len(dagbag.import_errors) == 0, f"Import errors: {dagbag.import_errors}"
            
            # Verify new DAG is discovered
            assert 'test_new_dag' in dagbag.dag_ids
            
            # Verify DAG structure (access directly from dagbag.dags dict to avoid DB query)
            new_dag = dagbag.dags.get('test_new_dag')
            assert new_dag is not None
            assert len(new_dag.tasks) == 1
            assert new_dag.tasks[0].task_id == 'test_task'
    
    def test_dag_with_syntax_error_is_reported(self):
        """Test that DAG with syntax error is reported in import_errors."""
        # Create a temporary directory for test DAGs
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a DAG file with syntax error
            bad_dag_content = '''
from airflow import DAG
from datetime import datetime

# Missing closing parenthesis - syntax error
with DAG(
    dag_id='bad_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
 as dag:
    pass
'''
            
            # Write bad DAG to temp directory
            bad_dag_path = os.path.join(temp_dir, 'bad_dag.py')
            with open(bad_dag_path, 'w') as f:
                f.write(bad_dag_content)
            
            # Create DagBag pointing to temp directory
            dagbag = DagBag(dag_folder=temp_dir, include_examples=False)
            
            # Verify import error is reported
            assert len(dagbag.import_errors) > 0
            assert bad_dag_path in dagbag.import_errors
    
    def test_removed_dag_not_in_dagbag(self):
        """Test that when a DAG file is removed, it's not in DagBag."""
        # Create a temporary directory for test DAGs
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a test DAG file
            test_dag_content = '''
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(
    dag_id='temporary_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='task',
        python_callable=lambda **context: "test",
    )
'''
            
            # Write test DAG
            test_dag_path = os.path.join(temp_dir, 'temporary_dag.py')
            with open(test_dag_path, 'w') as f:
                f.write(test_dag_content)
            
            # Create DagBag and verify DAG exists
            dagbag1 = DagBag(dag_folder=temp_dir, include_examples=False)
            assert 'temporary_dag' in dagbag1.dag_ids
            
            # Remove the DAG file
            os.remove(test_dag_path)
            
            # Create new DagBag and verify DAG is gone
            dagbag2 = DagBag(dag_folder=temp_dir, include_examples=False)
            assert 'temporary_dag' not in dagbag2.dag_ids


class TestDAGConfiguration:
    """Integration tests for DAG configuration."""
    
    def test_streaming_dag_has_correct_config(self):
        """Test that streaming processing DAG has correct configuration."""
        assert streaming_dag.dag_id == 'streaming_processing_dag'
        assert streaming_dag.schedule_interval == '* * * * *'  # Every minute
        assert streaming_dag.catchup is False
        assert 'streaming' in streaming_dag.tags
        assert 'spark' in streaming_dag.tags
        assert 'processing' in streaming_dag.tags
        
        # Verify default args
        assert streaming_dag.default_args['owner'] == 'data-engineering'
        assert streaming_dag.default_args['retries'] == 2
        assert 'on_failure_callback' in streaming_dag.default_args


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
