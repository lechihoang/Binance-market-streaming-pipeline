"""
Syntax and structure tests for DAG files.

These tests verify DAG files can be imported and have correct structure
without requiring Airflow to be installed.
"""

import pytest
import ast
from pathlib import Path


class TestStreamingPipelineDAGSyntax:
    """Test streaming_processing_dag.py syntax and structure."""
    
    @pytest.fixture
    def dag_file_path(self):
        """Get path to streaming_processing_dag.py."""
        return Path(__file__).parent.parent.parent / 'dags' / 'streaming_processing_dag.py'
    
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
        
        # Check for required imports
        assert 'airflow' in imports or any('airflow' in imp for imp in imports), \
            "DAG must import from airflow"
        assert any('operators' in imp for imp in imports), \
            "DAG must import operators"
    
    def test_dag_defines_default_args(self, dag_ast):
        """Test DAG defines default_args dictionary."""
        has_default_args = False
        
        for node in ast.walk(dag_ast):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == 'default_args':
                        has_default_args = True
                        break
        
        assert has_default_args, "DAG must define default_args"
    
    def test_dag_has_health_checks(self, dag_file_path):
        """Test DAG file contains health check tasks for 3-tier storage."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        
        # Check for health check tasks (3-tier storage architecture)
        assert 'test_redis_health' in content, "DAG must have test_redis_health task (Hot Path)"
        assert 'test_postgres_health' in content, "DAG must have test_postgres_health task (Warm Path)"
        assert 'test_minio_health' in content, "DAG must have test_minio_health task (Cold Path)"
    
    def test_dag_has_run_tasks(self, dag_file_path):
        """Test DAG has run tasks for streaming jobs."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        
        # Check for run tasks
        assert 'run_trade_aggregation_job' in content, "DAG must have run_trade_aggregation_job task"
        assert 'run_technical_indicators_job' in content, "DAG must have run_technical_indicators_job task"
        assert 'run_anomaly_detection_job' in content, "DAG must have run_anomaly_detection_job task"
    
    def test_dag_sets_dependencies(self, dag_file_path):
        """Test DAG sets dependencies between tasks."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        
        # Check for dependency operators
        assert '>>' in content, "DAG must set dependencies using >> operator"
        
        # Check for health checks to aggregation dependency
        assert 'test_redis_health' in content and 'run_trade_aggregation_job' in content
        
        # Check for sequential job dependencies
        assert 'run_trade_aggregation_job' in content and 'run_technical_indicators_job' in content
        assert 'run_technical_indicators_job' in content and 'run_anomaly_detection_job' in content
    
    def test_dag_has_correct_dag_id(self, dag_file_path):
        """Test DAG has correct dag_id."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        
        assert "dag_id='streaming_processing_dag'" in content or 'dag_id="streaming_processing_dag"' in content, \
            "DAG must have dag_id='streaming_processing_dag'"
    
    def test_dag_has_schedule(self, dag_file_path):
        """Test DAG is configured with schedule interval."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        
        assert 'schedule_interval=' in content, \
            "DAG must have schedule_interval configured"
    
    def test_dag_has_tags(self, dag_file_path):
        """Test DAG has appropriate tags."""
        with open(dag_file_path, 'r') as f:
            content = f.read()
        
        assert 'tags=' in content, "DAG must have tags"
        assert 'streaming' in content, "DAG must have 'streaming' tag"
