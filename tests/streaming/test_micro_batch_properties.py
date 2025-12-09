"""
Property-based tests for Micro-Batch Processing.

Tests correctness properties using Hypothesis library:
- Property 1: Empty batch counting triggers stop
- Property 2: Non-empty batch resets counter

Each test runs minimum 100 iterations per design spec.
"""

from typing import List
import pytest
from hypothesis import given, settings, assume
from hypothesis import strategies as st


class MicroBatchController:
    """
    Simulates the micro-batch auto-stop logic from streaming jobs.
    
    This is a testable extraction of the should_stop logic from
    TradeAggregationJob, TechnicalIndicatorsJob, and AnomalyDetectionJob.
    """
    
    def __init__(self, empty_batch_threshold: int = 3, max_runtime_seconds: int = 300):
        """
        Initialize micro-batch controller.
        
        Args:
            empty_batch_threshold: Number of consecutive empty batches before stopping
            max_runtime_seconds: Maximum runtime in seconds before stopping
        """
        self.empty_batch_count: int = 0
        self.empty_batch_threshold: int = empty_batch_threshold
        self.max_runtime_seconds: int = max_runtime_seconds
        self.start_time: float = 0.0
        self.stop_reason: str = ""
    
    def should_stop(self, is_empty_batch: bool, current_time: float) -> bool:
        """
        Check if job should stop based on empty batch count or timeout.
        
        Args:
            is_empty_batch: Whether the current batch is empty
            current_time: Current timestamp in seconds
            
        Returns:
            True if job should stop, False otherwise
        """
        # Check timeout
        if self.start_time > 0 and (current_time - self.start_time) > self.max_runtime_seconds:
            self.stop_reason = f"Max runtime {self.max_runtime_seconds}s exceeded"
            return True
        
        # Check empty batches
        if is_empty_batch:
            self.empty_batch_count += 1
            if self.empty_batch_count >= self.empty_batch_threshold:
                self.stop_reason = f"{self.empty_batch_threshold} consecutive empty batches"
                return True
        else:
            # Reset counter when data is received
            self.empty_batch_count = 0
        
        return False
    
    def process_batch_sequence(self, batch_sequence: List[bool], start_time: float = 0.0) -> int:
        """
        Process a sequence of batches and return the index where stop was triggered.
        
        Args:
            batch_sequence: List of booleans where True = empty batch, False = non-empty batch
            start_time: Start time for the job
            
        Returns:
            Index of batch that triggered stop, or -1 if no stop was triggered
        """
        self.start_time = start_time
        self.empty_batch_count = 0
        self.stop_reason = ""
        
        for i, is_empty in enumerate(batch_sequence):
            # Simulate time passing (1 second per batch, no timeout in this test)
            current_time = start_time + i
            if self.should_stop(is_empty, current_time):
                return i
        
        return -1


class TestEmptyBatchCountingTriggersStop:
    """
    **Feature: micro-batch-processing, Property 1: Empty batch counting triggers stop**
    
    Property 1: Empty batch counting triggers stop
    *For any* sequence of batches where the last N batches are empty (N = empty_batch_threshold),
    the streaming job SHALL stop after processing the Nth empty batch.
    **Validates: Requirements 1.1**
    """
    
    @settings(max_examples=100)
    @given(
        empty_batch_threshold=st.integers(min_value=1, max_value=10),
        prefix_length=st.integers(min_value=0, max_value=20),
    )
    def test_stops_after_threshold_consecutive_empty_batches(
        self,
        empty_batch_threshold: int,
        prefix_length: int,
    ):
        """
        **Feature: micro-batch-processing, Property 1: Empty batch counting triggers stop**
        
        When N consecutive empty batches are received, the job should stop.
        """
        controller = MicroBatchController(empty_batch_threshold=empty_batch_threshold)
        
        # Create a sequence: some non-empty batches followed by exactly threshold empty batches
        batch_sequence = [False] * prefix_length + [True] * empty_batch_threshold
        
        stop_index = controller.process_batch_sequence(batch_sequence)
        
        # Should stop at the last empty batch (threshold - 1 index within the empty section)
        expected_stop_index = prefix_length + empty_batch_threshold - 1
        
        assert stop_index == expected_stop_index, \
            f"Expected stop at index {expected_stop_index}, got {stop_index}"
        assert "consecutive empty batches" in controller.stop_reason, \
            f"Expected stop reason to mention empty batches, got: {controller.stop_reason}"

    @settings(max_examples=100)
    @given(
        empty_batch_threshold=st.integers(min_value=2, max_value=10),
        num_empty_batches=st.integers(min_value=1, max_value=20),
    )
    def test_does_not_stop_before_threshold(
        self,
        empty_batch_threshold: int,
        num_empty_batches: int,
    ):
        """
        **Feature: micro-batch-processing, Property 1: Empty batch counting triggers stop**
        
        When fewer than N consecutive empty batches are received, the job should not stop.
        """
        # Only test when num_empty_batches < threshold
        assume(num_empty_batches < empty_batch_threshold)
        
        controller = MicroBatchController(empty_batch_threshold=empty_batch_threshold)
        
        # Create a sequence with fewer empty batches than threshold
        batch_sequence = [True] * num_empty_batches
        
        stop_index = controller.process_batch_sequence(batch_sequence)
        
        assert stop_index == -1, \
            f"Should not stop with {num_empty_batches} empty batches (threshold={empty_batch_threshold})"

    @settings(max_examples=100)
    @given(
        empty_batch_threshold=st.integers(min_value=1, max_value=5),
        extra_empty_batches=st.integers(min_value=1, max_value=10),
    )
    def test_stops_exactly_at_threshold(
        self,
        empty_batch_threshold: int,
        extra_empty_batches: int,
    ):
        """
        **Feature: micro-batch-processing, Property 1: Empty batch counting triggers stop**
        
        The job should stop exactly when the threshold is reached, not before or after.
        """
        controller = MicroBatchController(empty_batch_threshold=empty_batch_threshold)
        
        # Create a sequence with more empty batches than threshold
        total_empty = empty_batch_threshold + extra_empty_batches
        batch_sequence = [True] * total_empty
        
        stop_index = controller.process_batch_sequence(batch_sequence)
        
        # Should stop exactly at threshold - 1 (0-indexed)
        expected_stop_index = empty_batch_threshold - 1
        
        assert stop_index == expected_stop_index, \
            f"Expected stop at index {expected_stop_index}, got {stop_index}"


class TestNonEmptyBatchResetsCounter:
    """
    **Feature: micro-batch-processing, Property 2: Non-empty batch resets counter**
    
    Property 2: Non-empty batch resets counter
    *For any* sequence of batches, if a non-empty batch is processed after empty batches,
    the empty batch counter SHALL be reset to zero.
    **Validates: Requirements 1.2**
    """
    
    @settings(max_examples=100)
    @given(
        empty_batch_threshold=st.integers(min_value=2, max_value=10),
        num_empty_before_reset=st.integers(min_value=1, max_value=20),
    )
    def test_non_empty_batch_resets_counter(
        self,
        empty_batch_threshold: int,
        num_empty_before_reset: int,
    ):
        """
        **Feature: micro-batch-processing, Property 2: Non-empty batch resets counter**
        
        A non-empty batch should reset the empty batch counter to zero.
        """
        # Only test when num_empty_before_reset < threshold (so we don't stop early)
        assume(num_empty_before_reset < empty_batch_threshold)
        
        controller = MicroBatchController(empty_batch_threshold=empty_batch_threshold)
        
        # Process some empty batches (less than threshold)
        for _ in range(num_empty_before_reset):
            controller.should_stop(is_empty_batch=True, current_time=0)
        
        assert controller.empty_batch_count == num_empty_before_reset, \
            f"Expected count {num_empty_before_reset}, got {controller.empty_batch_count}"
        
        # Process a non-empty batch
        controller.should_stop(is_empty_batch=False, current_time=0)
        
        # Counter should be reset to 0
        assert controller.empty_batch_count == 0, \
            f"Expected count 0 after non-empty batch, got {controller.empty_batch_count}"

    @settings(max_examples=100)
    @given(
        empty_batch_threshold=st.integers(min_value=3, max_value=10),
        num_cycles=st.integers(min_value=1, max_value=5),
    )
    def test_reset_allows_more_empty_batches(
        self,
        empty_batch_threshold: int,
        num_cycles: int,
    ):
        """
        **Feature: micro-batch-processing, Property 2: Non-empty batch resets counter**
        
        After reset, the job should be able to process more empty batches before stopping.
        """
        controller = MicroBatchController(empty_batch_threshold=empty_batch_threshold)
        
        # Create a sequence that alternates: (threshold-1) empty, 1 non-empty, repeated
        batch_sequence = []
        for _ in range(num_cycles):
            batch_sequence.extend([True] * (empty_batch_threshold - 1))  # Almost threshold empty
            batch_sequence.append(False)  # Reset with non-empty
        
        stop_index = controller.process_batch_sequence(batch_sequence)
        
        # Should not stop because we always reset before reaching threshold
        assert stop_index == -1, \
            f"Should not stop when resetting before threshold, but stopped at index {stop_index}"

    @settings(max_examples=100)
    @given(
        empty_batch_threshold=st.integers(min_value=2, max_value=5),
        num_resets=st.integers(min_value=1, max_value=5),
    )
    def test_multiple_resets_then_stop(
        self,
        empty_batch_threshold: int,
        num_resets: int,
    ):
        """
        **Feature: micro-batch-processing, Property 2: Non-empty batch resets counter**
        
        After multiple resets, the job should still stop when threshold is finally reached.
        """
        controller = MicroBatchController(empty_batch_threshold=empty_batch_threshold)
        
        # Create sequence: multiple reset cycles, then finally reach threshold
        batch_sequence = []
        for _ in range(num_resets):
            batch_sequence.extend([True] * (empty_batch_threshold - 1))  # Almost threshold
            batch_sequence.append(False)  # Reset
        
        # Finally, add enough empty batches to trigger stop
        batch_sequence.extend([True] * empty_batch_threshold)
        
        stop_index = controller.process_batch_sequence(batch_sequence)
        
        # Should stop at the last batch
        expected_stop_index = len(batch_sequence) - 1
        
        assert stop_index == expected_stop_index, \
            f"Expected stop at index {expected_stop_index}, got {stop_index}"

    @settings(max_examples=100)
    @given(
        empty_batch_threshold=st.integers(min_value=2, max_value=10),
        pattern=st.lists(
            st.booleans(),
            min_size=1,
            max_size=50,
        ),
    )
    def test_counter_state_consistency(
        self,
        empty_batch_threshold: int,
        pattern: List[bool],
    ):
        """
        **Feature: micro-batch-processing, Property 2: Non-empty batch resets counter**
        
        The counter should always reflect the number of consecutive empty batches.
        """
        controller = MicroBatchController(empty_batch_threshold=empty_batch_threshold)
        
        consecutive_empty = 0
        
        for is_empty in pattern:
            stopped = controller.should_stop(is_empty_batch=is_empty, current_time=0)
            
            if stopped:
                # If stopped, counter should be at threshold
                assert controller.empty_batch_count >= empty_batch_threshold, \
                    f"Stopped but counter {controller.empty_batch_count} < threshold {empty_batch_threshold}"
                break
            
            if is_empty:
                consecutive_empty += 1
            else:
                consecutive_empty = 0
            
            # Counter should match our tracking (unless we stopped)
            assert controller.empty_batch_count == consecutive_empty, \
                f"Counter mismatch: expected {consecutive_empty}, got {controller.empty_batch_count}"


# ============================================================================
# DAG Structure Property Tests
# ============================================================================

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
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False


def get_streaming_processing_dag():
    """Load and return the streaming_processing_dag."""
    if not AIRFLOW_AVAILABLE:
        return None
    dagbag = DagBag(dag_folder=str(dags_path), include_examples=False)
    return dagbag.dags.get('streaming_processing_dag')


def get_all_upstream_task_ids(task) -> set:
    """Get all upstream task IDs recursively."""
    upstream_ids = set()
    for upstream_task in task.upstream_list:
        upstream_ids.add(upstream_task.task_id)
        upstream_ids.update(get_all_upstream_task_ids(upstream_task))
    return upstream_ids


def get_direct_upstream_task_ids(task) -> set:
    """Get direct upstream task IDs."""
    return {t.task_id for t in task.upstream_list}


def get_direct_downstream_task_ids(task) -> set:
    """Get direct downstream task IDs."""
    return {t.task_id for t in task.downstream_list}


@pytest.mark.skipif(not AIRFLOW_AVAILABLE, reason="Airflow not installed")
class TestHealthChecksPrecedeStreamingJobs:
    """
    **Feature: micro-batch-processing, Property 3: Health checks precede streaming jobs**
    
    Property 3: Health checks precede streaming jobs
    *For any* streaming job task in the DAG, all health check tasks 
    (test_redis_health, test_duckdb_health, test_parquet_path) SHALL be 
    in its upstream dependency chain.
    **Validates: Requirements 3.1**
    """
    
    HEALTH_CHECK_TASKS = {'test_redis_health', 'test_duckdb_health', 'test_parquet_path'}
    STREAMING_JOB_TASKS = {
        'run_trade_aggregation_job',
        'run_technical_indicators_job', 
        'run_anomaly_detection_job'
    }
    
    @settings(max_examples=100, deadline=None)
    @given(
        streaming_job=st.sampled_from(list(STREAMING_JOB_TASKS)),
        health_check=st.sampled_from(list(HEALTH_CHECK_TASKS)),
    )
    def test_health_check_is_upstream_of_streaming_job(
        self,
        streaming_job: str,
        health_check: str,
    ):
        """
        **Feature: micro-batch-processing, Property 3: Health checks precede streaming jobs**
        
        For any streaming job and any health check, the health check should be
        in the upstream dependency chain of the streaming job.
        """
        dag = get_streaming_processing_dag()
        assume(dag is not None)
        
        streaming_task = dag.get_task(streaming_job)
        assume(streaming_task is not None)
        
        upstream_task_ids = get_all_upstream_task_ids(streaming_task)
        
        assert health_check in upstream_task_ids, \
            f"Health check '{health_check}' should be upstream of '{streaming_job}', " \
            f"but upstream tasks are: {upstream_task_ids}"

    def test_all_health_checks_precede_all_streaming_jobs(self):
        """
        **Feature: micro-batch-processing, Property 3: Health checks precede streaming jobs**
        
        Verify all health checks are upstream of all streaming jobs.
        """
        dag = get_streaming_processing_dag()
        if dag is None:
            pytest.skip("DAG not available")
        
        for streaming_job in self.STREAMING_JOB_TASKS:
            streaming_task = dag.get_task(streaming_job)
            if streaming_task is None:
                pytest.fail(f"Streaming job task '{streaming_job}' not found in DAG")
            
            upstream_task_ids = get_all_upstream_task_ids(streaming_task)
            
            for health_check in self.HEALTH_CHECK_TASKS:
                assert health_check in upstream_task_ids, \
                    f"Health check '{health_check}' should be upstream of '{streaming_job}'"


@pytest.mark.skipif(not AIRFLOW_AVAILABLE, reason="Airflow not installed")
class TestSequentialExecutionWithCorrectDependencies:
    """
    **Feature: micro-batch-processing, Property 4: Sequential execution with correct dependencies**
    
    Property 4: Sequential execution with correct dependencies
    *For any* DAG configuration:
    - run_technical_indicators_job SHALL have run_trade_aggregation_job as its streaming upstream
    - run_anomaly_detection_job SHALL have run_technical_indicators_job as its streaming upstream
      (because it reads from processed_indicators topic created by technical_indicators_job)
    **Validates: Requirements 3.2**
    """
    
    STREAMING_JOB_TASKS = {
        'run_trade_aggregation_job',
        'run_technical_indicators_job', 
        'run_anomaly_detection_job'
    }
    
    def test_technical_indicators_has_aggregation_as_upstream(self):
        """
        **Feature: micro-batch-processing, Property 4: Sequential execution with correct dependencies**
        
        run_technical_indicators_job should have run_trade_aggregation_job as its streaming upstream.
        """
        dag = get_streaming_processing_dag()
        if dag is None:
            pytest.skip("DAG not available")
        
        indicators_task = dag.get_task('run_technical_indicators_job')
        if indicators_task is None:
            pytest.fail("run_technical_indicators_job task not found in DAG")
        
        # Get direct upstream tasks
        direct_upstream_ids = get_direct_upstream_task_ids(indicators_task)
        
        # Filter to only streaming job tasks
        streaming_upstream = direct_upstream_ids.intersection(self.STREAMING_JOB_TASKS)
        
        # Should have run_trade_aggregation_job as streaming upstream
        assert streaming_upstream == {'run_trade_aggregation_job'}, \
            f"'run_technical_indicators_job' should have 'run_trade_aggregation_job' as streaming upstream, " \
            f"but has: {streaming_upstream}"

    def test_anomaly_detection_has_indicators_as_upstream(self):
        """
        **Feature: micro-batch-processing, Property 4: Sequential execution with correct dependencies**
        
        run_anomaly_detection_job should have run_technical_indicators_job as its streaming upstream
        because it reads from processed_indicators topic created by technical_indicators_job.
        """
        dag = get_streaming_processing_dag()
        if dag is None:
            pytest.skip("DAG not available")
        
        anomaly_task = dag.get_task('run_anomaly_detection_job')
        if anomaly_task is None:
            pytest.fail("run_anomaly_detection_job task not found in DAG")
        
        # Get direct upstream tasks
        direct_upstream_ids = get_direct_upstream_task_ids(anomaly_task)
        
        # Filter to only streaming job tasks
        streaming_upstream = direct_upstream_ids.intersection(self.STREAMING_JOB_TASKS)
        
        # Should have run_technical_indicators_job as streaming upstream
        assert streaming_upstream == {'run_technical_indicators_job'}, \
            f"'run_anomaly_detection_job' should have 'run_technical_indicators_job' as streaming upstream, " \
            f"but has: {streaming_upstream}"

    def test_anomaly_detection_depends_on_indicators(self):
        """
        **Feature: micro-batch-processing, Property 4: Sequential execution with correct dependencies**
        
        Anomaly detection should depend on technical indicators (not the other way around).
        """
        dag = get_streaming_processing_dag()
        if dag is None:
            pytest.skip("DAG not available")
        
        indicators_task = dag.get_task('run_technical_indicators_job')
        anomaly_task = dag.get_task('run_anomaly_detection_job')
        
        if indicators_task is None or anomaly_task is None:
            pytest.fail("Streaming job tasks not found in DAG")
        
        indicators_upstream = get_all_upstream_task_ids(indicators_task)
        anomaly_upstream = get_all_upstream_task_ids(anomaly_task)
        
        # Anomaly detection should have indicators as upstream
        assert 'run_technical_indicators_job' in anomaly_upstream, \
            "run_technical_indicators_job should be upstream of run_anomaly_detection_job"
        # Indicators should NOT have anomaly detection as upstream
        assert 'run_anomaly_detection_job' not in indicators_upstream, \
            "run_anomaly_detection_job should not be upstream of run_technical_indicators_job"


@pytest.mark.skipif(not AIRFLOW_AVAILABLE, reason="Airflow not installed")
class TestCleanupIsFinalTask:
    """
    **Feature: micro-batch-processing, Property 5: Cleanup is final task**
    
    Property 5: Cleanup is final task
    *For any* DAG configuration, the cleanup_streaming task SHALL have no downstream 
    dependencies and SHALL have run_anomaly_detection_job as its upstream dependency.
    **Validates: Requirements 3.3, 3.4**
    
    Note: The DAG uses schedule_interval for continuous processing instead of 
    manual retriggering, so cleanup_streaming is the final task.
    """
    
    def test_cleanup_has_no_downstream_dependencies(self):
        """
        **Feature: micro-batch-processing, Property 5: Cleanup is final task**
        
        The cleanup_streaming task should have no downstream dependencies.
        """
        dag = get_streaming_processing_dag()
        if dag is None:
            pytest.skip("DAG not available")
        
        cleanup_task = dag.get_task('cleanup_streaming')
        if cleanup_task is None:
            pytest.fail("cleanup_streaming task not found in DAG")
        
        downstream_ids = get_direct_downstream_task_ids(cleanup_task)
        
        assert len(downstream_ids) == 0, \
            f"cleanup_streaming should have no downstream dependencies, but has: {downstream_ids}"

    def test_cleanup_has_anomaly_detection_as_upstream(self):
        """
        **Feature: micro-batch-processing, Property 5: Cleanup is final task**
        
        The cleanup_streaming task should have run_anomaly_detection_job as its upstream dependency.
        """
        dag = get_streaming_processing_dag()
        if dag is None:
            pytest.skip("DAG not available")
        
        cleanup_task = dag.get_task('cleanup_streaming')
        if cleanup_task is None:
            pytest.fail("cleanup_streaming task not found in DAG")
        
        upstream_ids = get_direct_upstream_task_ids(cleanup_task)
        
        assert 'run_anomaly_detection_job' in upstream_ids, \
            f"cleanup_streaming should have 'run_anomaly_detection_job' as upstream, but has: {upstream_ids}"

    @settings(max_examples=100, deadline=None)
    @given(
        task_id=st.sampled_from([
            'test_redis_health', 'test_duckdb_health', 'test_parquet_path',
            'run_trade_aggregation_job', 'run_technical_indicators_job',
            'run_anomaly_detection_job'
        ]),
    )
    def test_cleanup_is_downstream_of_all_other_tasks(
        self,
        task_id: str,
    ):
        """
        **Feature: micro-batch-processing, Property 5: Cleanup is final task**
        
        For any task in the DAG (except cleanup_streaming itself), cleanup_streaming 
        should be in its downstream dependency chain.
        """
        dag = get_streaming_processing_dag()
        assume(dag is not None)
        
        task = dag.get_task(task_id)
        assume(task is not None)
        
        # Get all downstream tasks recursively
        def get_all_downstream_task_ids(t) -> set:
            downstream_ids = set()
            for downstream_task in t.downstream_list:
                downstream_ids.add(downstream_task.task_id)
                downstream_ids.update(get_all_downstream_task_ids(downstream_task))
            return downstream_ids
        
        downstream_ids = get_all_downstream_task_ids(task)
        
        assert 'cleanup_streaming' in downstream_ids, \
            f"cleanup_streaming should be downstream of '{task_id}', " \
            f"but downstream tasks are: {downstream_ids}"


@pytest.mark.skipif(not AIRFLOW_AVAILABLE, reason="Airflow not installed")
class TestDAGConfiguration:
    """
    Additional tests for DAG configuration related to continuous processing.
    """
    
    def test_dag_has_max_active_runs_set_to_one(self):
        """
        **Feature: micro-batch-processing, Property 5: Cleanup is final task**
        
        The DAG should have max_active_runs=1 to prevent overlapping runs.
        **Validates: Requirements 2.3**
        """
        dag = get_streaming_processing_dag()
        if dag is None:
            pytest.skip("DAG not available")
        
        assert dag.max_active_runs == 1, \
            f"DAG should have max_active_runs=1, but has: {dag.max_active_runs}"

    def test_dag_has_schedule_interval(self):
        """
        **Feature: micro-batch-processing, Property 5: Cleanup is final task**
        
        The DAG should have a schedule_interval for continuous processing.
        **Validates: Requirements 2.4**
        """
        dag = get_streaming_processing_dag()
        if dag is None:
            pytest.skip("DAG not available")
        
        assert dag.schedule_interval is not None, \
            "DAG should have a schedule_interval for continuous processing"
        assert dag.schedule_interval == '* * * * *', \
            f"DAG should run every minute, but has schedule_interval: {dag.schedule_interval}"
