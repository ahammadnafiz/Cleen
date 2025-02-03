# cleen/pipeline/builder.py
from typing import List, Dict, Any, Optional
from cleen.core.base import PipelineStep, BaseProcessor, BaseValidator
from cleen.pipeline.executor import ParallelExecutor
import pandas as pd

class PipelineBuilder:
    def __init__(self):
        self.steps: List[PipelineStep] = []
        self.executor = None
        self.metrics = None
        self.incremental_config = None

    def add_step(self, processor: Any) -> 'PipelineBuilder':
        """Add a processing step to the pipeline."""
        self.steps.append(PipelineStep(processor))
        return self

    def set_executor(self, executor: 'ParallelExecutor') -> 'PipelineBuilder':
        """Set the executor for the pipeline."""
        self.executor = executor
        return self

    def enable_incremental(self, checkpoint_column: str, lookback_days: int) -> 'PipelineBuilder':
        """Enable incremental processing."""
        self.incremental_config = {
            "checkpoint_column": checkpoint_column,
            "lookback_days": lookback_days
        }
        return self

    def set_metrics(self, metrics: Any) -> 'PipelineBuilder':
        """Set metrics collection for the pipeline."""
        self.metrics = metrics
        return self

    def build(self) -> 'Pipeline':
        """Build and return the pipeline."""
        return Pipeline(
            steps=self.steps,
            executor=self.executor,
            metrics=self.metrics,
            incremental_config=self.incremental_config
        )
    
class Pipeline:
    def __init__(
        self,
        steps: List[PipelineStep],
        executor: Optional[ParallelExecutor] = None,
        metrics: Optional[Any] = None,
        incremental_config: Optional[Dict[str, Any]] = None
    ):
        self.steps = steps
        self.executor = executor or ParallelExecutor()
        self.metrics = metrics
        self.incremental_config = incremental_config

    def run(self, df: pd.DataFrame, error_handling: Dict[str, Any]) -> pd.DataFrame:
        """Run the pipeline on input DataFrame."""
        try:
            # Apply incremental processing if configured
            if self.incremental_config:
                checkpoint_col = self.incremental_config["checkpoint_column"]
                lookback_days = self.incremental_config["lookback_days"]
                cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=lookback_days)
                df = df[df[checkpoint_col] >= cutoff_date]

            # Execute pipeline
            result = self.executor.execute(df, self.steps)

            # Handle errors
            if error_handling.get("max_error_rate"):
                error_rate = (len(df) - len(result)) / len(df)
                if error_rate > error_handling["max_error_rate"]:
                    raise ValueError(f"Error rate {error_rate:.2%} exceeds maximum allowed {error_handling['max_error_rate']:.2%}")

            return result

        finally:
            if self.metrics:
                self.metrics.collect(df, result)
