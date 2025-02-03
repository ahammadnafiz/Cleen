# cleen/pipeline/executor.py
import sys
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
import psutil
import os
from ..core.base import PipelineStep

class ParallelExecutor:
    """Executor for running pipeline steps in parallel."""
    
    def __init__(self, partitions: int = 1, memory_limit: str = "4GB", use_disk: bool = False):
        """
        Initialize the parallel executor.
        
        Args:
            partitions (int): Number of parallel partitions to use
            memory_limit (str): Memory limit per partition (e.g., "4GB", "512MB")
            use_disk (bool): Whether to use disk for overflow data
        """
        self.partitions = partitions
        self.memory_limit = self._parse_memory_limit(memory_limit)
        self.use_disk = use_disk
        self._validate_config()

    def _parse_memory_limit(self, limit: str) -> int:
        """Convert memory limit string to bytes."""
        unit_map = {
            'GB': 1024**3,
            'MB': 1024**2,
            'KB': 1024
        }
        
        number = float(limit[:-2])
        unit = limit[-2:].upper()
        
        if unit not in unit_map:
            raise ValueError(f"Invalid memory unit: {unit}. Use GB, MB, or KB.")
            
        return int(number * unit_map[unit])

    def _validate_config(self) -> None:
        """Validate executor configuration."""
        total_memory = psutil.virtual_memory().total
        required_memory = self.memory_limit * self.partitions
        
        if required_memory > total_memory and not self.use_disk:
            raise ValueError(
                f"Required memory ({required_memory/1024**3:.1f}GB) exceeds system memory "
                f"({total_memory/1024**3:.1f}GB). Enable use_disk=True or reduce partitions."
            )

    def _process_partition(self, df_partition: pd.DataFrame, steps: List[PipelineStep]) -> pd.DataFrame:
        """Process a single partition through all pipeline steps."""
        result = df_partition.copy()
        
        for step in steps:
            try:
                result = step.execute(result)
            except Exception as e:
                print(f"Error in step {step.name}: {str(e)}")
                raise
                
        return result

    def execute(self, df: pd.DataFrame, steps: List[PipelineStep]) -> pd.DataFrame:
        """
        Execute pipeline steps in parallel across partitions.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            steps (List[PipelineStep]): Pipeline steps to execute
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        if self.partitions <= 1:
            return self._process_partition(df, steps)
            
        # Split DataFrame into partitions
        partition_indices = np.array_split(range(len(df)), self.partitions)
        df_partitions = [df.iloc[idx] for idx in partition_indices]
        
        # Process partitions in parallel
        try:
            with ProcessPoolExecutor(max_workers=self.partitions) as executor:
                futures = [
                    executor.submit(self._process_partition, partition, steps)
                    for partition in df_partitions
                ]
                
                # Collect results
                results = []
                for future in futures:
                    try:
                        results.append(future.result())
                    except Exception as e:
                        print(f"Error processing partition: {str(e)}")
                        raise
                
            # Combine results
            return pd.concat(results, ignore_index=True)
            
        except Exception as e:
            print(f"Parallel execution failed: {str(e)}")
            if self.use_disk:
                print("Falling back to sequential processing...")
                return self._process_partition(df, steps)
            raise

    def get_optimal_partitions(self, df: pd.DataFrame, df_size: int) -> int:
        """Calculate optimal number of partitions based on system resources."""
        cpu_count = os.cpu_count() or 1
        available_memory = psutil.virtual_memory().available
        
        # Estimate memory per row (rough approximation)
        sample_size = min(1000, df_size)
        memory_per_row = sys.getsizeof(df[:sample_size]) / sample_size
        
        max_partitions_by_memory = available_memory / (memory_per_row * df_size)
        max_partitions_by_cpu = cpu_count - 1  # Leave one CPU for system
        
        return max(1, min(
            int(max_partitions_by_memory),
            int(max_partitions_by_cpu),
            self.partitions
        ))