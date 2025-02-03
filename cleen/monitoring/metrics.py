# cleen/monitoring/metrics.py
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np
from datetime import datetime
import plotly.graph_objects as go
import plotly.express as px

class DataQualityReport:
    def __init__(
        self,
        output_path: str,
        column_stats: bool = True,
        value_distributions: bool = True,
        correlation_matrix: bool = True,  # Add new parameter
        timestamp: Optional[datetime] = None  # Add timestamp parameter
    ):
        self.output_path = output_path
        self.column_stats = column_stats
        self.value_distributions = value_distributions
        self.correlation_matrix = correlation_matrix  # Store parameter
        self.metrics = {
            "timestamp": timestamp or datetime.now()  # Use provided timestamp or current time
        }

    def collect(self, input_df: pd.DataFrame, output_df: pd.DataFrame) -> None:
        """Collect metrics from input and output DataFrames."""
        self.metrics.update({
            "input_rows": len(input_df),
            "output_rows": len(output_df),
            "success_rate": len(output_df) / len(input_df),
            "column_metrics": self._collect_column_metrics(input_df, output_df),
            "processing_time": None  # Set by pipeline
        })
        
        # Add correlation matrix if enabled
        if self.correlation_matrix:
            numeric_cols = output_df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                self.metrics["correlation_matrix"] = output_df[numeric_cols].corr().to_dict()

    def _collect_column_metrics(self, input_df: pd.DataFrame, output_df: pd.DataFrame) -> Dict[str, Any]:
        """Collect detailed column-level metrics."""
        metrics = {}
        
        for col in output_df.columns:
            col_metrics = {
                "null_rate": output_df[col].isnull().mean(),
                "unique_values": output_df[col].nunique(),
                "data_type": str(output_df[col].dtype)
            }
            
            if pd.api.types.is_numeric_dtype(output_df[col]):
                col_metrics.update({
                    "mean": output_df[col].mean(),
                    "std": output_df[col].std(),
                    "min": output_df[col].min(),
                    "max": output_df[col].max()
                })
            
            metrics[col] = col_metrics
        
        return metrics

    def export(self) -> None:
        """Export metrics to HTML report."""
        html_content = []
        
        # Overview section
        html_content.append("""
            <h1>Data Quality Report</h1>
            <h2>Overview</h2>
            <ul>
                <li>Processing Time: {timestamp}</li>
                <li>Success Rate: {success_rate:.2%}</li>
                <li>Input Rows: {input_rows}</li>
                <li>Output Rows: {output_rows}</li>
            </ul>
        """.format(**self.metrics))
        
        # Column metrics section
        if self.column_stats:
            html_content.append("<h2>Column Metrics</h2>")
            for col, metrics in self.metrics["column_metrics"].items():
                html_content.append(f"""
                    <h3>{col}</h3>
                    <ul>
                        <li>Data Type: {metrics['data_type']}</li>
                        <li>Null Rate: {metrics['null_rate']:.2%}</li>
                        <li>Unique Values: {metrics['unique_values']}</li>
                    </ul>
                """)
        
        # Save report
        with open(self.output_path, 'w') as f:
            f.write("\n".join(html_content))

class ResourceMonitor:
    def __init__(self):
        self.start_time = None
        self.metrics = {}

    def start(self) -> 'ResourceMonitor':
        """Start monitoring resources."""
        self.start_time = datetime.now()
        return self

    def stop(self) -> 'ResourceMonitor':
        """Stop monitoring and collect final metrics."""
        self.metrics["duration"] = (datetime.now() - self.start_time).total_seconds()
        return self

    def alert_on_anomalies(self) -> None:
        """Check metrics for anomalies and alert if necessary."""
        if self.metrics.get("duration", 0) > 3600:  # 1 hour
            self._send_alert("Long running process detected")

    def _send_alert(self, message: str) -> None:
        """Send alert through configured channels."""
        print(f"ALERT: {message}")  # Replace with actual alert mechanism