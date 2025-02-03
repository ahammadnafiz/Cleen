import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from cleen.connectors.file.csv import CsvConnector
from cleen.connectors.file.parquet import ParquetConnector
from cleen.processors.structured.cleaners import (
    ColumnSanitizer,
    BulkTypeConverter,
    GeospatialEnricher
)
from cleen.validators.schema import SchemaValidator
from cleen.validators.rules import PatternValidator
from cleen.pipeline.executor import ParallelExecutor
from cleen.pipeline.builder import PipelineBuilder
from cleen.monitoring.metrics import DataQualityReport, ResourceMonitor

# Create sample data directory
os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)
os.makedirs("reports", exist_ok=True)

# Generate sample data
def generate_sample_data(num_rows=1000):
    np.random.seed(42)
    
    dates = [
        (datetime.now() - timedelta(days=x)).strftime("%Y-%m-%d")
        for x in range(num_rows)
    ]
    
    data = {
        "order_id": [f"ORD-{str(x).zfill(8)}" for x in range(num_rows)],
        "order_date": dates,
        "total_price": np.random.uniform(10, 1000, num_rows).round(2),
        "currency": np.random.choice(["USD", "EUR", "GBP"], num_rows),
        "customer_email": [f"customer{x}@example.com" for x in range(num_rows)],
        "customer_address": [f"{x} Main St, City {x}, Country" for x in range(num_rows)],
        "product_name": [f"Product {x}" for x in range(num_rows)],
        "product_description": [
            f"This is a sample product {x} with some details that need cleaning  !"
            for x in range(num_rows)
        ],
        "customer_comments": [
            f"Sample comment {x} that needs processing   ..."
            for x in range(num_rows)
        ]
    }
    
    # Add some noise to data
    data["total_price"] = [f"${x}" if i % 10 == 0 else x 
                           for i, x in enumerate(data["total_price"])]
    data["customer_email"] = [f"invalid_email" if i % 20 == 0 else x 
                              for i, x in enumerate(data["customer_email"])]
    
    return pd.DataFrame(data)

def main():
    print("🚀 Starting Cleen demo...")
    
    # Generate and save sample data
    print("\n1. Generating sample data...")
    df = generate_sample_data()
    df.to_csv("data/raw/sample_data.csv", index=False)
    
    # Initialize monitoring
    monitor = ResourceMonitor().start()
    
    try:
        # Configure and run pipeline
        print("\n2. Setting up data pipeline...")
        
        # Initialize connectors
        input_connector = CsvConnector(
            path="data/raw/sample_data.csv",
            encoding="utf-8",
            delimiter=",",
            datetime_formats={
                "order_date": "%Y-%m-%d"
            },
            null_values=["NA", "N/A", ""]
        )
        
        output_connector = ParquetConnector(
            path="data/processed/cleaned_data.parquet",
            partition_by=["currency"],
            compression="snappy"
        )
        
        # Define schema validation rules
        schema = SchemaValidator(
            rules={
                "order_id": {"type": "string", "regex": r"ORD-\d{8}"},
                "order_date": {"type": "date"},
                "total_price": {"type": "float", "min": 0},
                "currency": {"type": "category", "options": ["USD", "EUR", "GBP"]},
                "customer_email": {"type": "email"},
                "product_.*": {"type": "string"}
            },
            strict_mode="flexible"
        )
        
        # Create pipeline
        pipeline = (
            PipelineBuilder()
            .add_step(ColumnSanitizer(
                strip_whitespace=True,
                remove_special_chars=["$", "%", "!"],
                columns=["product_name", "product_description", "customer_comments", "total_price"]
            ))
            .add_step(BulkTypeConverter(
                column_patterns={
                    "total_price": "float",
                    "order_date": "date"
                }
            ))
            .add_step(PatternValidator(
                rules={
                    "customer_email": r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
                },
                error_handling="quarantine"
            ))
            .add_step(GeospatialEnricher(
                address_columns=["customer_address"],
                add_columns=["coordinates", "timezone"]
            ))
            .set_executor(ParallelExecutor(
                partitions=4,
                memory_limit="1GB",
                use_disk=True
            ))
            .set_metrics(DataQualityReport(
                output_path="reports/data_quality.html",
                column_stats=True,
                value_distributions=True
            ))
            .build()
        )
        
        # Load and process data
        print("\n3. Loading and processing data...")
        raw_df = input_connector.load()
        print(f"Loaded {len(raw_df)} rows")
        
        processed_df = pipeline.run(
            raw_df,
            error_handling={
                "invalid_rows": "reject",
                "max_error_rate": 0.1,
                "error_storage": "data/processed/errors.csv"
            }
        )
        
        # Save processed data
        print("\n4. Saving processed data...")
        output_connector.save(processed_df)
        
        # Print summary
        print("\n✅ Processing complete!")
        print(f"Input rows: {len(raw_df)}")
        print(f"Output rows: {len(processed_df)}")
        print(f"Success rate: {(len(processed_df) / len(raw_df) * 100):.1f}%")
        
        # Show sample of processed data
        print("\nSample of processed data:")
        print(processed_df.head().to_string())
        processed_df.to_csv("data/processed/clean_data.csv", index=False)
        
    finally:
        # Stop monitoring and check for anomalies
        monitor.stop().alert_on_anomalies()
        
        # Export metrics
        pipeline.metrics.export()
        print("\n📊 Data quality report generated at: reports/data_quality.html")

if __name__ == "__main__":
    main()