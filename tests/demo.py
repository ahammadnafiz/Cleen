import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import string
import json

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

# Initialize geocoding cache file
cache_file = "data/processed/geocoding_cache.pkl"
if not os.path.exists(cache_file):
    import pickle
    empty_cache = {}
    with open(cache_file, 'wb') as f:
        pickle.dump(empty_cache, f)


def main():
    print("🚀 Starting Enhanced Cleen demo...")
    
    # Initialize monitoring
    monitor = ResourceMonitor().start()
    
    try:
        # Configure and run pipeline
        print("\n2. Setting up enhanced data pipeline...")
        
        input_connector = CsvConnector(
            path="data/raw/complex_sample_data.csv",
            encoding="utf-8",
            delimiter=",",
            # Remove datetime_formats from here - we'll handle it in BulkTypeConverter
            null_values=["NA", "N/A", "", "NULL", "undefined", "None"],
            sampling={"strategy": "systematic", "sample_size": 1000}
        )
        
        output_connector = ParquetConnector(
            path="data/processed/cleaned_complex_data.parquet",
            partition_by=["currency", "payment_method"],
            compression="snappy"
        )
        
        # Modified pipeline setup
        pipeline = (
            PipelineBuilder()
            # First, sanitize the data
            .add_step(ColumnSanitizer(
                strip_whitespace=True,
                remove_special_chars=["$", "%", "!", "@", "#", "*", "&"],
                columns=None,
                to_lower=True,
                replace_patterns={
                    r'\s+': ' ',
                    r'[^\x00-\x7F]+': ''
                },
                remove_urls=True,
                remove_emails=False,
                custom_replacements={
                    "undefined": None,
                    "null": None
                }
            ))
            # Modified type converter with proper date handling
            .add_step(BulkTypeConverter(
                column_patterns={
                    "order_date": "date",  # Handle date first
                    "total_price": "float",
                    "quantity": "integer",
                    "currency": "category",
                    "payment_method": "category",
                    "shipping_method": "category"
                },
                date_formats=["%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"],  # Simplified date formats
                coerce_errors=True
            ))
            # Modified schema validator with more lenient settings
            .add_step(SchemaValidator(
                rules={
                "order_id": {
                    "type": "string", 
                    "regex": r"ORD-\d{8}", 
                    "required": True,
                    "unique": True
                },
                "order_date": {
                    "type": "date", 
                    "required": True,
                    "min_value": "2020-01-01",
                    "max_value": datetime.now().strftime("%Y-%m-%d")
                },
                "total_price": {
                    "type": "float", 
                    "min": 0, 
                    "required": True,
                    "max": 10000  # reasonable maximum price
                },
                "currency": {
                    "type": "category", 
                    "options": ["USD", "EUR", "GBP", "JPY", "CAD"],
                    "required": True
                },
                "customer_email": {
                    "type": "email", 
                    "required": True,
                    "unique": True
                },
                "customer_address": {
                    "type": "string",
                    "required": True,
                    "min_length": 10,
                    "max_length": 200
                },
                "product_category": {
                    "type": "string",
                    "required": True,
                    "regex": r"^[A-Za-z& ]+ > [A-Za-z& ]+ > [A-Za-z& ]+$"
                },
                "product_sku": {
                    "type": "string",
                    "regex": r"^SKU-[A-Z]\d{4}$",
                    "required": True
                },
                "quantity": {
                    "type": "integer", 
                    "min": 1,
                    "max": 100,
                    "required": True
                },
                "metadata": {
                    "type": "json",
                    "required": True,
                    "schema": {
                        "device_info": {
                            "type": "object",
                            "required": True
                        },
                        "user_preferences": {
                            "type": "object",
                            "required": True
                        },
                        "session_data": {
                            "type": "object",
                            "required": True
                        }
                    }
                },
                "payment_method": {
                    "type": "category", 
                    "options": ["CREDIT_CARD", "PAYPAL", "BANK_TRANSFER", "CRYPTO"],
                    "required": True
                },
                "shipping_method": {
                    "type": "category", 
                    "options": ["STANDARD", "EXPRESS", "OVERNIGHT", "PICKUP"],
                    "required": True
                }
            },
            strict_mode="flexible",
            error_handling={
                "missing_required": "reject",
                "invalid_type": "quarantine",
                "invalid_value": "quarantine",
                "invalid_format": "quarantine"
            }
            ))
            # Rest of the pipeline remains the same
            .add_step(PatternValidator(
                rules={
                    "customer_email": r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
                    "product_sku": r"^SKU-[A-Z]\d{4}$",
                    "order_id": r"^ORD-\d{8}$"
                },
                error_handling="quarantine"
            ))
            .add_step(GeospatialEnricher(
                address_columns=["customer_address"],
                add_columns=["coordinates", "timezone", "country", "state", "city", "postal_code"],
                cache_file="data/processed/geocoding_cache.pkl"
            ))
            # Use sequential processing initially until parallel issues are resolved
            .set_metrics(DataQualityReport(
                output_path="reports/enhanced_data_quality.html",
                column_stats=True,
                value_distributions=True,
                correlation_matrix=True,
            ))
            .build()
        )
                
        # Load and process data
        print("\n3. Loading and processing complex data...")
        raw_df = input_connector.load()
        print(f"Loaded {len(raw_df)} rows")
        
        processed_df = pipeline.run(
            raw_df,
            error_handling={
                "invalid_rows": "quarantine",
                "max_error_rate": 0.15,
                "error_storage": "data/processed/complex_errors.csv",
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
        processed_df.to_csv("data/processed/clean_complex_data.csv", index=False)
        
    finally:
        # Stop monitoring and check for anomalies
        monitor.stop().alert_on_anomalies()
        
        # Export metrics
        pipeline.metrics.export()
        print("\n📊 Enhanced data quality report generated at: reports/enhanced_data_quality.html")

if __name__ == "__main__":
    main()