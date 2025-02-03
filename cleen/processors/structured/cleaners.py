# cleen/processors/structured/cleaners.py
from typing import List, Optional, Dict, Any
import pandas as pd
from ...core.base import BaseProcessor

class ColumnSanitizer(BaseProcessor):
    def __init__(
        self,
        strip_whitespace: bool = True,
        remove_special_chars: Optional[List[str]] = None,
        columns: Optional[List[str]] = None,
        to_lower: bool = False,
        to_upper: bool = False
    ):
        self.strip_whitespace = strip_whitespace
        self.remove_special_chars = remove_special_chars or []
        self.columns = columns
        self.to_lower = to_lower
        self.to_upper = to_upper

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sanitize specified columns in the DataFrame."""
        result = df.copy()
        target_columns = self.columns or df.select_dtypes(include=['object']).columns
        
        for col in target_columns:
            if col in result.columns:
                # Strip whitespace
                if self.strip_whitespace:
                    result[col] = result[col].str.strip()

                # Convert case
                if self.to_lower:
                    result[col] = result[col].str.lower()
                elif self.to_upper:
                    result[col] = result[col].str.upper()
                
                # Remove special characters
                for char in self.remove_special_chars:
                    result[col] = result[col].str.replace(char, '', regex=False)
        
        return result

class BulkTypeConverter(BaseProcessor):
    def __init__(self, column_patterns: Dict[str, str]):
        self.column_patterns = column_patterns
        self._conversion_map = {
            "float": pd.to_numeric,
            "date": pd.to_datetime,
            "boolean": lambda x: x.astype(bool),
            "string": str
        }

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert column types based on patterns."""
        result = df.copy()
        
        for pattern, type_name in self.column_patterns.items():
            if type_name not in self._conversion_map:
                raise ValueError(f"Unsupported type conversion: {type_name}")
            
            matching_cols = [col for col in df.columns if self._matches_pattern(col, pattern)]
            converter = self._conversion_map[type_name]
            
            for col in matching_cols:
                try:
                    result[col] = converter(result[col])
                except Exception as e:
                    print(f"Warning: Failed to convert {col} to {type_name}: {str(e)}")
        
        return result

    def _matches_pattern(self, column: str, pattern: str) -> bool:
        """Check if column name matches the pattern."""
        if pattern.endswith(".*"):
            return column.startswith(pattern[:-2])
        return column == pattern

class GeospatialEnricher(BaseProcessor):
    def __init__(
        self,
        address_columns: List[str],
        add_columns: List[str]
    ):
        self.address_columns = address_columns
        self.add_columns = add_columns
        self._geocoder = None  # Initialize geocoding service here

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich addresses with geospatial data."""
        result = df.copy()
        
        for addr_col in self.address_columns:
            if addr_col in df.columns:
                # Simulate geocoding for demo
                result[f"{addr_col}_lat"] = 0.0
                result[f"{addr_col}_lon"] = 0.0
                
                if "timezone" in self.add_columns:
                    result[f"{addr_col}_timezone"] = "UTC"
        
        return result