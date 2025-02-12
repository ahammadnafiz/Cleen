from typing import Dict, Any, Optional, List, Union
import pandas as pd
import numpy as np
import logging
from pathlib import Path
from ...core.base import BaseConnector

class CsvConnector(BaseConnector):

    def __init__(
        self,
        path: Union[str, Path],
        encoding: str = "utf-8",
        delimiter: str = ",",
        sampling: Optional[Dict[str, Any]] = None,
        datetime_formats: Optional[Dict[str, Any]] = None,
        null_values: Optional[List[str]] = None,
        dtype_map: Optional[Dict[str, Any]] = None,
        skip_rows: Optional[Union[int, List[int]]] = None,
        chunk_size: Optional[int] = None,
        max_rows: Optional[int] = None,
        validate_schema: bool = True,
        clean_column_names: bool = True,
        remove_duplicates: bool = True,
        logger: Optional[logging.Logger] = None
    ):
    
        self.path = Path(path)
        self.encoding = encoding
        self.delimiter = delimiter
        self.sampling = sampling or {}
        self.datetime_formats = datetime_formats or {}
        self.null_values = null_values or ['', 'NA', 'N/A', 'null', 'NULL', 'none', 'None']
        self.dtype_map = dtype_map or {}
        self.skip_rows = skip_rows
        self.chunk_size = chunk_size
        self.max_rows = max_rows
        self.validate_schema = validate_schema
        self.clean_column_names = clean_column_names
        self.remove_duplicates = remove_duplicates
        self.logger = logger or logging.getLogger(__name__)

    def _clean_column_name(self, name: str) -> str:
        """Clean and standardize column names."""
        return (name.strip()
                .lower()
                .replace(' ', '_')
                .replace('-', '_')
                .replace('/', '_')
                .replace('\\', '_'))

    def _validate_dtypes(self, df: pd.DataFrame) -> None:
        """Validate column data types against schema."""
        for col, dtype in self.dtype_map.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype)
                except Exception as e:
                    self.logger.warning(f"Failed to convert column {col} to {dtype}: {str(e)}")

    def _handle_datetime_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        for col, formats in self.datetime_formats.items():
            if col in df.columns:
                def parse_datetime(x, formats=formats):
                    if pd.isna(x):
                        return pd.NaT
                    for fmt in formats:
                        try:
                            return pd.to_datetime(x, format=fmt)
                        except:
                            continue
                    return pd.NaT
                df[col] = df[col].apply(parse_datetime)
        return df

    def _apply_sampling(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply sampling strategy to the dataframe."""
        if not self.sampling:
            return df
            
        strategy = self.sampling.get("strategy", "")
        sample_size = self.sampling.get("sample_size", len(df))
        
        if strategy == "header_based":
            return df.head(sample_size)
        elif strategy == "random":
            return df.sample(n=min(sample_size, len(df)))
        elif strategy == "systematic":
            step = max(len(df) // sample_size, 1)
            return df.iloc[::step].head(sample_size)
        
        return df

    def load(self) -> pd.DataFrame:
        """
        Load CSV file with robust error handling and data processing.
        
        Returns:
            pandas.DataFrame: Processed dataframe
        """
        try:
            # Read CSV with chunking if specified
            if self.chunk_size:
                chunks = []
                for chunk in pd.read_csv(
                    self.path,
                    encoding=self.encoding,
                    delimiter=self.delimiter,
                    na_values=self.null_values,
                    dtype=self.dtype_map,
                    skiprows=self.skip_rows,
                    chunksize=self.chunk_size,
                    on_bad_lines='warn',
                    low_memory=False
                ):
                    chunks.append(chunk)
                    if self.max_rows and len(chunks) * self.chunk_size >= self.max_rows:
                        break
                df = pd.concat(chunks, ignore_index=True)
                if self.max_rows:
                    df = df.head(self.max_rows)
            else:
                df = pd.read_csv(
                    self.path,
                    encoding=self.encoding,
                    delimiter=self.delimiter,
                    na_values=self.null_values,
                    dtype=self.dtype_map,
                    skiprows=self.skip_rows,
                    nrows=self.max_rows,
                    on_bad_lines='warn',
                    low_memory=False
                )

            # Clean column names if requested
            if self.clean_column_names:
                df.columns = [self._clean_column_name(col) for col in df.columns]

            # Handle datetime columns
            df = self._handle_datetime_columns(df)

            # Validate schema if requested
            if self.validate_schema:
                self._validate_dtypes(df)

            # Remove duplicates if requested
            if self.remove_duplicates:
                df = df.drop_duplicates()

            # Apply sampling
            df = self._apply_sampling(df)

            return df

        except Exception as e:
            self.logger.error(f"Error loading CSV file {self.path}: {str(e)}")
            raise

    def save(self, df: pd.DataFrame, **kwargs) -> None:
        """
        Save DataFrame to CSV with error handling.
        
        Args:
            df: DataFrame to save
            **kwargs: Additional arguments passed to pd.DataFrame.to_csv()
        """
        try:
            # Create directory if it doesn't exist
            self.path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save with specified parameters
            df.to_csv(
                self.path,
                index=False,
                encoding=self.encoding,
                sep=self.delimiter,
                **kwargs
            )
        except Exception as e:
            self.logger.error(f"Error saving CSV file {self.path}: {str(e)}")
            raise