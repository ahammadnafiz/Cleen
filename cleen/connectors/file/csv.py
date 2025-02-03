# cleen/connectors/file/csv.py
from typing import Dict, Any, Optional, List
import pandas as pd
from ...core.base import BaseConnector

class CsvConnector(BaseConnector):
    def __init__(
        self,
        path: str,
        encoding: str = "utf-8",
        delimiter: str = ",",
        sampling: Optional[Dict[str, Any]] = None,
        datetime_formats: Optional[Dict[str, Any]] = None,
        null_values: Optional[List[str]] = None
    ):
        self.path = path
        self.encoding = encoding
        self.delimiter = delimiter
        self.sampling = sampling or {}
        self.datetime_formats = datetime_formats or {}
        self.null_values = null_values or []

    def load(self) -> pd.DataFrame:
        df = pd.read_csv(
            self.path,
            encoding=self.encoding,
            delimiter=self.delimiter,
            na_values=self.null_values
        )
        
        # Apply datetime parsing
        for col, fmt in self.datetime_formats.items():
            if fmt == "auto":
                df[col] = pd.to_datetime(df[col], infer_datetime_format=True)
            else:
                df[col] = pd.to_datetime(df[col], format=fmt)
        
        # Apply sampling if specified
        if self.sampling:
            if self.sampling["strategy"] == "header_based":
                df = df.head(self.sampling["sample_size"])
        
        return df

    def save(self, df: pd.DataFrame) -> None:
        df.to_csv(self.path, index=False, encoding=self.encoding, sep=self.delimiter)