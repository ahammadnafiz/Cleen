# cleen/connectors/file/parquet.py
from ...core.base import BaseConnector
from typing import List, Optional
import pandas as pd

class ParquetConnector(BaseConnector):
    def __init__(
        self,
        path: str,
        partition_by: Optional[List[str]] = None,
        compression: str = "snappy"
    ):
        self.path = path
        self.partition_by = partition_by
        self.compression = compression

    def load(self) -> pd.DataFrame:
        return pd.read_parquet(self.path)

    def save(self, df: pd.DataFrame) -> None:
        if self.partition_by:
            df.to_parquet(
                self.path,
                partition_cols=self.partition_by,
                compression=self.compression
            )
        else:
            df.to_parquet(self.path, compression=self.compression)  