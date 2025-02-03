# cleen/core/base.py
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
from pathlib import Path
import pandas as pd

class BaseConnector(ABC):
    """Base class for all data connectors."""
    
    @abstractmethod
    def load(self) -> pd.DataFrame:
        """Load data from source."""
        pass
    
    @abstractmethod
    def save(self, df: pd.DataFrame) -> None:
        """Save data to destination."""
        pass

class BaseProcessor(ABC):
    """Base class for all data processors."""
    
    @abstractmethod
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process the dataframe."""
        pass

class BaseValidator(ABC):
    """Base class for all data validators."""
    
    @abstractmethod
    def validate(self, df: pd.DataFrame) -> bool:
        """Validate the dataframe."""
        pass

class BaseLLMSkill(ABC):
    """Base class for LLM-based processing skills."""
    
    @abstractmethod
    def apply(self, texts: List[str]) -> List[str]:
        """Apply the skill to a list of texts."""
        pass

class PipelineStep:
    """Represents a single step in the data pipeline."""
    
    def __init__(self, processor: Union[BaseProcessor, BaseValidator], name: Optional[str] = None):
        self.processor = processor
        self.name = name or processor.__class__.__name__
        self.metrics = {}

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the pipeline step."""
        return self.processor.process(df) if isinstance(self.processor, BaseProcessor) \
            else df[self.processor.validate(df)]