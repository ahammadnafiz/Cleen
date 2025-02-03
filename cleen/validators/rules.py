# cleen/validators/rules.py
from cleen.core.base import BaseValidator
from typing import Dict
import pandas as pd
import re

class PatternValidator(BaseValidator):
    def __init__(self, rules: Dict[str, str], error_handling: str = "raise"):
        self.rules = rules
        self.error_handling = error_handling
        self._compiled_patterns = {
            name: re.compile(pattern) for name, pattern in rules.items()
        }

    def validate(self, df: pd.DataFrame) -> bool:
        """Validate DataFrame against pattern rules."""
        valid_mask = pd.Series(True, index=df.index)
        
        for col, pattern in self._compiled_patterns.items():
            if col in df.columns:
                valid_mask &= df[col].str.match(pattern).fillna(False)
        
        if self.error_handling == "quarantine":
            return valid_mask
        elif self.error_handling == "raise" and not valid_mask.all():
            raise ValueError(f"Pattern validation failed for {(~valid_mask).sum()} rows")
        
        return valid_mask