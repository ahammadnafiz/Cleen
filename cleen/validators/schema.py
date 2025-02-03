# cleen/validators/schema.py
from typing import Dict, Any
import pandas as pd
from cleen.core.base import BaseValidator

class SchemaValidator(BaseValidator):
    def __init__(self, rules: Dict[str, Dict[str, Any]], strict_mode: str = "strict"):
        self.rules = rules
        self.strict_mode = strict_mode
        self._compiled_rules = None

    def infer_types(self, df: pd.DataFrame) -> 'SchemaValidator':
        """Infer and update schema rules based on DataFrame content."""
        for column in df.columns:
            if column not in self.rules:
                # Handle dynamic column patterns
                for pattern, rule in self.rules.items():
                    if pattern.endswith(".*") and column.startswith(pattern[:-2]):
                        self.rules[column] = rule.copy()
        return self

    def validate(self, df: pd.DataFrame) -> bool:
        """Validate DataFrame against schema rules."""
        valid_mask = pd.Series(True, index=df.index)
        
        for column, rules in self.rules.items():
            if column in df.columns:
                # Type validation
                if rules.get("type"):
                    if rules["type"] == "date":
                        valid_mask &= pd.to_datetime(df[column], errors='coerce').notna()
                    elif rules["type"] == "float":
                        valid_mask &= pd.to_numeric(df[column], errors='coerce').notna()
                        if "min" in rules:
                            valid_mask &= df[column] >= rules["min"]
                
                # Pattern validation
                if "regex" in rules:
                    valid_mask &= df[column].str.match(rules["regex"]).fillna(False)
                
                # Category validation
                if "options" in rules:
                    valid_mask &= df[column].isin(rules["options"])
        
        return valid_mask