from typing import Dict, Any, Optional
import pandas as pd
from cleen.core.base import BaseValidator

class SchemaValidator(BaseValidator):
    VALID_ERROR_ACTIONS = {"reject", "quarantine", "ignore"}
    DEFAULT_ERROR_HANDLING = {
        "missing_required": "reject",
        "invalid_type": "quarantine", 
        "invalid_value": "quarantine",
        "invalid_format": "quarantine"
    }

    def __init__(
        self, 
        rules: Dict[str, Dict[str, Any]], 
        strict_mode: str = "strict",
        error_handling: Optional[Dict[str, str]] = None
    ):
        self.rules = rules
        self.strict_mode = strict_mode
        self.error_handling = self._validate_error_handling(error_handling or self.DEFAULT_ERROR_HANDLING)
        self._compiled_rules = None

    def _validate_error_handling(self, error_handling: Dict[str, str]) -> Dict[str, str]:
        """Validate error handling configuration."""
        for key, action in error_handling.items():
            if action not in self.VALID_ERROR_ACTIONS:
                raise ValueError(f"Invalid error handling action '{action}' for '{key}'. Must be one of {self.VALID_ERROR_ACTIONS}")
        return error_handling

    def validate(self, df: pd.DataFrame) -> bool:
        """Validate DataFrame against schema rules."""
        valid_mask = pd.Series(True, index=df.index)
        
        for column, rules in self.rules.items():
            if column in df.columns:
                column_mask = pd.Series(True, index=df.index)
                
                # Type validation
                if rules.get("type"):
                    if rules["type"] == "date":
                        type_mask = pd.to_datetime(df[column], errors='coerce').notna()
                    elif rules["type"] == "float":
                        type_mask = pd.to_numeric(df[column], errors='coerce').notna()
                        if not type_mask.all() and self.error_handling["invalid_type"] == "reject":
                            return False
                        column_mask &= type_mask
                        
                        if "min" in rules:
                            value_mask = df[column] >= rules["min"]
                            if not value_mask.all() and self.error_handling["invalid_value"] == "reject":
                                return False
                            column_mask &= value_mask
                
                # Pattern validation
                if "regex" in rules:
                    format_mask = df[column].str.match(rules["regex"]).fillna(False)
                    if not format_mask.all() and self.error_handling["invalid_format"] == "reject":
                        return False
                    column_mask &= format_mask
                
                # Category validation
                if "options" in rules:
                    value_mask = df[column].isin(rules["options"])
                    if not value_mask.all() and self.error_handling["invalid_value"] == "reject":
                        return False
                    column_mask &= value_mask

                valid_mask &= column_mask

            elif rules.get("required", False) and self.error_handling["missing_required"] == "reject":
                return False
                
        return valid_mask