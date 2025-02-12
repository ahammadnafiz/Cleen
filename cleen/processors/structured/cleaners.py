from typing import List, Optional, Dict, Any, Union, Callable
import pandas as pd
import numpy as np
import re
from datetime import datetime
import logging
from ...core.base import BaseProcessor

class ColumnSanitizer(BaseProcessor):
    """
    A processor for sanitizing and standardizing column values with enhanced capabilities.
    """
    def __init__(
        self,
        strip_whitespace: bool = True,
        remove_special_chars: Optional[List[str]] = None,
        columns: Optional[List[str]] = None,
        to_lower: bool = False,
        to_upper: bool = False,
        replace_patterns: Optional[Dict[str, str]] = None,
        max_length: Optional[int] = None,
        remove_urls: bool = False,
        remove_emails: bool = False,
        remove_numbers: bool = False,
        custom_replacements: Optional[Dict[str, str]] = None,
        fill_empty: Optional[str] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the column sanitizer with enhanced options.
        
        Args:
            strip_whitespace: Whether to strip whitespace
            remove_special_chars: List of special characters to remove
            columns: Specific columns to process (None for all string columns)
            to_lower: Convert to lowercase
            to_upper: Convert to uppercase
            replace_patterns: Dict of regex patterns and their replacements
            max_length: Maximum length for string values
            remove_urls: Whether to remove URLs
            remove_emails: Whether to remove email addresses
            remove_numbers: Whether to remove numeric characters
            custom_replacements: Dictionary of custom string replacements
            fill_empty: Value to use for empty strings after processing
            logger: Custom logger instance
        """
        self.strip_whitespace = strip_whitespace
        self.remove_special_chars = remove_special_chars or []
        self.columns = columns
        self.to_lower = to_lower
        self.to_upper = to_upper
        self.replace_patterns = replace_patterns or {}
        self.max_length = max_length
        self.remove_urls = remove_urls
        self.remove_emails = remove_emails
        self.remove_numbers = remove_numbers
        self.custom_replacements = custom_replacements or {}
        self.fill_empty = fill_empty
        self.logger = logger or logging.getLogger(__name__)

        # Precompile regex patterns
        self.url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
        self.email_pattern = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+')

    def _sanitize_string(self, value):
        """Sanitize a single value with improved error handling."""
        if pd.isna(value) or value is None:
            return self.fill_empty if self.fill_empty is not None else value

        try:
            result = str(value)

            if self.strip_whitespace:
                result = result.strip()

            if self.to_lower:
                result = result.lower()
            elif self.to_upper:
                result = result.upper()

            if self.remove_urls:
                result = self.url_pattern.sub('', result)

            if self.remove_emails:
                result = self.email_pattern.sub('', result)

            if self.remove_numbers:
                result = re.sub(r'\d+', '', result)

            for char in self.remove_special_chars:
                result = result.replace(char, '')

            for pattern, replacement in self.replace_patterns.items():
                replacement = str(replacement) if replacement is not None else ''
                result = re.sub(pattern, replacement, result)

            for old, new in self.custom_replacements.items():
                new = str(new) if new is not None else ''
                result = result.replace(str(old), new)

            if self.max_length and len(result) > self.max_length:
                result = result[:self.max_length]

            if not result and self.fill_empty is not None:
                result = self.fill_empty

            return result
        except Exception as e:
            self.logger.warning(f"Error sanitizing value '{value}': {str(e)}")
            return value

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sanitize specified columns in the DataFrame with error handling."""
        result = df.copy()
        target_columns = self.columns or df.select_dtypes(include=['object']).columns
        
        for col in target_columns:
            if col in result.columns:
                try:
                    result[col] = result[col].apply(self._sanitize_string)
                except Exception as e:
                    self.logger.error(f"Error processing column {col}: {str(e)}")
        
        return result

class BulkTypeConverter(BaseProcessor):
    """
    A processor for converting column types with enhanced type inference and validation.
    """
    def __init__(
        self,
        column_patterns: Dict[str, str],
        date_formats: Optional[List[str]] = None,
        coerce_errors: bool = True,
        custom_converters: Optional[Dict[str, Callable]] = None,
        validate_unique: bool = False,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the type converter with enhanced options.
        
        Args:
            column_patterns: Dictionary mapping column patterns to desired types
            date_formats: List of date formats to try when converting to datetime
            coerce_errors: Whether to coerce conversion errors to NaN
            custom_converters: Dictionary of custom type conversion functions
            validate_unique: Whether to validate uniqueness after conversion
            logger: Custom logger instance
        """
        self.column_patterns = column_patterns
        self.date_formats = date_formats or ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y']
        self.coerce_errors = coerce_errors
        self.custom_converters = custom_converters or {}
        self.validate_unique = validate_unique
        self.logger = logger or logging.getLogger(__name__)

        self._conversion_map = {
            "float": lambda x: pd.to_numeric(x, errors='coerce' if self.coerce_errors else 'raise'),
            "integer": lambda x: pd.to_numeric(x, errors='coerce' if self.coerce_errors else 'raise').astype('Int64'),
            "date": self._convert_datetime,
            "boolean": self._convert_boolean,
            "string": str,
            "category": pd.Categorical,
            "timedelta": pd.to_timedelta
        }
        self._conversion_map.update(self.custom_converters)

    def _convert_datetime(self, series: pd.Series) -> pd.Series:
        """Convert series to datetime with multiple format attempts."""
        for date_format in self.date_formats:
            try:
                return pd.to_datetime(series, format=date_format, errors='coerce' if self.coerce_errors else 'raise')
            except Exception:
                continue
        return pd.to_datetime(series, infer_datetime_format=True, errors='coerce' if self.coerce_errors else 'raise')

    def _convert_boolean(self, series: pd.Series) -> pd.Series:
        """Convert series to boolean with enhanced mapping."""
        true_values = {'true', 'yes', '1', 't', 'y', 'on'}
        false_values = {'false', 'no', '0', 'f', 'n', 'off'}
        
        def map_bool(x):
            if pd.isna(x):
                return np.nan
            if isinstance(x, (bool, np.bool_)):
                return x
            if isinstance(x, (int, float)):
                return bool(x)
            if isinstance(x, str):
                x = x.lower().strip()
                if x in true_values:
                    return True
                if x in false_values:
                    return False
            return np.nan

        return series.apply(map_bool)

    def _matches_pattern(self, column: str, pattern: str) -> bool:
        """Check if column name matches the pattern using regex."""
        if pattern.endswith(".*"):
            return column.startswith(pattern[:-2])
        try:
            return bool(re.match(f"^{pattern}$", column))
        except re.error:
            return column == pattern

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert column types with enhanced error handling and validation."""
        result = df.copy()
        
        for pattern, type_name in self.column_patterns.items():
            if type_name not in self._conversion_map:
                raise ValueError(f"Unsupported type conversion: {type_name}")
            
            matching_cols = [col for col in df.columns if self._matches_pattern(col, pattern)]
            converter = self._conversion_map[type_name]
            
            for col in matching_cols:
                try:
                    result[col] = converter(result[col])
                    
                    if self.validate_unique and not result[col].is_unique:
                        self.logger.warning(f"Column {col} contains duplicate values after conversion")
                        
                except Exception as e:
                    self.logger.error(f"Error converting column {col} to {type_name}: {str(e)}")
                    if not self.coerce_errors:
                        raise
        
        return result

class GeospatialEnricher(BaseProcessor):
    """
    A processor for enriching address data with geospatial information.
    """
    def __init__(
        self,
        address_columns: List[str],
        add_columns: List[str],
        geocoding_service: str = "nominatim",
        batch_size: int = 100,
        rate_limit: float = 1.0,
        cache_file: Optional[str] = None,
        timeout: int = 30,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the geospatial enricher with enhanced options.
        
        Args:
            address_columns: List of columns containing addresses
            add_columns: List of additional columns to add
            geocoding_service: Name of the geocoding service to use
            batch_size: Number of addresses to process in each batch
            rate_limit: Minimum time between API calls in seconds
            cache_file: Path to cache file for geocoding results
            timeout: Timeout for API calls in seconds
            logger: Custom logger instance
        """
        self.address_columns = address_columns
        self.add_columns = add_columns
        self.geocoding_service = geocoding_service
        self.batch_size = batch_size
        self.rate_limit = rate_limit
        self.cache_file = cache_file
        self.timeout = timeout
        self.logger = logger or logging.getLogger(__name__)
        
        # Initialize cache if specified
        self._cache = {}
        if self.cache_file:
            try:
                self._cache = pd.read_pickle(self.cache_file)
            except Exception as e:
                self.logger.warning(f"Failed to load cache file: {str(e)}")

    def _geocode_address(self, address: str) -> Dict[str, Any]:
        """Geocode a single address with caching."""
        if address in self._cache:
            return self._cache[address]

        # Here you would implement actual geocoding logic
        # For now, return dummy data
        result = {
            "latitude": 0.0,
            "longitude": 0.0,
            "timezone": "UTC",
            "country": "Unknown",
            "postal_code": "00000",
            "state": "Unknown",
            "city": "Unknown"
        }
        
        self._cache[address] = result
        return result

    def _save_cache(self) -> None:
        """Save the geocoding cache to file."""
        if self.cache_file:
            try:
                pd.to_pickle(self._cache, self.cache_file)
            except Exception as e:
                self.logger.error(f"Failed to save cache file: {str(e)}")

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich addresses with geospatial data using batching and caching."""
        result = df.copy()
        
        for addr_col in self.address_columns:
            if addr_col in df.columns:
                # Process addresses in batches
                for i in range(0, len(df), self.batch_size):
                    batch = df[addr_col].iloc[i:i + self.batch_size]
                    
                    for idx, address in batch.items():
                        try:
                            if pd.isna(address):
                                continue
                                
                            geocoded = self._geocode_address(str(address))
                            
                            # Add requested columns
                            if "latitude" in self.add_columns:
                                result.at[idx, f"{addr_col}_lat"] = geocoded["latitude"]
                            if "longitude" in self.add_columns:
                                result.at[idx, f"{addr_col}_lon"] = geocoded["longitude"]
                            if "timezone" in self.add_columns:
                                result.at[idx, f"{addr_col}_timezone"] = geocoded["timezone"]
                            if "country" in self.add_columns:
                                result.at[idx, f"{addr_col}_country"] = geocoded["country"]
                            if "postal_code" in self.add_columns:
                                result.at[idx, f"{addr_col}_postal"] = geocoded["postal_code"]
                            if "state" in self.add_columns:
                                result.at[idx, f"{addr_col}_state"] = geocoded["state"]
                            if "city" in self.add_columns:
                                result.at[idx, f"{addr_col}_city"] = geocoded["city"]
                                
                        except Exception as e:
                            self.logger.error(f"Error geocoding address '{address}': {str(e)}")
        
        # Save updated cache
        self._save_cache()
        
        return result
