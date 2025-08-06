#!/usr/bin/env python3
"""
Mankind Matrix Product Feed Validator and Cleaner

This script validates and cleans incoming product feeds for the Mankind Matrix platform.
It ensures all product entries follow proper formats, units, and standards before they
are integrated into the platform.
"""

import os
import json
import pandas as pd
import numpy as np
import re
import logging
from datetime import datetime
from typing import Dict, List, Union, Any, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"product_feed_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ProductFeedValidator")

class ProductFeedValidator:
    """
    Validates and cleans product feeds for Mankind Matrix.
    """
    
    # Define product categories based on Mankind Matrix catalog
    VALID_CATEGORIES = [
        "GPUs", "AI Hardware", "AI Software", "Semiconductors", "Data Center Products",
        "Automotive Solutions", "Networking Products", "Cloud Computing", "Supercomputing",
        "Gaming Technologies", "AI Applications", "Professional Workstation"
    ]
    
    # Define expected fields and their data types
    EXPECTED_FIELDS = {
        "product_id": str,
        "name": str,
        "category": str,
        "brand": str,
        "description": str,
        "price": float,
        "currency": str,
        "in_stock": bool,
        "specifications": dict,
        "images": list
    }
    
    # Define validation rules for specific fields
    VALIDATION_RULES = {
        "product_id": r"^[A-Za-z0-9\-_]{6,50}$",  # Alphanumeric with hyphens/underscores, 6-50 chars
        "price": (0, 1000000),  # Price range
        "currency": ["USD", "EUR", "GBP", "JPY", "CNY"],  # Valid currencies
    }
    
    # Unit standardization mappings
    UNIT_MAPPINGS = {
        "memory": {
            "GB": 1,
            "TB": 1024,
            "MB": 1/1024,
        },
        "frequency": {
            "GHz": 1,
            "MHz": 1/1000,
            "kHz": 1/1000000,
        },
        "power": {
            "W": 1,
            "kW": 1000,
            "mW": 1/1000,
        }
    }

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the validator with optional custom configuration.
        
        Args:
            config_path: Path to custom configuration JSON file
        """
        self.config = {}
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                self.config = json.load(f)
                
            # Override default rules with custom config if provided
            if "valid_categories" in self.config:
                self.VALID_CATEGORIES = self.config["valid_categories"]
            if "validation_rules" in self.config:
                self.VALIDATION_RULES.update(self.config["validation_rules"])
        
        logger.info(f"ProductFeedValidator initialized with {len(self.VALID_CATEGORIES)} categories")

    def validate_file(self, file_path: str) -> Tuple[pd.DataFrame, Dict[str, List[str]]]:
        """
        Validate a product feed file and return a cleaned DataFrame.
        
        Args:
            file_path: Path to the product feed file (CSV, JSON, XLSX)
            
        Returns:
            Tuple containing:
            - Cleaned DataFrame
            - Dictionary of validation errors keyed by product_id
        """
        logger.info(f"Starting validation of file: {file_path}")
        
        # Detect file format and load data
        file_ext = os.path.splitext(file_path)[1].lower()
        if file_ext == '.csv':
            df = pd.read_csv(file_path)
        elif file_ext == '.json':
            df = pd.read_json(file_path)
        elif file_ext in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path)
        else:
            error_msg = f"Unsupported file format: {file_ext}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"Loaded {len(df)} product entries for validation")
        
        # Track validation errors by product_id
        validation_errors = {}
        
        # Validate and clean the DataFrame
        cleaned_df = self._validate_and_clean_dataframe(df, validation_errors)
        
        logger.info(f"Validation complete. Found errors in {len(validation_errors)} products")
        return cleaned_df, validation_errors

    def validate_product(self, product: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
        """
        Validate and clean a single product entry.
        
        Args:
            product: Dictionary containing product data
            
        Returns:
            Tuple containing:
            - Cleaned product dictionary
            - List of validation errors
        """
        errors = []
        cleaned_product = product.copy()
        
        # Check for required fields
        for field, field_type in self.EXPECTED_FIELDS.items():
            if field not in product:
                errors.append(f"Missing required field: {field}")
                continue
                
            # Type validation
            if not isinstance(product[field], field_type):
                errors.append(f"Field '{field}' must be of type {field_type.__name__}")
                # Try to convert if possible
                try:
                    if field_type == bool and isinstance(product[field], str):
                        # Handle boolean conversions from strings
                        value = product[field].lower()
                        if value in ["true", "yes", "1", "y"]:
                            cleaned_product[field] = True
                        elif value in ["false", "no", "0", "n"]:
                            cleaned_product[field] = False
                    else:
                        cleaned_product[field] = field_type(product[field])
                except (ValueError, TypeError):
                    cleaned_product[field] = None
        
        # Validate specific fields using rules
        if "product_id" in cleaned_product and cleaned_product["product_id"]:
            if not re.match(self.VALIDATION_RULES["product_id"], str(cleaned_product["product_id"])):
                errors.append(f"Invalid product_id format: {cleaned_product['product_id']}")
        
        if "category" in cleaned_product and cleaned_product["category"]:
            if cleaned_product["category"] not in self.VALID_CATEGORIES:
                errors.append(f"Invalid category: {cleaned_product['category']}. Must be one of: {', '.join(self.VALID_CATEGORIES)}")
        
        if "price" in cleaned_product and cleaned_product["price"] is not None:
            min_price, max_price = self.VALIDATION_RULES["price"]
            if not (min_price <= float(cleaned_product["price"]) <= max_price):
                errors.append(f"Price out of valid range ({min_price}-{max_price}): {cleaned_product['price']}")
        
        if "currency" in cleaned_product and cleaned_product["currency"]:
            if cleaned_product["currency"] not in self.VALIDATION_RULES["currency"]:
                errors.append(f"Invalid currency: {cleaned_product['currency']}. Must be one of: {', '.join(self.VALIDATION_RULES['currency'])}")
        
        # Clean and standardize specifications if present
        if "specifications" in cleaned_product and cleaned_product["specifications"]:
            cleaned_product["specifications"] = self._standardize_specifications(cleaned_product["specifications"])
        
        return cleaned_product, errors

    def _validate_and_clean_dataframe(self, df: pd.DataFrame, validation_errors: Dict[str, List[str]]) -> pd.DataFrame:
        """
        Validate and clean an entire DataFrame of products.
        
        Args:
            df: DataFrame containing product data
            validation_errors: Dictionary to store validation errors keyed by product_id
            
        Returns:
            Cleaned DataFrame
        """
        # Create a new DataFrame for cleaned data
        cleaned_records = []
        
        # Process each row
        for _, row in df.iterrows():
            # Convert row to dictionary
            product = row.to_dict()
            
            # Get product ID for error tracking
            product_id = product.get("product_id", f"unknown_{_}")
            
            # Validate and clean the product
            cleaned_product, errors = self.validate_product(product)
            
            # Store errors if any
            if errors:
                validation_errors[product_id] = errors
            
            cleaned_records.append(cleaned_product)
        
        # Create new DataFrame from cleaned records
        cleaned_df = pd.DataFrame(cleaned_records)
        
        return cleaned_df

    def _standardize_specifications(self, specs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Standardize units in product specifications.
        
        Args:
            specs: Dictionary of product specifications
            
        Returns:
            Dictionary with standardized specifications
        """
        standardized_specs = {}
        
        for key, value in specs.items():
            # Skip if value is None
            if value is None:
                standardized_specs[key] = None
                continue
                
            # Check if this is a value with a unit
            if isinstance(value, str):
                # Try to extract numeric value and unit
                match = re.match(r"^([\d.]+)\s*([a-zA-Z]+)$", value.strip())
                if match:
                    numeric_value = float(match.group(1))
                    unit = match.group(2)
                    
                    # Check if we need to standardize this unit
                    for measure_type, unit_map in self.UNIT_MAPPINGS.items():
                        if unit in unit_map:
                            # Convert to standard unit
                            if key.lower().find("memory") >= 0 or key.lower().find("storage") >= 0:
                                # Assume this is memory/storage specification
                                standardized_value = numeric_value * unit_map[unit]
                                standard_unit = "GB"  # Use GB as standard unit for memory/storage
                                standardized_specs[key] = f"{standardized_value:.2f} {standard_unit}"
                                break
                            elif key.lower().find("frequency") >= 0 or key.lower().find("speed") >= 0 or key.lower().find("clock") >= 0:
                                # Assume this is frequency specification
                                standardized_value = numeric_value * unit_map[unit]
                                standard_unit = "GHz"  # Use GHz as standard unit for frequency
                                standardized_specs[key] = f"{standardized_value:.2f} {standard_unit}"
                                break
                            elif key.lower().find("power") >= 0 or key.lower().find("watt") >= 0:
                                # Assume this is power specification
                                standardized_value = numeric_value * unit_map[unit]
                                standard_unit = "W"  # Use W as standard unit for power
                                standardized_specs[key] = f"{standardized_value:.2f} {standard_unit}"
                                break
                    else:
                        # No standardization needed or possible
                        standardized_specs[key] = value
                else:
                    # Not a value with unit
                    standardized_specs[key] = value
            else:
                # Not a string value
                standardized_specs[key] = value
        
        return standardized_specs

    def save_cleaned_data(self, df: pd.DataFrame, output_path: str, format: str = "csv") -> None:
        """
        Save the cleaned data to a file.
        
        Args:
            df: DataFrame containing cleaned product data
            output_path: Path to save the cleaned data
            format: Format to save the data in ('csv', 'json', 'xlsx')
        """
        format = format.lower()
        if format == 'csv':
            df.to_csv(output_path, index=False)
        elif format == 'json':
            df.to_json(output_path, orient='records', indent=2)
        elif format == 'xlsx':
            df.to_excel(output_path, index=False)
        else:
            raise ValueError(f"Unsupported output format: {format}")
        
        logger.info(f"Cleaned data saved to {output_path}")

    def generate_validation_report(self, validation_errors: Dict[str, List[str]], output_path: str) -> None:
        """
        Generate a report of validation errors.
        
        Args:
            validation_errors: Dictionary of validation errors keyed by product_id
            output_path: Path to save the validation report
        """
        with open(output_path, 'w') as f:
            f.write("# Mankind Matrix Product Feed Validation Report\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            if not validation_errors:
                f.write("No validation errors found. All products passed validation.\n")
            else:
                f.write(f"Found validation errors in {len(validation_errors)} products:\n\n")
                
                for product_id, errors in validation_errors.items():
                    f.write(f"## Product ID: {product_id}\n")
                    for error in errors:
                        f.write(f"- {error}\n")
                    f.write("\n")
        
        logger.info(f"Validation report saved to {output_path}")


def main():
    """
    Example usage of the ProductFeedValidator.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Validate and clean a product feed for Mankind Matrix')
    parser.add_argument('input_file', help='Path to input file (CSV, JSON, XLSX)')
    parser.add_argument('--output', help='Path to save cleaned data', default='cleaned_product_feed.csv')
    parser.add_argument('--format', choices=['csv', 'json', 'xlsx'], default='csv', help='Output format')
    parser.add_argument('--report', help='Path to save validation report', default='validation_report.md')
    parser.add_argument('--config', help='Path to custom configuration JSON file')
    
    args = parser.parse_args()
    
    # Initialize validator
    validator = ProductFeedValidator(config_path=args.config)
    
    try:
        # Validate and clean the file
        cleaned_df, validation_errors = validator.validate_file(args.input_file)
        
        # Save cleaned data
        validator.save_cleaned_data(cleaned_df, args.output, args.format)
        
        # Generate validation report
        validator.generate_validation_report(validation_errors, args.report)
        
        print(f"Validation complete. Cleaned data saved to {args.output}")
        print(f"Validation report saved to {args.report}")
        
        if validation_errors:
            print(f"Found {len(validation_errors)} products with validation errors.")
            print(f"Please check the validation report at {args.report} for details.")
        else:
            print("All products passed validation!")
            
    except Exception as e:
        logger.error(f"Error during validation: {str(e)}", exc_info=True)
        print(f"Error: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())