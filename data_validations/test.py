"""
Example usage of the ProductFeedValidator with a sample product feed.
This script demonstrates how to validate a small sample of AI and semiconductor products.
"""

import json
import pandas as pd
from product_feed_validator import ProductFeedValidator

# Create a sample product feed with a mix of valid and invalid products
sample_products = [
    {
        "product_id": "SFXS-12345",
        "name": "SpectraForce X900 Ultra",
        "category": "GPUs",
        "brand": "Mankind Tech",
        "description": "High-performance GPU for gaming and AI applications",
        "price": 1299.99,
        "currency": "USD",
        "in_stock": True,
        "specifications": {
            "memory": "24 GB",
            "clock_speed": "1.8 GHz",
            "power_consumption": "350 W",
            "cores": 10240
        },
        "images": ["sfxs_12345_1.jpg", "sfxs_12345_2.jpg"]
    },
    {
        "product_id": "QMS-7890",
        "name": "QuantumMind System Pro",
        "category": "AI Hardware",
        "brand": "Mankind AI",
        "description": "Enterprise AI computing system for deep learning",
        "price": 24999.99,
        "currency": "USD",
        "in_stock": True,
        "specifications": {
            "processors": 8,
            "memory": "128 GB",
            "storage": "2 TB",
            "performance": "500 TFLOPS"
        },
        "images": ["qms_7890_1.jpg", "qms_7890_2.jpg"]
    },
    {
        "product_id": "invalid-id!",  # Invalid format
        "name": "EdgeNexus Platform",
        "category": "Gaming",  # Invalid category
        "brand": "Mankind Edge",
        "description": "Edge computing solution for AI workloads",
        "price": -100,  # Invalid price
        "currency": "BTC",  # Invalid currency
        "in_stock": "yes",  # Should be boolean
        "specifications": {
            "memory": "4GB",
            "clock_speed": "1200 MHz",  # Non-standardized unit
            "power_consumption": "15 W"
        },
        "images": []
    },
    {
        "product_id": "NXP-56789",
        "name": "NeuraX Processor X1",
        "category": "AI Hardware",
        "brand": "Mankind Neural",
        "description": "High-performance AI processor",
        "price": 5499.99,
        "currency": "USD",
        "in_stock": True,
        "specifications": {
            "cores": 128,
            "memory": "32 GB",
            "bandwidth": "900 GB/s",
            "frequency": "1.5 GHz"
        },
        "images": ["nxp_56789.jpg"]
    }
]

def main():
    # Save sample data to a temporary file
    sample_df = pd.DataFrame(sample_products)
    sample_file = "sample_product_feed.json"
    sample_df.to_json(sample_file, orient="records", indent=2)
    
    print(f"Created sample product feed with {len(sample_products)} products")
    
    # Create validator instance
    validator = ProductFeedValidator()
    
    print("Starting validation process...")
    
    # Validate the file
    cleaned_df, validation_errors = validator.validate_file(sample_file)
    
    # Save cleaned data
    cleaned_file = "cleaned_product_feed.csv"
    validator.save_cleaned_data(cleaned_df, cleaned_file)
    
    # Generate validation report
    report_file = "validation_report.md"
    validator.generate_validation_report(validation_errors, report_file)
    
    # Print summary
    print("\nValidation Summary:")
    print(f"- Total products processed: {len(sample_products)}")
    print(f"- Products with errors: {len(validation_errors)}")
    print(f"- Cleaned data saved to: {cleaned_file}")
    print(f"- Validation report saved to: {report_file}")
    
    # Print errors for demonstration
    if validation_errors:
        print("\nValidation Errors Found:")
        for product_id, errors in validation_errors.items():
            print(f"\nProduct ID: {product_id}")
            for error in errors:
                print(f"  - {error}")
    else:
        print("\nNo validation errors found.")

if __name__ == "__main__":
    main()