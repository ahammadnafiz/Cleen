import numpy as np
import pandas as pd
import random
import string
import json
from datetime import datetime, timedelta


def generate_noisy_value(original_value, noise_type, probability=0.1):
    """Add different types of noise to values."""
    if random.random() > probability:
        return original_value
        
    if noise_type == "missing":
        return random.choice([None, np.nan, "", "N/A", "NULL", "undefined"])
    elif noise_type == "whitespace":
        return f"  {original_value}   {random.choice(['  ', '\t', '\n'])}  "
    elif noise_type == "special_chars":
        special_chars = "!@#$%^&*()_+-=[]{}|;:,.<>?"
        return f"{original_value}{random.choice(special_chars)}"
    elif noise_type == "case":
        return random.choice([
            original_value.upper(),
            original_value.lower(),
            original_value.title(),
            ''.join(random.choice([str.upper, str.lower])(c) for c in original_value)
        ])
    return original_value

def generate_complex_sample_data(num_rows=1000):
    """Generate complex sample data with various data quality issues."""
    np.random.seed(42)
    random.seed(42)
    
    # Generate base dates with some invalid formats
    dates = []
    for x in range(num_rows):
        date = datetime.now() - timedelta(days=x)
        if x % 20 == 0:  # Add some differently formatted dates
            date = random.choice([
                date.strftime("%d-%m-%Y"),
                date.strftime("%m/%d/%Y"),
                date.strftime("%Y.%m.%d"),
                f"{date.day}/{date.month}/{date.year}",
                "invalid_date"
            ])
        else:
            date = date.strftime("%Y-%m-%d")
        dates.append(date)
    
    # Generate complex product categories
    categories = [
        "Electronics > Smartphones > Android",
        "Electronics > Smartphones > iOS",
        "Electronics > Laptops > Gaming",
        "Clothing > Men > Casual > T-Shirts",
        "Clothing > Women > Formal > Dresses",
        "Home & Garden > Furniture > Living Room",
        "Home & Garden > Tools > Power Tools",
        "Sports > Outdoor > Camping",
    ]
    
    # Generate complex addresses
    def generate_address():
        street_types = ["St", "Ave", "Blvd", "Rd", "Lane", "Drive"]
        cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
        states = ["NY", "CA", "IL", "TX", "AZ"]
        
        number = random.randint(1, 9999)
        street = f"{random.choice(string.ascii_uppercase)}{random.choice(string.ascii_lowercase)}street"
        street_type = random.choice(street_types)
        city = random.choice(cities)
        state = random.choice(states)
        zipcode = f"{random.randint(10000, 99999)}"
        
        return f"{number} {street} {street_type}, {city}, {state} {zipcode}"
    
    # Generate complex JSON metadata
    def generate_metadata():
        return json.dumps({
            "device_info": {
                "type": random.choice(["mobile", "desktop", "tablet"]),
                "os": random.choice(["iOS", "Android", "Windows", "macOS"]),
                "browser": random.choice(["Chrome", "Firefox", "Safari"])
            },
            "user_preferences": {
                "language": random.choice(["en", "es", "fr", "de"]),
                "theme": random.choice(["light", "dark"]),
                "notifications": random.choice([True, False])
            },
            "session_data": {
                "duration": random.randint(60, 3600),
                "pages_visited": random.randint(1, 20),
                "actions": random.randint(5, 50)
            }
        })
    
    # Generate base data
    data = {
        "order_id": [generate_noisy_value(f"ORD-{str(x).zfill(8)}", "special_chars") 
                     for x in range(num_rows)],
        "order_date": [generate_noisy_value(date, "whitespace") 
                      for date in dates],
        "total_price": [generate_noisy_value(
            str(round(np.random.uniform(10, 1000), 2)),
            random.choice(["special_chars", "whitespace"])
        ) for _ in range(num_rows)],
        "currency": [generate_noisy_value(
            random.choice(["USD", "EUR", "GBP", "JPY", "CAD"]),
            "case"
        ) for _ in range(num_rows)],
        "customer_email": [generate_noisy_value(
            f"customer{x}@{random.choice(['example.com', 'test.com', 'demo.net'])}",
            random.choice(["case", "special_chars", "missing"])
        ) for x in range(num_rows)],
        "customer_address": [generate_noisy_value(generate_address(), "whitespace") 
                           for _ in range(num_rows)],
        "product_category": [generate_noisy_value(random.choice(categories), "case") 
                           for _ in range(num_rows)],
        "product_sku": [generate_noisy_value(
            f"SKU-{random.choice(string.ascii_uppercase)}{random.randint(1000, 9999)}",
            "special_chars"
        ) for _ in range(num_rows)],
        "quantity": [generate_noisy_value(
            str(random.randint(1, 10)),
            "special_chars"
        ) for _ in range(num_rows)],
        "payment_method": [generate_noisy_value(
            random.choice(["CREDIT_CARD", "PAYPAL", "BANK_TRANSFER", "CRYPTO"]),
            random.choice(["case", "whitespace", "special_chars"])
        ) for _ in range(num_rows)],
        "shipping_method": [generate_noisy_value(
            random.choice(["STANDARD", "EXPRESS", "OVERNIGHT", "PICKUP"]),
            random.choice(["case", "whitespace"])
        ) for _ in range(num_rows)],
        "metadata": [generate_noisy_value(generate_metadata(), "whitespace") 
                    for _ in range(num_rows)],
        "customer_notes": [generate_noisy_value(
            f"Customer note {x} with special requirements",
            random.choice(["case", "whitespace", "special_chars", "missing"])
        ) for x in range(num_rows)]
    }

    return pd.DataFrame(data)