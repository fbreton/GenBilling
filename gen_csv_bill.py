import pandas as pd
import random
import sys
from datetime import datetime, timedelta

try:
    target = sys.argv[3]

    start_date = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
    end_date = datetime.strptime(sys.argv[2], '%Y-%m-%d').date()
    time_range = pd.date_range(start=start_date, end=end_date, freq='D')

except Exception as e:
    print("Usage: python gen_csv_bill.py <start_date> <end_date> <target>")
    print("  - date format: YYYY-MM-DD")
    sys.exit(1)

# Generate synthetic Databricks billing data
def generate_databricks_billing():
    sku_names = ["Standard_D3_v2", "Premium_D4_v2", "Jobs Compute DBU", "Interactive Compute DBU"]
    sku_families = {
        "Standard_D3_v2": "Compute",
        "Premium_D4_v2": "Compute",
        "Jobs Compute DBU": "DBU",
        "Interactive Compute DBU": "DBU"
    }
    sku_unitprice = {
        "Standard_D3_v2": 0.25,
        "Premium_D4_v2": 0.4,
        "Jobs Compute DBU": 0.15,
        "Interactive Compute DBU": 0.55
    }
    regions = ["us-east-1", "us-west-2", "us-central1","eu-west-1","eu-central-1","ap-southeast-1","ap-northeast-1"]
    workspace = ["Marketing", "Analytics", "Data"]

    for date in time_range:
        for wk in workspace:
            usage = round(random.uniform(0.5, 10.0), 2)
            sku = random.choice(sku_names)
            cost = round(usage * sku_unitprice[sku], 2)
            data.append({
                "fixed.project_name": wk,
                "fixed.sku_description": sku,
                "usage_date": date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "fixed.pricing_unit": sku_families[sku],
                "metric.usage": usage,
            "metric.unit_price": sku_unitprice[sku],
            "metric.cost": cost,
            "fixed.region": random.choice(regions)
        })

data = []

match target:
    case"Databricks" | "databricks":
        generate_databricks_billing()
    case _:
        print("Invalid target")
        sys.exit(1)

# Create DataFrame
df = pd.DataFrame(data)

# Save to CSV
file_path = "./csvfiles/billing.csv"
df.to_csv(file_path, index=False)
