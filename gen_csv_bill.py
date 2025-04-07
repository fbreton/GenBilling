import pandas as pd
import random
import sys
from datetime import datetime
import requests
import argparse

# Constants to modify regarding your environment
filename = "./csvfiles/billing.csv"         # CSV file path 
token_file = "../tokens/env2-token.json"    # Token file path

try:
    parser = argparse.ArgumentParser()
    parser.add_argument("start_date", type=str, help="Start date in YYYY-MM-DD format")
    parser.add_argument("end_date", type=str, help="End date in YYYY-MM-DD format")
    parser.add_argument("target", type=str, help="Target provider")
    parser.add_argument('-l', '--load', action='store_true', help='Load data to DataHub')
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    time_range = pd.date_range(start=start_date, end=end_date, freq='D')
    target = args.target

except Exception as e:
    print("Usage: python gen_csv_bill.py <start_date> <end_date> <target>")
    print("  - date format: YYYY-MM-DD")
    sys.exit(1)

if end_date < start_date:
    print("End date must be after start date")
    sys.exit(1)


#add function to generate billing data, use Databricks exemple

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

# loading data to DataHub in the targeted Dataset
def upload_to_datahub():

    api_key_file = open(token_file, "r")
    api_key = "Bearer " + api_key_file.read().strip()
    api_key_file.close()

    url = "https://api.doit.com/datahub/v1/csv/upload"
    files = { "file": (filename, open(filename, "rb"), "text/csv") }
    payload = { "provider": target }
    headers = {
        "accept": "application/json",
        "Authorization": api_key
    }

    response = requests.post(url, data=payload, files=files, headers=headers)
    return response

data = []

match target:
    case "Databricks":
        generate_databricks_billing()
    case _:
        print("Invalid target")
        sys.exit(1)

# Create DataFrame
df = pd.DataFrame(data)

# Save to CSV
df.to_csv(filename, index=False)

if args.load:
    response = upload_to_datahub()
    print(response.json())

