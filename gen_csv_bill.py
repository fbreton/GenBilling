import pandas as pd
import random
import sys
from datetime import datetime
import requests
import argparse
import string

# Constants to modify regarding your environment
filename = "./csvfiles/billing.csv"         # CSV file path 
token_file = "../tokens/env1-token.json"    # Token file path

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

regions = ["us-east-1", "us-west-2", "us-central1","eu-west-1","eu-central-1","ap-southeast-1","ap-northeast-1"]
tag_values = {
    "project":["AstroFrontier", "CelestialNavigator", "CosmicDiscovery", "GalaticExplorer", "GalaticOdyssey", "InfinityVoyage", "InterstellarMission", "NebulaPioneer", "OrionExpedition", "StarlightQuest"],
    "env":["demo", "development", "playground", "production", "qa", "sandbox", "staging"],
    "app":["AstroCloud", "ByteGalaxy", "CloudCosmos", "CloudSwiftX", "CyberOrbit", "NebulaCraft", "NebulaInsight", "NexusStellar", "QuantumNimbus", "SpackCelestial"]
    }

#add function to generate billing data, use Databricks exemple

# Generate synthetic OCI billing data
def generate_oci_billing():
    account_id = ["SUBSCRIPTION104", "SUBSCRIPTION207", "SUBSCRIPTION321","SUBSCRIPTION410"]
    services = ["vm", "database", "storage", "networking", "ai"]
    pricing_unit = {
        "vm": "Hours", 
        "database": "OCPU Hours", 
        "storage": "GB", 
        "networking": "GB", 
        "ai": "Calls"
    }
    # generate resource_id
    resource_id = {
        "vm": [],
        "database": [],
        "storage": [],
        "networking": [],
        "ai": []
    }
    for service in services:
        for i in range(1, 12):
            id = "".join(random.choices(string.ascii_uppercase + string.digits, k=32))
            id = "ocid1." + service + ".oc1." + id
            resource_id[service].append(id)

    unitprice = {
        "vm": 0.15, 
        "database": 0.35, 
        "storage": 0.22, 
        "networking": 0.18, 
        "ai": 0.08
    }

    for date in time_range:
        for account in account_id:
            for service in services:
                for i in range(1, 4):
                    usage = round(random.triangular(5.0, 24.0, 15.0), 2)
                    cost = round(usage * unitprice[service], 2)
                    data.append({
                        "usage_date": date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "fixed.billing_account_id": account,
                        "fixed.service_description": service,
                        "fixed.service_id": service,
                        "fixed.region": random.choice(regions),
                        "metric.usage": usage,
                        "metric.unit_price": unitprice[service],
                        "fixed.pricing_unit": pricing_unit[service],
                    "metric.cost": cost,
                    "label.currency": "USD",
                    "label.project": random.choice(tag_values["project"]),
                    "label.env": random.choice(tag_values["env"]),
                    "label.app": random.choice(tag_values["app"]),
                    "fixed.resource_id": random.choice(resource_id[service])
                })

# Generate synthetic Databricks billing data
def generate_databricks_billing():
    services = ["JOBS", "DLT", "SQL", "MODEL_SERVING", "INTERACTIVE", "DEFAULT_STORAGE", "VECTOR_SEARCH", "LAKEHOUSE_MONITORING", "PREDICTIVE_OPTIMIZATION", "ONLINE_TABLES", "FOUNDATION_MODEL_TRAINING", "AGENT_EVALUATION", "FINE_GRAIN_ACCESS_CONTROL", "NETWORKING", "APPS"]
    pricing_unit = {
        "JOBS": "DBU",
        "DLT": "DBU",
        "SQL": "DBU",
        "MODEL_SERVING": "DBU",
        "INTERACTIVE": "DBU",
        "DEFAULT_STORAGE": "DSU",
        "VECTOR_SEARCH": "DBU",
        "LAKEHOUSE_MONITORING": "DBU",
        "PREDICTIVE_OPTIMIZATION": "DBU",
        "ONLINE_TABLES": "DBU",
        "FOUNDATION_MODEL_TRAINING": "TOKENS",
        "AGENT_EVALUATION": "DBU",
        "FINE_GRAIN_ACCESS_CONTROL": "DBU",
        "NETWORKING": "DSU",
        "APPS": "DBU"
    }
    unitprice = {
       "JOBS": 0.15,
       "DLT": 0.35,
       "SQL": 0.22,
       "MODEL_SERVING": 0.18,
       "INTERACTIVE": 0.55,
       "DEFAULT_STORAGE": 0.023,
       "VECTOR_SEARCH": 0.15,
       "LAKEHOUSE_MONITORING": 0.12,
       "PREDICTIVE_OPTIMIZATION": 0.18,
       "ONLINE_TABLES": 0.15,
       "FOUNDATION_MODEL_TRAINING": 0.08,
       "AGENT_EVALUATION": 0.12,
       "FINE_GRAIN_ACCESS_CONTROL": 0.15,
       "NETWORKING": 0.05,
       "APPS": 0.12
    }
    account_id = ["bd59efba-4444-4444-443f-444444494203", "bd59efba-4444-4444-393f-444444448104","bd59efba-4444-4444-49ac-444444446325"]
    account_workspace = {
        "bd59efba-4444-4444-443f-444444494203": ["Marketing", "Sales"],
        "bd59efba-4444-4444-393f-444444448104": ["Analytics", "Data"],
        "bd59efba-4444-4444-49ac-444444446325": ["Rocket","AI"]
    }

    for date in time_range:
        for account in account_id:
            for workspace in account_workspace[account]:
                for service in services:
                    usage = round(random.uniform(0.5, 10.0), 2)
                    cost = round(usage * unitprice[service], 2)
                    data.append({
                        "usage_date": date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "fixed.billing_account_id": account,
                        "fixed.service_description": service,
                        "fixed.service_id": service,
                        "fixed.region": random.choice(regions),
                        "metric.usage": usage,
                        "metric.unit_price": unitprice[service],
                        "fixed.pricing_unit": pricing_unit[service],
                        "metric.cost": cost,
                        "label.workspace": workspace,
                        "label.currency": "USD",
                        "label.project": random.choice(tag_values["project"]),
                        "label.env": random.choice(tag_values["env"]),
                        "label.app": random.choice(tag_values["app"])
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
    case "OCI":
        generate_oci_billing()
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

