import pandas as pd
import random
from datetime import datetime, timedelta

# Generate synthetic Databricks billing data
start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 1, 15)
time_range = pd.date_range(start=start_date, end=end_date, freq='D')

data = []
sku_names = ["Standard_D3_v2", "Premium_D4_v2", "Jobs Compute DBU", "Interactive Compute DBU"]
sku_families = {
    "Standard_D3_v2": "Compute",
    "Premium_D4_v2": "Compute",
    "Jobs Compute DBU": "DBU",
    "Interactive Compute DBU": "DBU"
}
regions = ["eastus", "westus", "centralus"]

for date in time_range:
    for sku in sku_names:
        usage = round(random.uniform(0.5, 2.0), 2)
        unit_price = round(random.uniform(0.2, 1.0), 2)
        cost = round(usage * unit_price, 2)
        data.append({
            "fixed.project_name": "my-workspace",
            "fixed.sku_description": sku,
            "time": date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "fixed.pricing_unit": sku_families[sku],
            "metrics.usage": usage,
            "metrics.unit_price": unit_price,
            "metrics.cost": cost,
            "fixed.region": random.choice(regions),
            "provider": "Databricks"
        })

# Create DataFrame
df = pd.DataFrame(data)

# Save to CSV
file_path = "./csvfiles/databricks_billing_jan_2025.csv"
df.to_csv(file_path, index=False)
