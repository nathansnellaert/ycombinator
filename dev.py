import os

# Set environment variables for this run
os.environ['CONNECTOR_NAME'] = 'ycombinator'
os.environ['RUN_ID'] = 'local-dev'
os.environ['ENABLE_HTTP_CACHE'] = 'true'
os.environ['CATALOG_TYPE'] = 'local'
os.environ['DATA_DIR'] = 'data'

# Test fetching just a small batch first
from assets.companies.companies import retrieve_batch
import pyarrow as pa

# Test with just one batch to check schema
test_batch = 'Summer 2024'
companies = retrieve_batch(test_batch)
print(f"Retrieved {len(companies)} companies from {test_batch}")

if companies:
    # Check if year_founded exists in the raw data
    sample = companies[0]
    print(f"\nSample company keys: {list(sample.keys())}")
    if 'year_founded' in sample:
        print(f"year_founded value: {sample['year_founded']}")
    else:
        print("year_founded key not found in raw data")

# Now test the full processing
from utils import validate_environment, upload_data
from assets.companies.companies import process_companies

validate_environment()
print("\nProcessing all companies...")
companies_data = process_companies()
print(f"\nSchema: {companies_data.schema}")
print(f"Number of rows: {len(companies_data)}")

# Check for any null type columns
for field in companies_data.schema:
    if str(field.type) == 'null':
        print(f"WARNING: Column '{field.name}' has null type")

upload_data(companies_data, "yc_companies")
print("\nSuccessfully uploaded yc_companies data")