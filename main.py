import os
os.environ['CONNECTOR_NAME'] = 'ycombinator'
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from utils import validate_environment, upload_data
from assets.companies.companies import process_companies

def main():
    validate_environment()
    
    companies_data = process_companies()
    upload_data(companies_data, "yc_companies")

if __name__ == "__main__":
    main()