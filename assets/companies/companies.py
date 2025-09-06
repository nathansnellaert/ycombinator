import pyarrow as pa
import json
from datetime import datetime
from utils.http_client import post
from utils import load_state, save_state

ALGOLIA_CONFIG = {
    "url": "https://45bwzj1sgc-dsn.algolia.net/1/indexes/*/queries",
    "app_id": "45BWZJ1SGC",
    "api_key": "Zjk5ZmFjMzg2NmQxNTA0NGM5OGNiNWY4MzQ0NDUyNTg0MDZjMzdmMWY1NTU2YzZkZGVmYjg1ZGZjMGJlYjhkN3Jlc3RyaWN0SW5kaWNlcz1ZQ0NvbXBhbnlfcHJvZHVjdGlvbiZ0YWdGaWx0ZXJzPSU1QiUyMnljZGNfcHVibGljJTIyJTVEJmFuYWx5dGljc1RhZ3M9JTVCJTIyeWNkYyUyMiU1RA%3D%3D",
    "index_name": "YCCompany_production"
}

BATCH_VALUES = [
    'Fall 2024', 'Spring 2025', 
    'Summer 2005', 'Summer 2006', 'Summer 2007', 'Summer 2008', 'Summer 2009', 
    'Summer 2010', 'Summer 2011', 'Summer 2012', 'Summer 2013', 'Summer 2014', 
    'Summer 2015', 'Summer 2016', 'Summer 2017', 'Summer 2018', 'Summer 2019', 
    'Summer 2020', 'Summer 2021', 'Summer 2022', 'Summer 2023', 'Summer 2024',
    'Winter 2007', 'Winter 2008', 'Winter 2009', 'Winter 2010', 'Winter 2011', 
    'Winter 2012', 'Winter 2013', 'Winter 2014', 'Winter 2015', 'Winter 2016', 
    'Winter 2017', 'Winter 2018', 'Winter 2019', 'Winter 2020', 'Winter 2021', 
    'Winter 2022', 'Winter 2023', 'Winter 2024', 'Winter 2025',
    'Unspecified', 'IK12'
]

def retrieve_batch(batch):
    body = {"requests": [{"indexName": ALGOLIA_CONFIG["index_name"], "params": f'filters=batch:"{batch}"&hitsPerPage=1000'}]}
    headers = {"accept": "application/json", "content-type": "application/x-www-form-urlencoded"}
    url = f'{ALGOLIA_CONFIG["url"]}?x-algolia-agent=Algolia%20for%20JavaScript%20(3.35.1)%3B%20Browser%3B%20JS%20Helper%20(3.11.3)&x-algolia-application-id={ALGOLIA_CONFIG["app_id"]}&x-algolia-api-key={ALGOLIA_CONFIG["api_key"]}'
    
    response = post(url, headers=headers, data=json.dumps(body))
    if response.status_code == 200:
        return response.json()['results'][0]['hits']
    return []

def process_companies() -> pa.Table:
    state = load_state("companies")
    
    all_companies = []
    for batch in sorted(BATCH_VALUES):
        companies = retrieve_batch(batch)
        print(f'Retrieved {len(companies)} companies for batch "{batch}"')
        all_companies.extend(companies)
    
    processed_companies = []
    for company in all_companies:
        if company.get('name'):
            processed_companies.append({
                'id': company.get('id'),
                'name': company.get('name'),
                'slug': company.get('slug', ''),
                'batch': company.get('batch', ''),
                'description': company.get('description', ''),
                'long_description': company.get('long_description', ''),
                'website': company.get('website', ''),
                'status': company.get('status', ''),
                'tags': json.dumps(company.get('tags', [])) if company.get('tags') else None,
                'location': company.get('location', ''),
                'country': company.get('country', ''),
                'team_size': company.get('team_size') if company.get('team_size') else None,
                'linkedin_url': company.get('linkedin_url', ''),
                'twitter_url': company.get('twitter_url', ''),
                'facebook_url': company.get('facebook_url', ''),
                'cb_url': company.get('cb_url', ''),
                'logo_url': company.get('logo_url', ''),
                'is_hiring': company.get('is_hiring', False),
                'nonprofit': company.get('nonprofit', False),
                'highlight_black': company.get('highlight_black', False),
                'highlight_latinx': company.get('highlight_latinx', False),
                'highlight_women': company.get('highlight_women', False),
            })
    
    if not processed_companies:
        raise ValueError("No YC companies data found")
    
    table = pa.Table.from_pylist(processed_companies)
    save_state("companies", {"last_update": datetime.now().isoformat()})
    
    return table