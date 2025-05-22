import requests
import pandas as pd
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

api_key = "AC7514647EA44412B4FA5C799F780DEE"
search_terms = ["Pickleball Paddles"]
url = 'https://api.rainforestapi.com/request'
session = requests.Session()

def setup_logger(term):
    log_name = term.replace(" ", "_")
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.FileHandler(f"{log_name}.log", encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        print(f"Log file created: {log_name}.log")  

    return logger

def fetch_product_data(asin, term, logger):
    product_params = {
        'api_key': api_key,
        'type': 'product',
        'output': 'json',
        'amazon_domain': 'amazon.com',
        'asin': asin,
    }
    try:
        response = session.get(url, params=product_params)
        data = response.json()
        if 'product' in data:
            logger.info(f"Fetched product for ASIN: {asin}")
            return data['product']
        else:
            logger.warning(f"No product info for ASIN: {asin}")
    except Exception as e:
        logger.error(f"Error fetching ASIN {asin}: {e}")
    return None

def fetch_product_details(term):
    logger = setup_logger(term)
    logger.info(f"Starting processing for: {term}")

    params = {
        'api_key': api_key,
        'type': 'search',
        'amazon_domain': 'amazon.com',
        'max_page': 5,
        'search_term': term
    }

    try:
        response = session.get(url, params=params)
        data = response.json()
        products = data.get('search_results', [])
        asins = [p.get('asin') for p in products if p.get('asin')]

        logger.info(f"Found {len(asins)} ASINs for term '{term}'")

        all_products_data = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_asin = {
                executor.submit(fetch_product_data, asin, term, logger): asin for asin in asins
            }
            for future in as_completed(future_to_asin):
                result = future.result()
                if result:
                    all_products_data.append(result)

        if all_products_data:
            df = pd.DataFrame(all_products_data)

            
            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                    df[col] = df[col].astype(str)

            df = df.drop_duplicates()

            csv_file = f'{term.replace(" ", "_")}_product_output.csv'
            df.to_csv(csv_file, index=False)
            logger.info(f"{len(df)} products written to '{csv_file}'")
            print(f"CSV file written successfully: {csv_file}")
        else:
            logger.warning(f"No products fetched for '{term}'")

    except Exception as e:
        logger.error(f"Failed to fetch details for '{term}': {e}")

start = time.time()
print("Starting product fetch process")

with ThreadPoolExecutor(max_workers=2) as executor:
    futures = [executor.submit(fetch_product_details, term) for term in search_terms]
    for future in as_completed(futures):
        future.result()

elapsed = time.time() - start
print(f"All search terms processed in {elapsed:.2f} seconds.")
