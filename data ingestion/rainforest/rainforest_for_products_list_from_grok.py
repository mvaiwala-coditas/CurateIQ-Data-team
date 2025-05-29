import requests
import pandas as pd
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

api_key = "AC7514647EA44412B4FA5C799F780DEE"
search_terms = [
    "ASICS GT-2000 11 running shoes",
    "ASICS Gel-Cumulus 24 running shoes",
    "Adidas Adizero Adios Pro 3 running shoes",
    "Adidas Ultraboost Light running shoes",
    "Allbirds Tree Dashers running shoes",
    "Allbirds Wool Runners running shoes",
    "Altra Lone Peak 6 running shoes",
    "Anta Hydrogen Running Shoe running shoes",
    "Arc'teryx Norvan LD 3 running shoes",
    "Asics GT-2000 11 running shoes",
    "Bedrock Sandals Cairn Adventure Sandals running shoes",
    "Brooks Adrenaline GTS 23 running shoes",
    "Cariuma Catiba Pro running shoes",
    "Columbia Montrail F.K.T. running shoes",
    "Coros Pace Pro running shoes",
    "Craft CTM Ultra 2 running shoes",
    "Decathlon Kiprun KS Light running shoes",
    "Diadora Equipe Nucleo running shoes",
    "Dynafit Ultra 100 running shoes",
    "Earth Runners Elemental Sandals running shoes",
    "Ecoalf Eco Runner running shoes",
    "Everlane Court Sneaker running shoes",
    "Freet Flex 2 running shoes",
    "Helly Hansen Trailcutter running shoes",
    "Hoka Kawana running shoes",
    "Hoka Mach 5 running shoes",
    "Icebug Acceleritas8 RB9X running shoes",
    "Inov-8 Mudclaw G 260 running shoes",
    "Inov-8 Roclite G 275 running shoes",
    "Joe Nimble NimbleToes Addict running shoes",
    "Joma R.3000 running shoes",
    "Kalenji Run Active running shoes",
    "Karhu Ikoni 2.0 running shoes",
    "La Sportiva Akasha running shoes",
    "La Sportiva Akasha II running shoes",
    "Li-Ning Super Light 18 running shoes",
    "Lotto Ultraleggera running shoes",
    "Lowa Fortux running shoes",
    "Lululemon Blissfeel running shoes",
    "Luna Sandals Oso Flaco Winged Edition running shoes",
    "Mammut Sertig II Low running shoes",
    "Merrell Trail Glove 6 running shoes",
    "Mizics Airflow Pro running shoes",
    "Mizuno Wave Inspire 19 running shoes",
    "Montane Via Razor running shoes",
    "Native Shoes Jefferson Bloom running shoes",
    "New Balance Summit Unknown v2 running shoes",
    "Newton Running Gravity 12 running shoes",
    "Nike Air Max 270 running shoes",
    "Nike Air Zoom Pegasus 39 running shoes",
    "Nike React Infinity Run Flyknit 3 running shoes",
    "Norda 001 running shoes",
    "On Cloudflow running shoes",
    "Patagonia Everlong running shoes",
    "Peak Taichi Flash 3.0 running shoes",
    "Puma ForeverRun Nitro running shoes",
    "Puma Velocity Nitro 2 running shoes",
    "RaidLight Responsiv Ultra running shoes",
    "Reebok Floatride Energy 4 running shoes",
    "Rothy’s The Active Sneaker running shoes",
    "Salomon Aero Blaze running shoes",
    "Saola Cannon running shoes",
    "Saucony Canyon TR 2 running shoes",
    "Saucony Cohesion 16 running shoes",
    "Scarpa Spin Ultra running shoes",
    "Shamma Sandals Warriors running shoes",
    "Skechers GOrun Ride 10 running shoes",
    "The North Face Flight Vectiv running shoes",
    "Thousand Fell The Court Sneaker running shoes",
    "Topo Athletic MTN Racer 2 running shoes",
    "Topo Athletic Ultraventure Pro running shoes",
    "Under Armour Charged Assert 9 running shoes",
    "Unshoes Wokova Feather running shoes",
    "VIVOBAREFOOT Primus Trail II FG running shoes",
    "VJ Sport XTRM 2 running shoes",
    "Veja Campo running shoes",
    "Vibram V-Trail 2.0 running shoes",
    "Vivobarefoot Geo Racer II running shoes",
    "Xero Shoes HFS II running shoes",
    "Zoot Ultra TT 9.0 running shoes",
    "361 Degrees Fierce 2 running shoes"]
url = 'https://api.rainforestapi.com/request'
session = requests.Session()

# Setup single logger for all operations
logger = logging.getLogger('product_search_new2')
logger.setLevel(logging.INFO)
handler = logging.FileHandler('product_search_new2.log', encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
print("Log file created: product_search_new.log")

def fetch_product_data(asin, term):
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
            logger.info(f"Fetched product for ASIN: {asin} (Search term: {term})")
            product_data = data['product']
            product_data['search_term'] = term  # Add search term to product data
            return product_data
        else:
            logger.warning(f"No product info for ASIN: {asin} (Search term: {term})")
    except Exception as e:
        logger.error(f"Error fetching ASIN {asin} (Search term: {term}): {e}")
    return None

def fetch_product_details(term):
    logger.info(f"Starting processing for: {term}")
    
    params = {
        'api_key': api_key,
        'type': 'search',
        'amazon_domain': 'amazon.com',
        'number_of_results': 5,
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
                executor.submit(fetch_product_data, asin, term): asin for asin in asins
            }
            for future in as_completed(future_to_asin):
                result = future.result()
                if result:
                    all_products_data.append(result)

        return all_products_data

    except Exception as e:
        logger.error(f"Failed to fetch details for '{term}': {e}")
        return []

start = time.time()
print("Starting product fetch process")

all_products = []
with ThreadPoolExecutor(max_workers=2) as executor:
    futures = [executor.submit(fetch_product_details, term) for term in search_terms]
    for future in as_completed(futures):
        products = future.result()
        all_products.extend(products)

if all_products:
    df = pd.DataFrame(all_products)
    csv_file = 'product_list_details_running_shoes3.csv'
    df.to_csv(csv_file, index=False)
    logger.info(f"Total of {len(df)} products written to '{csv_file}'")
    print(f"CSV file written successfully: {csv_file}")
else:
    logger.warning("No products were fetched for any search term")

elapsed = time.time() - start
print(f"All search terms processed in {elapsed:.2f} seconds.")
