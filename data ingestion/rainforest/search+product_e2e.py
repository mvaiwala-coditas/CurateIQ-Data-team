import requests
import pandas as pd
import time

api_key = ""
search_terms = ["Pickleball Paddles", "Table Tennis Paddles"]

for term in search_terms:
    url = 'https://api.rainforestapi.com/request'

    params = {
        'api_key': api_key,
        'type': 'search',
        'amazon_domain': 'amazon.com',
        'max_page': 5,
        'search_term': term,
    }

    response = requests.get(url, params=params)
    data = response.json()
    products = data.get('search_results', [])
    asins = [product.get('asin') for product in products]

    all_products_data = []
    record_count = 0

    for asin in asins:
        product_params = {
            'api_key': api_key,
            'type': 'product',
            'output': 'json',
            'amazon_domain': 'amazon.com',
            'asin': asin,
        }   
        product_response = requests.get(url, params=product_params)
        product_data = product_response.json()

        if 'product' in product_data:
            all_products_data.append(product_data['product'])
            record_count += 1
            print(f"{record_count} record written")

        time.sleep(3)

    if all_products_data:
        df = pd.DataFrame(all_products_data)
        csv_file = f'{term}.csv'
        df.to_csv(csv_file, index=False)
        print(f"Data for '{term}' has been written to {csv_file}")
    else:
        print(f"No product details found for '{term}'.")

print("All search terms processed.")
