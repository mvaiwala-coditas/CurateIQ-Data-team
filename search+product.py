# sort_by: featured
import requests
import json
import pandas as pd 
import os

def get_amazon_data(product_name):
    api_key = os.getenv("API_KEY") 
    url = 'https://api.rainforestapi.com/request'

    params = {
        'api_key': api_key,
        'type': 'search',
        'amazon_domain': 'amazon.com',
        'number_of_results': 50,
        'search_term': product_name,
        'sort_by': 'featured'
    }
    response = requests.get(url, params=params)
    data = response.json()

    products = data.get('search_results')
    asins = [product.get('asin') for product in products]
    all_products_data = []

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

    
    if all_products_data:
        df = pd.DataFrame(all_products_data)
        
        csv_file = 'search+product_output.csv'
        df.to_csv(csv_file, index=False)
        
        print(f"Data has been written to {csv_file}")
    else:
        print("No product details found.")

get_amazon_data('Blender')
