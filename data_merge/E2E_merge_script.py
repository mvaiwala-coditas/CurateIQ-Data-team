import pandas as pd
import os
import ast
import json

# Required columns
required_columns = [
    "platform", "product_type", "title", "keywords_list", "asin", "link", "brand",
    "categories", "categories_flat", "description", "rating", "rating_breakdown",
    "ratings_total", "main_image", "images", "images_count", "feature_bullets",
    "top_reviews", "price", "specifications", "model_number",
    "parent_asin", "review"
]

# Map filenames to product types
file_product_type_map = {
    "pickkle_ball_part_2": "pickleball_balls",
    "pickkleball_nets_portable": "pickleball_nets",
    "tennis_ball_machines": "tennis_ball_machines",
    "tennis_shoes_part_2": "tennis_shoes"
}

# Load files safely
def load_file(file_path):
    if file_path.endswith(".xlsx") or file_path.endswith(".xls"):
        return pd.read_excel(file_path, engine='openpyxl')
    elif file_path.endswith(".csv"):
        return pd.read_csv(file_path, encoding='utf-8', engine='python')
    else:
        raise ValueError(f"Unsupported file format: {file_path}")

# Extract price from buybox_winner nested structure
def extract_price(buybox):
    try:
        if isinstance(buybox, str):
            buybox = ast.literal_eval(buybox)
        return buybox.get('price', {}).get('value')
    except Exception:
        return None

# Clean top_reviews by removing 'body_html'
def clean_top_reviews(reviews):
    try:
        if isinstance(reviews, str):
            reviews = ast.literal_eval(reviews)
        if isinstance(reviews, list):
            for r in reviews:
                r.pop("body_html", None)
        return reviews
    except Exception:
        return None

# Extract and convert specifications to JSON string with name-value pairs
def extract_all_specifications(spec_str):
    try:
        specs = ast.literal_eval(spec_str)
        result = {}
        for item in specs:
            key = item.get('name', '').strip()
            value = item.get('value', '').replace('\u200e', '').strip()
            result[key] = value
        return json.dumps(result, ensure_ascii=False)
    except Exception:
        return None

# Process each file with the new specs extraction added
def process_file(file_path):
    file_name = os.path.splitext(os.path.basename(file_path))[0]
    df = load_file(file_path)

    # Add platform and product_type columns
    df["platform"] = "amazon"
    df["product_type"] = file_product_type_map.get(file_name, "Unknown")

    # # Extract price from buybox_winner
    df["price"] = df["buybox_winner"].apply(extract_price) if "buybox_winner" in df.columns else None

    # # Clean top_reviews by removing 'body_html'
    if "top_reviews" in df.columns:
        df["top_reviews"] = df["top_reviews"].apply(clean_top_reviews)

    # # Extract specifications
    if "specifications" in df.columns:
        df["specifications"] = df["specifications"].apply(extract_all_specifications)
    else:
        df["specifications"] = None

    # # Drop buybox_winner column if exists
    if "buybox_winner" in df.columns:
        df = df.drop(columns=["buybox_winner"])

    # Ensure all required columns exist, fill missing with None
    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    # Select and reorder columns
    df = df[required_columns]

    return df

# Merge multiple processed files and save the final CSV
def merge_files(file_paths, output_path="final.csv"):
    merged_dfs = [process_file(path) for path in file_paths]
    merged_df = pd.concat(merged_dfs, ignore_index=True)
    merged_df.to_csv(output_path, index=False)
    print(f"Merged file saved at: {output_path}")

# === USAGE EXAMPLE ===
file_paths = [
    "pickkle_ball_part_2",
    "tennis_shoes_part_2",
    "tennis_ball_machines",
    "pickkleball_nets_portable"
]

merge_files(file_paths)
