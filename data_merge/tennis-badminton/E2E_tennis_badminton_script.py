import pandas as pd
import os
import ast

# Step 1: Define required columns
required_columns = [
    "platform", "product_type", "title", "keywords_list", "asin", "link", "brand",
    "categories", "categories_flat", "description", "rating", "rating_breakdown",
    "ratings_total", "main_image", "images", "images_count", "feature_bullets",
    "top_reviews", "price", "specifications", "model_number",
    "parent_asin", "review"
]

# Step 2: Map filenames to product types
file_product_type_map = {
    "E2E_tennis_final": "tennis_racket",
    "E2E_badminton_final": "badminton_racket"
}

# Step 3: Safely load CSV or Excel files
def load_file(file_path):
    if file_path.endswith(".xlsx") or file_path.endswith(".xls"):
        return pd.read_excel(file_path, engine='openpyxl')
    elif file_path.endswith(".csv"):
        return pd.read_csv(file_path, encoding='utf-8', engine='python')
    else:
        raise ValueError(f"Unsupported file format: {file_path}")

# Step 4a: Extract price from nested buybox_winner column
def extract_price(buybox):
    try:
        if isinstance(buybox, str):
            buybox = ast.literal_eval(buybox)
        return buybox.get('price', {}).get('value')
    except Exception:
        return None


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




# Step 5: Process individual file
def process_file(file_path):
    file_name = os.path.splitext(os.path.basename(file_path))[0]
    df = load_file(file_path)

    # Add platform and product_type
    df["platform"] = "amazon"
    df["product_type"] = file_product_type_map.get(file_name, "Unknown")

    # Extract price from buybox_winner
    df["price"] = df["buybox_winner"].apply(extract_price)

    # Clean top_reviews by removing 'body_html'
    if "top_reviews" in df.columns:
        df["top_reviews"] = df["top_reviews"].apply(clean_top_reviews)

    # Drop buybox_winner column
    if "buybox_winner" in df.columns:
        df = df.drop(columns=["buybox_winner"])

    # Ensure all required columns exist
    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    # Select and reorder columns
    df = df[required_columns]

    return df

# Step 6: Merge all files and save
def merge_files(file_paths, output_path="final.csv"):
    merged_dfs = [process_file(path) for path in file_paths]
    merged_df = pd.concat(merged_dfs, ignore_index=True)
    merged_df.to_csv(output_path, index=False)
    print(f"Merged file saved at: {output_path}")

# === USAGE ===
file_paths = [
    "E2E_tennis_final.xlsx",
    "E2E_badminton_final.xlsx"
]

merge_files(file_paths)
