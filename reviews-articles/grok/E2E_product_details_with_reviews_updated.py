import os
import ast
import pandas as pd
import time
import json
import requests
from openai import OpenAI
from openai import OpenAIError
from concurrent.futures import ThreadPoolExecutor, as_completed
# Your search_grok_api function stays exactly the same.
def search_grok_api(search_prompt: str, api_key: str, model: str = "grok-3", max_tokens: int = 3000, max_retries: int = 3, log_file: str = "grok_api_log.json") -> dict:
    """
    Send a search prompt to the Grok API and return the response, with follow-up for tool calls.
    
    Args:
        search_prompt (str): The search query to send to the API.
        api_key (str): Your xAI API key.
        model (str): The Grok model to use (default: grok-3).
        max_tokens (int): Maximum tokens for the response (default: 3000).
        max_retries (int): Maximum retry attempts for failed API calls (default: 3).
        log_file (str): File to log API responses for debugging (default: grok_api_log.json).
    
    Returns:
        dict: The API response containing search results or an error message.
    """
    retry_count = 0
    base_delay = 1  # 1 second base delay for retries
    current_prompt = search_prompt
    
    client = OpenAI(
        api_key=api_key,
        base_url="https://api.x.ai/v1"
    )

    while retry_count < max_retries:
        try:
            # Make the initial API call
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "user", "content": f"Search for {current_prompt}"}
                ],
                tools=[
                    {
                        "type": "function",
                        "function": {
                            "name": "web_search",
                            "description": "Perform a web search to retrieve real-time information",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "query": {"type": "string", "description": "The search query"}
                                },
                                "required": ["query"]
                            }
                        }
                    }
                ],
                tool_choice={"type": "function", "function": {"name": "web_search"}},
                max_tokens=max_tokens,
                temperature=0.7,
                timeout=90  # 90-second timeout
            )

            # Log the raw response
            with open(log_file, 'a', encoding='utf-8') as f:
                json.dump(response.to_dict(), f, indent=2)
                f.write('\n')

            # Extract content
            content = response.choices[0].message.content or ""
            tool_output = ""

            # Handle tool call
            if response.choices[0].message.tool_calls:
                for tool_call in response.choices[0].message.tool_calls:
                    if tool_call.function.name == "web_search":
                        try:
                            tool_args = json.loads(tool_call.function.arguments)
                            tool_output += f"\nWeb search executed for query: {tool_args.get('query', 'unknown')}"

                            # Follow-up API call to retrieve tool results
                            follow_up_response = client.chat.completions.create(
                                model=model,
                                messages=[
                                    {"role": "user", "content": f"Provide the results for the web search: {tool_args.get('query', 'unknown')}"},
                                    {
                                        "role": "assistant",
                                        "content": content,
                                        "tool_calls": response.choices[0].message.tool_calls
                                    }
                                ],
                                max_tokens=max_tokens,
                                temperature=0.7,
                                timeout=90
                            )

                            # Log follow-up response
                            with open(log_file, 'a', encoding='utf-8') as f:
                                json.dump(follow_up_response.to_dict(), f, indent=2)
                                f.write('\n')

                            # Update content with follow-up results
                            follow_up_content = follow_up_response.choices[0].message.content
                            if follow_up_content:
                                content = follow_up_content
                        except (json.JSONDecodeError, OpenAIError) as e:
                            tool_output += f"\nFollow-up failed or arguments could not be parsed: {str(e)}"

            # Check for incomplete or empty response
            if (not content or "Initiating a web search" in content or len(content.strip()) < 100) and not tool_output.lower().find("follow-up") != -1:
                retry_count += 1
                if retry_count < max_retries:
                    delay = base_delay * (2 ** retry_count)
                    print(f"Incomplete or empty response. Retrying in {delay} seconds...")
                    time.sleep(delay)
                    current_prompt = f"{search_prompt} (retry {retry_count})"
                    continue
                return {
                    "status": "error",
                    "content": f"Error: Incomplete response after {max_retries} retries. Raw tool output: {tool_output}"
                }

            # Combine content and tool output
            final_content = content + tool_output

            # Extract response
            result = {
                "status": "success",
                "content": final_content,
                "model": response.model,
                "created": response.created
            }
            return result

        except OpenAIError as e:
            retry_count += 1
            if retry_count < max_retries:
                delay = base_delay * (2 ** retry_count)
                print(f"API error: {str(e)}. RetryingNSW in {delay} seconds...")
                time.sleep(delay)
                current_prompt = f"{search_prompt} (retry {retry_count})"
            else:
                return {
                    "status": "error",
                    "content": f"Error after {max_retries} retries: {str(e)}"
                }
        except requests.exceptions.Timeout:
            retry_count += 1
            if retry_count < max_retries:
                delay = base_delay * (2 ** retry_count)
                print(f"Request timed out. Retrying in {delay} seconds...")
                time.sleep(delay)
                current_prompt = f"{search_prompt} (retry {retry_count})"
            else:
                return {
                    "status": "error",
                    "content": f"Error after {max_retries} retries: Request timed out"
                }
        except Exception as e:
            return {
                "status": "error",
                "content": f"Unexpected error: {str(e)}"
            }


# Map filenames to product types and prompt templates
file_product_type_map = {
    "tennis_sample_dataset": "tennis_racket",
    "badminton_sample_dataset": "badminton_racket",
    # Add more mappings here
}

file_prompt_map = {
    "tennis_racket": """
Please search for and summarize the articles and reviews you can find on
URL : {URL}
Name : {Name}
Brand : {Brand}

Give the results strictly in key value pairs
using information you can find (e.g., tennis-warehouse.com, tennisexpress.com, mytennishq.com, tennisnerd.net, thetennisbros.com, tenniscompanion.org, courtsidetennis.com, racquetguys.ca, tennisplaza.com, doittennis.com, perfect-tennis.com, tennisracket.me, tennisproguru.com, tenniscreative.com, tennislocation.com and other trusted sources) and attach their source page link as well.
""",
    "badminton_racket": """
Please search for and summarize the articles and reviews you can find on
URL : {URL}
Name : {Name}
Brand : {Brand}

Give the results strictly in key value pairs
using information you can find (e.g., badmintonplanet.com, badmintontalk.com, badmintoncentral.com, badmintonbay.com and other trusted sources) and attach their source page link as well.
"""
    # Add prompts for other product types here
}

required_columns = [
    "platform", "product_type", "title", "keywords_list", "asin", "link", "brand",
    "categories", "categories_flat", "description", "rating", "rating_breakdown",
    "ratings_total", "main_image", "images", "images_count", "feature_bullets",
    "top_reviews", "price", "specifications", "model_number",
    "parent_asin", "review"
]

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
    
def extract_all_specifications(spec_str):
    try:
        specs = ast.literal_eval(spec_str)
        result = {}
        for item in specs:
            key = item.get('name', '').strip()
            value = item.get('value', '').replace('\u200e', '').strip()
            result[key] = value
        return json.dumps(result, ensure_ascii=False)
    except Exception as e:
        return None

def load_file(file_path):
    if file_path.endswith(".xlsx") or file_path.endswith(".xls"):
        return pd.read_excel(file_path, engine='openpyxl')
    elif file_path.endswith(".csv"):
        return pd.read_csv(file_path, encoding='utf-8', engine='python')
    else:
        raise ValueError(f"Unsupported file format: {file_path}")

def process_file(file_path, api_key):
    file_name = os.path.splitext(os.path.basename(file_path))[0]
    product_type = file_product_type_map.get(file_name, "Unknown")

    df = load_file(file_path)

    # Add platform and product_type columns
    df["platform"] = "amazon"
    df["product_type"] = product_type

    # Extract price and clean reviews
    if "buybox_winner" in df.columns:
        df["price"] = df["buybox_winner"].apply(extract_price)
        df = df.drop(columns=["buybox_winner"])

    if "top_reviews" in df.columns:
        df["top_reviews"] = df["top_reviews"].apply(clean_top_reviews)

    if "specifications" in df.columns:
        df["specifications"] = df["specifications"].apply(extract_all_specifications)

    # Make sure required columns exist
    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    # Reorder columns
    df = df[required_columns]

    # Compose prompt template for this product_type
    prompt_template = file_prompt_map.get(product_type, "")

    # Log file for this product type
    log_file = f"grok_api_log_{product_type}.json"

    # Call Grok API for each row
    for index, row in df.iterrows():
        try:
            if not prompt_template:
                print(f"No prompt template found for product type: {product_type}")
                df.at[index, 'review'] = "No prompt template defined."
                continue

            # Format prompt with row values, fallback to empty string if missing
            prompt = prompt_template.format(
                URL=row.get('link', ''),
                Name=row.get('title', ''),
                Brand=row.get('brand', '')
            )

            result = search_grok_api(prompt, api_key, log_file=log_file)

            if result["status"] == "success":
                df.at[index, 'review'] = result["content"]
                print(f"Processed {product_type} row {index + 1}")
            else:
                df.at[index, 'review'] = f"Error: {result['content']}"
                print(f"Error in {product_type} row {index + 1}: {result['content']}")

        except Exception as e:
            print(f"Exception processing {product_type} row {index + 1}: {e}")
            df.at[index, 'review'] = f"Exception: {str(e)}"

    return df

def main(file_list, api_key):
    all_data = {}  # key = product_type, value = DataFrame
    combined_df = pd.DataFrame()

    # Create output directory for individual CSVs
    os.makedirs("individual_product_reviews", exist_ok=True)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_file, f, api_key): f for f in file_list}

        for future in as_completed(futures):
            file_path = futures[future]
            try:
                df_processed = future.result()
                product_type = df_processed['product_type'].iloc[0] if not df_processed.empty else 'unknown'
                
                # Save individual product reviews CSV immediately
                output_path = f"individual_product_reviews/{product_type}_reviews.csv"
                df_processed.to_csv(output_path, index=False)
                print(f" Saved {product_type} product reviews to {output_path}")

                # Store the dataframe for combining later
                all_data[product_type] = df_processed

            except Exception as e:
                print(f"Error processing file {file_path}: {e}")

    # Combine all individual DataFrames into one
    if all_data:
        combined_df = pd.concat(all_data.values(), ignore_index=True)



        combined_df.to_csv("combined_product_reviews.csv", index=False)
        print(" Saved combined product reviews to combined_product_reviews.csv")
    else:
        print("No data processed to combine.")


if __name__ == "__main__":
    # Example usage:
    # List all input files here (full or relative paths)
    input_files = [
        "tennis_sample_dataset.xlsx",
        "badminton_sample_dataset.xlsx"
        # Add more files here
    ]

    # Your API key
    api_key = ""

    main(input_files, api_key)
