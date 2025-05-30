import requests
import json
import os
import csv
import time
from datetime import datetime

# API Configuration
BASE_URL = "https://api.x.ai/v1"
API_KEY = ""

def get_grok_response(prompt):
    """
    Get response from Grok API with web search enabled
    """
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }

    data = {
        "model": "grok-3",
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "web_search": True
    }

    try:
        response = requests.post(
            f"{BASE_URL}/chat/completions",
            headers=headers,
            json=data
        )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]
    except requests.exceptions.RequestException as e:
        return f"Error occurred: {str(e)}"

def remove_duplicates(data_rows, headers):
    """
    Remove duplicate entries based on Brand and Model column (smart matching)
    """
    # Find index of the header that best matches "Brand and Model"
    brand_idx = next(
        (i for i, h in enumerate(headers) if 'brand' in h.lower() and 'model' in h.lower()), None
    )
    if brand_idx is None:
        raise ValueError(f"Could not find a column with 'Brand and Model' in headers: {headers}")

    seen = set()
    unique_rows = []

    for row in data_rows:
        key = row[brand_idx]
        if key not in seen:
            seen.add(key)
            unique_rows.append(row)

    return unique_rows

def save_to_csv(response_text):
    """
    Convert Grok response table to CSV and save
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"pickleball_paddles_{timestamp}.csv"

    # Split response into lines
    lines = response_text.strip().split('\n')

    # Find table start
    table_start = 0
    for i, line in enumerate(lines):
        if '|' in line and '-' in lines[i+1] if i+1 < len(lines) else False:
            table_start = i
            break

    # Extract headers
    headers = [h.strip() for h in lines[table_start].split('|') if h.strip()]
    data_rows = []

    for line in lines[table_start+2:]:  # Skip header and separator line
        if '|' in line:
            row = [cell.strip() for cell in line.split('|') if cell.strip()]
            if len(row) == len(headers):
                data_rows.append(row)

    # Remove duplicates
    unique_data_rows = remove_duplicates(data_rows, headers)

    # Write to CSV
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(unique_data_rows)

    return filename
  

def get_batch_of_rackets(start_num, end_num):
    prompt = f""" Generate a detailed comparison table of exactly {end_num - start_num + 1} unique pickleball paddles (paddles #{start_num} to #{end_num}).

Follow this structured two-phase approach:

---

 **Phase 1: Identify Products and Specifications**

Select currently available pickleball paddles from verified US retailers. For each paddle, fetch the following **key attributes**:

1. Brand and Model Name  
2. Price (USD)  
3.Weight  
4. Material  
5. Paddle Shape  
6. Grip Comfort   

 **Phase 2: Fetch Reviews from Trusted Sources**

After gathering paddle specs, search for real-world reviews from the following **trusted sources only**:

- pickleheads.com  
- pickleballeffect.com  
- pickleballstudio.com  
- wired.com  
- nytimes.com  
- forbes.com  
- pickleballwarehouse.com  
- pickleballsurge.com  
- mattspickleball.com  
- pickleballportal.com  
- thepickler.com  
- usapickleball.org  

Write a **unique review (minimum 100 words)** for each paddle using actual insights from customers and expert reviewers. Discuss:
- Overall performance  
- Feel (control, power, spin)  
- Durability and comfort  
- Key pros and any noted cons  
- Unique features or technologies

---

 **Output Format**

Present the results in a clean **Markdown table** with `|` separators. Each row should include:

1. Brand and Model  
2. Price (USD)  
3. Paddle Weight  
4. Construction Material  
5. Paddle Shape  
6. Grip Comfort  
7. Expert & Customer Review (100+ words)

---

 Requirements:
- Include EXACTLY {end_num - start_num + 1} paddles  
- Each paddle must be unique  
- All fields must be fully completed  
- Do not reuse models  
- Reviews must reflect **authentic expert and customer opinions**

"""
    return get_grok_response(prompt)

def combine_table_data(responses):
    """
    Combine multiple Grok responses into one large Markdown table
    """
    all_data = []
    headers = None

    for response in responses:
        lines = response.strip().split('\n')

        # Find table start
        table_start = 0
        for i, line in enumerate(lines):
            if '|' in line and '-' in lines[i+1] if i+1 < len(lines) else False:
                table_start = i
                break

        if headers is None:
            headers = [h.strip() for h in lines[table_start].split('|') if h.strip()]

        for line in lines[table_start+2:]:
            if '|' in line:
                row = [cell.strip() for cell in line.split('|') if cell.strip()]
                if len(row) == len(headers):
                    all_data.append(row)

    # Combine to Markdown table format string
    result = '|' + '|'.join(headers) + '|\n'
    result += '|' + '|'.join(['-' * len(h) for h in headers]) + '|\n'
    for row in all_data:
        result += '|' + '|'.join(row) + '|\n'

    return result

def main():
    print("Processing your request...")

    batch_size = 10
    responses = []

    for i in range(0, 50, batch_size):
        start = i + 1
        end = min(i + batch_size, 50)
        print(f"\nGenerating paddles {start} to {end}...")
        response = get_batch_of_rackets(start, end)
        responses.append(response)
        if end < 50:
            print("Waiting 5 seconds before next batch...\n")
            time.sleep(5)

    combined_response = combine_table_data(responses)
    csv_filename = save_to_csv(combined_response)

    print(f"\n Data has been saved to {csv_filename}")
    print("Program completed successfully!")

if __name__ == "__main__":
    main()
