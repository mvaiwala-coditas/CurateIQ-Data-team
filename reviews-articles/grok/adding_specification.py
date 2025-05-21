import pandas as pd
import ast
import json

# Load the CSV file
df = pd.read_csv("ESE_tennis_badminton_output_final - final.csv.csv")

# Function to extract all name-value pairs
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

# Overwrite the 'specifications' column
df['specifications'] = df['specifications'].apply(extract_all_specifications)

# Save updated file
df.to_csv(r"E2E_tennis_badminton_output_with_specification.csv", index=False)
