import pandas as pd
import os
import re
from datetime import datetime

def clean_filename(filename):
    """
    Remove numbers from filename using regex and convert to lowercase.
    
    Args:
        filename (str): Original filename
    
    Returns:
        str: Cleaned filename without numbers, in lowercase
    """
    # Remove numbers but keep underscores
    cleaned_name = re.sub(r'\d+', '', filename)
    # Convert to lowercase
    cleaned_name = cleaned_name.lower()
    return cleaned_name

def log_processed_file(csv_file, excel_file):
    """
    Log only the processed filenames to a text file.
    
    Args:
        csv_file (str): Original CSV filename
        excel_file (str): Created Excel filename
    """
    with open("processed_files.txt", "a", encoding='utf-8') as log_file:
        log_file.write(f"{csv_file}\n")

def csv_to_excel(csv_path, excel_path=None):
    """
    Convert a CSV file to Excel format.
    
    Args:
        csv_path (str): Path to the input CSV file
        excel_path (str, optional): Path for the output Excel file. If not provided,
                                  will use the same name as CSV with .xlsx extension
    
    Returns:
        str: Path to the created Excel file
    """
    try:
        # Read the CSV file
        df = pd.read_csv(csv_path)
        
        # If excel_path is not provided, create one based on the CSV filename
        if excel_path is None:
            base_name = os.path.splitext(csv_path)[0]
            # Clean the filename by removing numbers and converting to lowercase
            clean_base_name = clean_filename(base_name)
            excel_path = f"{clean_base_name}.xlsx"
        
        # Convert to Excel
        df.to_excel(excel_path, index=False, engine='openpyxl')
        
        # Log the processed file
        log_processed_file(csv_path, excel_path)
        
        print(f"Successfully converted {csv_path} to {excel_path}")
        return excel_path
    
    except FileNotFoundError:
        print(f"Error: The file {csv_path} was not found.")
        return None
    except pd.errors.EmptyDataError:
        print(f"Error: The file {csv_path} is empty.")
        return None
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

def process_all_csv_files():
    """
    Process all CSV files in the current directory.
    """
    # Get all CSV files in the current directory
    csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
    
    if not csv_files:
        print("No CSV files found in the current directory.")
        return
    
    print(f"Found {len(csv_files)} CSV files to process.")
    
    # Create or clear the log file at the start of processing
    with open("processed_files.txt", "w", encoding='utf-8') as log_file:
        pass  # Just create/clear the file
    
    # Process each CSV file
    for csv_file in csv_files:
        print(f"\nProcessing {csv_file}...")
        excel_file = csv_to_excel(csv_file)
        if excel_file:
            print(f"Excel file created at: {excel_file}")
    
    print("\nProcessing complete. Check 'processed_files.txt' for the list of processed files.")

def main():
    process_all_csv_files()

if __name__ == "__main__":
    main()
