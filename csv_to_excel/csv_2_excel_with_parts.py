import pandas as pd
import os
import re
import math
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

def log_processed_file(csv_file):
    """
    Log only the processed CSV filename to a text file.
    
    Args:
        csv_file (str): Original CSV filename
    """
    with open("processed_files.txt", "a", encoding='utf-8') as log_file:
        log_file.write(f"{csv_file}\n")

def split_and_convert_to_excel(df, base_name, rows_per_part):
    """
    Split DataFrame into parts and convert each part to Excel.
    
    Args:
        df (DataFrame): Input DataFrame
        base_name (str): Base name for output files
        rows_per_part (int): Number of rows per output file
    
    Returns:
        list: List of created Excel file paths
    """
    total_rows = len(df)
    num_parts = math.ceil(total_rows / rows_per_part)
    created_files = []
    
    print(f"Total rows: {total_rows}")
    print(f"Splitting into {num_parts} parts with {rows_per_part} rows each")
    
    for i in range(num_parts):
        start_idx = i * rows_per_part
        end_idx = min((i + 1) * rows_per_part, total_rows)
        
        # Get the part
        df_part = df.iloc[start_idx:end_idx]
        
        # Create output filename
        excel_path = f"{base_name}_part_{i+1}_of_{num_parts}.xlsx"
        
        # Save to Excel
        with pd.ExcelWriter(excel_path) as writer:
            df_part.to_excel(writer, sheet_name=f'Part_{i+1}', index=False)
        
        created_files.append(excel_path)
        print(f"Created {excel_path} with rows {start_idx+1} to {end_idx}")
    
    return created_files

def get_split_choice():
    """
    Get user choice for splitting a file.
    
    Returns:
        tuple: (split_choice, rows_per_part)
    """
    while True:
        try:
            split_choice = input("Do you want to split this file into parts? (yes/no): ").lower()
            if split_choice in ['yes', 'no']:
                break
            print("Please enter 'yes' or 'no'")
        except Exception:
            print("Invalid input. Please try again.")
    
    rows_per_part = None
    if split_choice == 'yes':
        while True:
            try:
                rows_per_part = int(input("Enter number of rows per part: "))
                if rows_per_part > 0:
                    break
                print("Please enter a positive number.")
            except ValueError:
                print("Please enter a valid number.")
    
    return split_choice, rows_per_part

def csv_to_excel(csv_path, excel_path=None, rows_per_part=None):
    """
    Convert a CSV file to Excel format, optionally splitting into parts.
    
    Args:
        csv_path (str): Path to the input CSV file
        excel_path (str, optional): Path for the output Excel file
        rows_per_part (int, optional): Number of rows per output file
    
    Returns:
        str or list: Path(s) to the created Excel file(s)
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
        
        # If rows_per_part is specified, split the file
        if rows_per_part is not None:
            created_files = split_and_convert_to_excel(df, clean_filename(os.path.splitext(csv_path)[0]), rows_per_part)
            # Log the CSV file only once
            log_processed_file(csv_path)
            return created_files
        else:
            # Convert to Excel without splitting
            df.to_excel(excel_path, index=False, engine='openpyxl')
            # Log the CSV file
            log_processed_file(csv_path)
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
        
        # Get splitting choice for this file
        split_choice, rows_per_part = get_split_choice()
        
        # Process the file
        excel_files = csv_to_excel(csv_file, rows_per_part=rows_per_part)
        if excel_files:
            if isinstance(excel_files, list):
                print(f"Created {len(excel_files)} Excel files")
            else:
                print(f"Excel file created at: {excel_files}")
    
    print("\nProcessing complete. Check 'processed_files.txt' for the list of processed files.")

def main():
    process_all_csv_files()

if __name__ == "__main__":
    main()
