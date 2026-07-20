#!/usr/bin/env python3
"""Convert CSV to JSON Lines format."""

import pandas as pd
import json
import argparse

def csv_to_jsonl(csv_path: str, json_path: str):
    """
    Converts a CSV file to a JSON Lines file with additional data processing and cleaning.

    This function attempts to read a CSV file using multiple strategies to ensure resilience
    against format inconsistencies. Once successfully read, the CSV data is processed
    row-by-row to handle NaN values, clean problematic characters in string fields,
    and parse predefined JSON fields. The processed data is then written to the specified
    JSON Lines file.

    Parameters:
    csv_path: str
        The file path to the input CSV file.
    json_path: str
        The file path where the output JSON Lines file will be written.

    Raises:
    ValueError
        If all CSV reading strategies fail.
    """
    # Try multiple CSV reading strategies
    df = None
    strategies = [
        {'sep': ',', 'quotechar': '"', 'quoting': 1},  # QUOTE_ALL
        {'sep': ',', 'quotechar': '"', 'quoting': 0},  # QUOTE_MINIMAL  
        {'sep': ',', 'quotechar': '"', 'quoting': 3, 'skipinitialspace': True},  # QUOTE_NONE
        {'sep': ',', 'on_bad_lines': 'skip'},  # Skip bad lines
    ]
    
    for i, strategy in enumerate(strategies):
        try:
            print(f"Trying CSV read strategy {i+1}...")
            df = pd.read_csv(csv_path, **strategy)
            print(f"✅ Success with strategy {i+1}")
            break
        except Exception as e:
            print(f"❌ Strategy {i+1} failed: {e}")
            continue
    
    if df is None:
        raise ValueError("All CSV reading strategies failed")
    
    print(f"Successfully read CSV with {len(df)} rows and {len(df.columns)} columns")
    
    with open(json_path, 'w', encoding='utf-8') as f:
        for _, row in df.iterrows():
            # Convert pandas Series to dict, handling NaN values
            record = row.to_dict()
            # Replace NaN with None for proper JSON serialization
            record = {k: (None if pd.isna(v) else v) for k, v in record.items()}
            
            # Clean string values - remove problematic characters
            for k, v in record.items():
                if isinstance(v, str):
                    # Clean up common problematic characters
                    v = v.replace('\r\n', '\n').replace('\r', '\n')
                    # Ensure proper encoding
                    record[k] = v
            
            # Handle special JSON fields
            json_fields = ['Resource original JSON', 'Evidence']
            for field in json_fields:
                if field in record and record[field] and record[field] != 'NaN':
                    try:
                        # Try to parse as JSON
                        if isinstance(record[field], str):
                            record[field] = json.loads(record[field])
                    except (json.JSONDecodeError, TypeError):
                        # Keep as string if not valid JSON
                        pass
            
            # Use json.dumps with ensure_ascii=False for proper encoding
            f.write(json.dumps(record, ensure_ascii=False, separators=(',', ':')) + '\n')
    
    print(f"Converted {len(df)} records from CSV to JSON Lines")

def main():
    """
    Main function for converting a CSV file into a JSON Lines file.

    This function initializes an argument parser to handle the input and output
    file paths. It processes the command line arguments, and executes the
    conversion using the csv_to_jsonl function. If the conversion is successful,
    it displays a confirmation message; otherwise, it displays an error message
    and raises the encountered exception.

    Raises:
        Exception: Re-raises any exception encountered during the conversion
        process.
    """
    parser = argparse.ArgumentParser(description="Convert CSV to JSON Lines")
    parser.add_argument("csv_file", help="Input CSV file")
    parser.add_argument("json_file", help="Output JSON Lines file")
    
    args = parser.parse_args()
    
    try:
        csv_to_jsonl(args.csv_file, args.json_file)
        print(f"✅ Successfully converted {args.csv_file} to {args.json_file}")
    except Exception as e:
        print(f"❌ Conversion failed: {e}")
        raise

if __name__ == "__main__":
    main()