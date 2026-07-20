#!/usr/bin/env python3
"""Binary-level CSV line normalizer to fix corrupted line endings."""

import argparse
from pathlib import Path

def normalize_csv_lines(input_path: str, output_path: str, expected_fields: int = 39):
    """
    Normalizes lines in a CSV file to ensure proper field structure and formatting.

    This function reads a CSV file in binary mode, processes its lines to ensure
    each record has the expected number of fields, and writes normalized lines
    to an output file. Lines with incomplete records are adjusted to maintain
    proper field continuity by replacing line breaks with spaces within fields.

    Attributes:
        None

    Args:
        input_path: str
            Path to the input CSV file to be normalized.
        output_path: str
            Path to the output CSV file where normalized content will be saved.
        expected_fields: int, optional
            The number of fields expected in each record. Defaults to 39.

    Raises:
        None

    Returns:
        None
    """
    
    with open(input_path, 'rb') as infile, open(output_path, 'wb') as outfile:
        buffer = b''
        field_count = 0
        in_quotes = False
        line_count = 0
        
        while True:
            chunk = infile.read(8192)
            if not chunk:
                break
                
            buffer += chunk
            
            i = 0
            while i < len(buffer):
                byte = buffer[i:i+1]
                
                if byte == b'"':
                    in_quotes = not in_quotes
                elif byte == b',' and not in_quotes:
                    field_count += 1
                elif byte in (b'\n', b'\r') and not in_quotes:
                    # Check if we have the right number of fields
                    if field_count == expected_fields - 1:  # -1 because last field doesn't have comma
                        # Proper record end - write LF
                        outfile.write(b'\n')
                        field_count = 0
                        line_count += 1
                        if line_count % 10000 == 0:
                            print(f"Processed {line_count} records")
                    else:
                        # Incomplete record - replace with space to continue field
                        outfile.write(b' ')
                    
                    # Skip any additional CR/LF bytes
                    while i + 1 < len(buffer) and buffer[i+1:i+2] in (b'\n', b'\r'):
                        i += 1
                else:
                    # Regular byte - pass through
                    outfile.write(byte)
                
                i += 1
            
            # Keep last incomplete line in buffer
            buffer = b''
    
    print(f"Normalized {line_count} records")

def main():
    """
    Parses command-line arguments to normalize CSV line endings at the binary level
    and invokes the function for processing the CSV file.

    Summary:
    This script provides functionality to take an input corrupted CSV file and an output file location,
    normalize the corrupted line endings by considering the provided number of fields,
    and save the normalized version into the output file.

    Args:
        input_csv (str): Path to the input corrupted CSV file.
        output_csv (str): Path where the output normalized CSV file should be saved.
        fields (int, optional): The expected number of fields per record. Defaults to 39.
    """
    parser = argparse.ArgumentParser(description="Normalize CSV line endings at binary level")
    parser.add_argument("input_csv", help="Input corrupted CSV file")
    parser.add_argument("output_csv", help="Output normalized CSV file")
    parser.add_argument("--fields", type=int, default=39, help="Expected number of fields per record")
    
    args = parser.parse_args()
    
    normalize_csv_lines(args.input_csv, args.output_csv, args.fields)
    print(f"Normalized CSV saved to: {args.output_csv}")

if __name__ == "__main__":
    main()