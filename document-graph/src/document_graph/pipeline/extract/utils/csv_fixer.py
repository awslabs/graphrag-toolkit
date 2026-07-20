#!/usr/bin/env python3
"""CSV Fixer utility using document-graph schema for validation and repair."""

import json
import csv
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class CSVFixer:
    """
    Class to handle fixing malformed CSV files.

    This class supports fixing and processing CSV files based on a predefined schema. It uses a two-stage
    process to handle the input, ensuring proper formatting of the records. The process identifies and corrects
    issues such as incomplete records caused by multiline fields and provides logging for errors and corrections.

    Attributes
    ----------
    schema : dict
        Dictionary extracting column metadata and optional fields from the given schema file.
    expected_columns : list[str]
        List of expected column names as per the schema.
    expected_count : int
        Total count of expected columns based on the schema.
    optional_fields : set[str]
        Set of column names identified as optional.
    """
    
    def __init__(self, schema_path: str):
        """Initialize with schema file."""
        with open(schema_path, 'r') as f:
            self.schema = json.load(f)
        
        self.expected_columns = list(self.schema['columns'].keys())
        self.expected_count = len(self.expected_columns)
        self.optional_fields = set(self.schema.get('optional_fields', []))
        
        logger.info(f"Schema loaded: {self.expected_count} columns expected")
    
    def _stage_lines(self, input_path: str, staging_path: Path):
        """
        Stages the lines from an input file by copying them to a staging file.

        This method reads the contents of the specified input file line by line and
        writes them to a designated staging file. Errors encountered during input file
        reading are replaced to ensure the process completes without interruption.

        Parameters:
            input_path (str): The path to the input file to be staged.
            staging_path (Path): The path to the output staging file.

        Returns:
            None
        """
        with open(input_path, 'r', encoding='utf-8', errors='replace') as infile:
            with open(staging_path, 'w', encoding='utf-8') as stagefile:
                for line in infile:
                    stagefile.write(line)
    
    def fix_csv(self, input_path: str, output_path: str, badlog_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Fixes and processes a CSV file, identifying and resolving errors or inconsistencies such as incomplete records,
        excess fields, or multiline fields. The method can optionally log bad lines to a specified file.

        Parameters:
        input_path: str
            Path to the input CSV file.
        output_path: str
            Path to the corrected output CSV file.
        badlog_path: Optional[str]
            Path to the log file where bad/incomplete lines will be recorded, if provided.

        Returns:
        Dict[str, Any]
            A dictionary containing processing statistics:
                - 'total_lines': Total lines read from the input file.
                - 'valid_records': Number of valid records written to the output file without modification.
                - 'fixed_records': Number of records corrected and written.
                - 'skipped_lines': Number of invalid or irrecoverable lines that were skipped.
                - 'errors': List of error messages encountered during processing.
        """
        # Stage 1: Read all lines into staging
        staging_path = Path(output_path).with_suffix('.staging')
        self._stage_lines(input_path, staging_path)
        
        # Stage 2: Process staged lines
        stats = {
            'total_lines': 0,
            'valid_records': 0,
            'fixed_records': 0,
            'skipped_lines': 0,
            'errors': []
        }

        badlog_file = open(badlog_path, 'w', encoding='utf-8') if badlog_path else None
        
        with (open(staging_path, 'r', encoding='utf-8') as infile, open(output_path, 'w', encoding='utf-8', newline='') as outfile):
            
            writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)

            # Write header
            writer.writerow(self.expected_columns)

            # Process line by line
            current_record = []
            in_multiline_field = False
            multiline_buffer = ""
            multiline_line_count = 0
            MAX_MULTILINE_LINES = 10

            for line_num, line in enumerate(infile, 1):
                stats['total_lines'] += 1
                line = line.rstrip('\n\r')
                
                if line_num % 10000 == 0 or line_num > 78000:
                    print(f"Processing line {line_num}, records output: {stats['valid_records'] + stats['fixed_records']}, multiline: {in_multiline_field}")

                if line_num == 1:  # Skip original header
                    continue

                try:
                    # Parse line as CSV
                    parsed = list(csv.reader([line]))[0]

                    if not in_multiline_field:
                        # Start new record
                        if len(parsed) == self.expected_count:
                            # Perfect record
                            writer.writerow(parsed)
                            stats['valid_records'] += 1
                        elif len(parsed) < self.expected_count:
                            # Potentially incomplete (multiline field)
                            current_record = parsed
                            in_multiline_field = True
                            multiline_buffer = line
                            multiline_line_count = 1
                        else:
                            # Too many fields - log and skip
                            error_msg = f"Line {line_num}: Too many fields ({len(parsed)}) expected {self.expected_count}"
                            stats['errors'].append(error_msg)
                            if badlog_file:
                                badlog_file.write(error_msg + "\n")
                            stats['skipped_lines'] += 1
                    else:
                        # Continue multiline field
                        multiline_line_count += 1
                        
                        # Safety check: reset if too many lines accumulated
                        if multiline_line_count > MAX_MULTILINE_LINES:
                            # Output partial record with padding
                            while len(current_record) < self.expected_count:
                                current_record.append("")
                            writer.writerow(current_record[:self.expected_count])
                            stats['fixed_records'] += 1
                            
                            error_msg = f"Line {line_num}: Multiline field exceeded {MAX_MULTILINE_LINES} lines, outputting partial record"
                            stats['errors'].append(error_msg)
                            if badlog_file:
                                badlog_file.write(error_msg + "\n")
                            in_multiline_field = False
                            current_record = []
                            multiline_buffer = ""
                            multiline_line_count = 0
                            continue
                            
                        multiline_buffer += "\n" + line

                        # Try to parse accumulated buffer
                        try:
                            accumulated = list(csv.reader([multiline_buffer]))[0]
                            if len(accumulated) == self.expected_count:
                                # Complete record found
                                writer.writerow(accumulated)
                                stats['fixed_records'] += 1
                                in_multiline_field = False
                                current_record = []
                                multiline_buffer = ""
                                multiline_line_count = 0
                            elif len(accumulated) > self.expected_count:
                                # Overshot - reset and try current line as new record
                                error_msg = f"Line {line_num}: Multiline field overshot, resetting"
                                stats['errors'].append(error_msg)
                                if badlog_file:
                                    badlog_file.write(error_msg + "\n")
                                in_multiline_field = False
                                current_record = []
                                multiline_buffer = ""
                                multiline_line_count = 0
                                # Retry current line as new record
                                if len(parsed) == self.expected_count:
                                    writer.writerow(parsed)
                                    stats['valid_records'] += 1
                        except Exception:
                            # Continue accumulating
                            pass

                except Exception as e:
                    if in_multiline_field:
                        # Add to multiline buffer
                        multiline_buffer += "\n" + line
                    else:
                        error_msg = f"Line {line_num}: Parse error - {e}"
                        stats['errors'].append(error_msg)
                        if badlog_file:
                            badlog_file.write(error_msg + "\n")
                        stats['skipped_lines'] += 1

        # Handle any remaining multiline buffer at end of file
        if in_multiline_field and multiline_buffer:
            try:
                accumulated = list(csv.reader([multiline_buffer]))[0]
                if len(accumulated) <= self.expected_count:
                    # Pad with empty strings if needed
                    while len(accumulated) < self.expected_count:
                        accumulated.append("")
                    writer.writerow(accumulated[:self.expected_count])
                    stats['fixed_records'] += 1
                    if badlog_file:
                        badlog_file.write(f"EOF: Flushed incomplete multiline record with {len(accumulated)} fields\n")
            except Exception as e:
                if badlog_file:
                    badlog_file.write(f"EOF: Failed to flush multiline buffer: {e}\n")
                stats['skipped_lines'] += 1
        
        if badlog_file:
            badlog_file.close()
        
        # Cleanup staging file
        try:
            staging_path.unlink()
        except Exception:
            pass
            
        logger.info(f"CSV fix complete: {stats['valid_records']} valid, {stats['fixed_records']} fixed, {stats['skipped_lines']} skipped")
        return stats

def main():
    """
    Fixes a malformed CSV file using a provided JSON schema, saving the corrected
    file and optionally logging errors.

    Parameters:
    input_csv: str
        The path to the input malformed CSV file.
    schema_json: str
        The path to the JSON file containing the schema to validate and fix the
        CSV.
    output_csv: str
        The path to the output file for the fixed CSV.
    badlog: str, optional
        The path to the file where errors will be logged during the processing.

    Returns:
    None

    Raises:
    None
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Fix malformed CSV using schema")
    parser.add_argument("input_csv", help="Input CSV file")
    parser.add_argument("schema_json", help="Schema JSON file")
    parser.add_argument("output_csv", help="Output fixed CSV file")
    parser.add_argument("--badlog", help="Write error log to file")
    
    args = parser.parse_args()
    
    fixer = CSVFixer(args.schema_json)
    stats = fixer.fix_csv(args.input_csv, args.output_csv, args.badlog)
    
    print(f"Fixed CSV saved to: {args.output_csv}")
    print(f"Stats: {stats}")

if __name__ == "__main__":
    main()