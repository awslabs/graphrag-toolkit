# Copyright (c) Evan Erwee. All rights reserved.

#!/usr/bin/env python3
"""
Script to create Parquet and Excel versions of test_documents.csv
Run this script to generate test_documents.parquet and test_documents.xlsx
"""

import pandas as pd
import json

# Read the CSV file with proper quoting and escaping
df = pd.read_csv('test_documents.csv', quotechar='"', escapechar='\\', skipinitialspace=True)

# Convert metadata column from string to proper JSON objects for better handling
df['metadata'] = df['metadata'].apply(lambda x: json.loads(x.replace("'", '"')))

# Create Parquet file
print("Creating test_documents.parquet...")
df.to_parquet('test_documents.parquet', index=False, engine='pyarrow')
print("✓ Parquet file created successfully")

# Create Excel file
print("Creating test_documents.xlsx...")
with pd.ExcelWriter('test_documents.xlsx', engine='openpyxl') as writer:
    # Convert metadata back to string for Excel compatibility
    df_excel = df.copy()
    df_excel['metadata'] = df_excel['metadata'].apply(lambda x: json.dumps(x))
    
    # Write to Excel with formatting
    df_excel.to_excel(writer, sheet_name='test_documents', index=False)
    
    # Get the workbook and worksheet
    workbook = writer.book
    worksheet = writer.sheets['test_documents']
    
    # Auto-adjust column widths
    for column in worksheet.columns:
        max_length = 0
        column_letter = column[0].column_letter
        for cell in column:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = min(max_length + 2, 50)  # Cap at 50 characters
        worksheet.column_dimensions[column_letter].width = adjusted_width

print("✓ Excel file created successfully")

print("\nFiles created:")
print("- test_documents.parquet (Apache Parquet format)")
print("- test_documents.xlsx (Microsoft Excel format)")
print("\nBoth files contain the same 20 test documents with all transformers test scenarios.")