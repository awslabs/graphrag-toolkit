# Copyright (c) Evan Erwee. All rights reserved.

# Test Document Formats

This directory contains the same test data in multiple formats for comprehensive testing:

## Available Formats

1. **CSV** (`test_documents.csv`) - Original comma-separated values format
2. **JSON** (`test_documents.json`) - JavaScript Object Notation with proper data types
3. **XML** (`test_documents.xml`) - Extensible Markup Language with structured elements
4. **YAML** (`test_documents.yaml`) - YAML Ain't Markup Language, human-readable format

## Additional Formats (Generate as needed)

### Parquet Format
To create `test_documents.parquet`:
```python
import pandas as pd
df = pd.read_csv('test_documents.csv')
df.to_parquet('test_documents.parquet', index=False)
```

### Excel Format
To create `test_documents.xlsx`:
```python
import pandas as pd
df = pd.read_csv('test_documents.csv')
df.to_excel('test_documents.xlsx', index=False, sheet_name='test_documents')
```

## Data Content

All formats contain 20 test documents with:

- **Standard Content**: Regular documents for basic processing
- **PII Data**: Document #13 with SSN, email, phone, IP, credit card
- **Spelling Errors**: Document #14 with intentional misspellings
- **Multi-language**: Document #16 with 8 different languages
- **Toxic Content**: Document #17 with inappropriate language
- **Mixed Sentiment**: Document #18 with positive and negative emotions
- **Security Threats**: Document #19 with XSS, SQL, command injection
- **Performance Test**: Document #20 with long text content

## Usage in Notebooks

Each format can be used to test different ingestion methods:

```python
# CSV
import pandas as pd
df = pd.read_csv('test_documents.csv')

# JSON
import json
with open('test_documents.json', 'r') as f:
    data = json.load(f)

# XML
import xml.etree.ElementTree as ET
tree = ET.parse('test_documents.xml')

# YAML
import yaml
with open('test_documents.yaml', 'r') as f:
    data = yaml.safe_load(f)

# Parquet (if created)
df = pd.read_parquet('test_documents.parquet')

# Excel (if created)
df = pd.read_excel('test_documents.xlsx')
```

## Requirements

For Parquet and Excel formats, install required packages:
```bash
pip install pandas pyarrow openpyxl
```