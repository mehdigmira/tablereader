# TableReader

Automatically extract clean, typed data from messy Excel and CSV files using LLM-powered table detection.

## Installation

```bash
pip install git+https://github.com/mehdigmira/tablereader.git
```

## Usage

Set environment variables:

```bash
export OPENAI_API_KEY="your-deepseek-api-key"
export OPENAI_BASE_URL="https://api.deepseek.com"
```

Then

```python
from tablereader import read

# Uses OpenAI by default (requires OPENAI_API_KEY env var)
result = read("data.xlsx", sheet_name="Sheet1")

# Process a CSV file
result = read("data.csv", is_csv=True)

# Iterate through extracted tables
for table in result.tables:
    for row in table:
        print(row)  # Each row is a dict with typed values
```

## Features

The `read()` function automatically:
- Detects table boundaries (skips headers, footers, totals)
- Identifies column types (numbers, dates, times, strings)
- Handles messy formatting (currency symbols, percentages, thousand separators)
- Streams data efficiently for large files

## Requirements

- Python 3.12+
- OpenAI API key or any OpenAI-compatible API (DeepSeek, etc.)
