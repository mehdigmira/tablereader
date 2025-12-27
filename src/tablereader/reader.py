from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Iterator, cast
from openai import OpenAI
from tablereader.csv import get_csv_content, table_iterator
from tablereader.parser import parse_iterator
from tablereader.xls import get_sheet_content, table_iterator as xls_table_iterator


@dataclass
class TableReaderResult:
    tables: list[Iterator[dict[str, str]]]
    errors: list[str]


CLEANING_AGENT_PROMPT = """You are an Excel sheet analyzer.

## Your Role

You receive ONE Excel sheet and identify where the raw data table(s) are located.

**Your job**: Identify all distinct data tables in the sheet. Call `identify_table_range` once for each table found.

## What You Receive

1. **Sheet content**: Complete data from the sheet (all rows shown with row numbers)

## Your Process

1. **Scan the entire sheet**
   - Look for all distinct data tables (there may be multiple)
   - Tables are separated by: blank rows, different column structures, or metadata sections

2. **For each table found, identify:**
   - Where the **header row** is (column names like "Date", "Product", "Amount")
   - Where the **data starts** (first row after headers)
   - Where the **data ends** (before aggregation rows like "Total", "Sum", "Average")
   - Which **columns** contain the table data

3. **What to skip**
   - **Skip at top**: Title rows, metadata, report headers, blank rows before table
   - **Skip at bottom**: Totals, sums, aggregations, notes, blank rows after table
   - **Skip between tables**: Blank rows, section dividers

4. **Call identify_table_range() for each table**

Each call should include:
   - `header_columns`: List of column definitions with name, type, and optional decimal_separator for numbers (e.g., [{"name": "Date", "type": "date"}, {"name": "Product", "type": "str"}, {"name": "Amount", "type": "number", "decimal_separator": "."}])
   - `data_start_row`: First row of actual data (0-indexed)
   - `data_end_row`: Last row of actual data (0-indexed, inclusive)
   - `start_col`: First column with data (0-indexed)
   - `end_col`: Last column with data (0-indexed, inclusive)
   - `skip_rows`: List of row indices to skip within the data range (empty if none)
   - `table_name`: Suggested name for this table (lowercase, underscores)
   - `description`: Brief description of what this table contains

## What to Focus On

**DO focus on:**
- ✅ Finding the header row (column names and their data types)
- ✅ Inferring data types from the first few data rows (number, date, datetime, time, str)
- ✅ Finding where actual data starts and ends (rows)
- ✅ Finding which columns contain the table (skip empty columns on left/right)
- ✅ Identifying rows to skip (blank rows, subtotals, totals)

**DON'T focus on:**
- ❌ Data type conversion (handled automatically)
- ❌ Formatting cleaning (handled automatically)
- ❌ Column renaming (keep original names)
- ❌ Data validation or quality checks

## Tool Available

- **identify_table_range(...)**: Submit your analysis

Parameters:
- `header_columns` (list[object]): List of column definitions with name, type, and optional metadata
  - Each object has:
    - `name` (str) - column name
    - `type` (str) - one of: "number", "date", "datetime", "time", "str"
    - `decimal_separator` (str, optional) - for number columns only: "." or ","
  - Example: [{"name": "Date", "type": "date"}, {"name": "Product", "type": "str"}, {"name": "Amount", "type": "number", "decimal_separator": "."}]
- `data_start_row` (int): First row of actual data (0-indexed)
- `data_end_row` (int): Last row of actual data (0-indexed, inclusive)
- `start_col` (int): First column index with data (0-indexed)
- `end_col` (int): Last column index with data (0-indexed, inclusive)
- `skip_rows` (list[int]): Row indices to skip within data range (empty list if none)
- `table_name` (str): Suggested table name (lowercase, underscores, no spaces)
- `description` (str): What this table contains

**Type Inference Guidelines:**
- `number`: Numeric values (integers, decimals, currency like "$1,234.56", percentages like "25%")
  - **decimal_separator**: Look at the first few numeric values to detect:
    - `"."` for formats like: 1234.56, $1,234.56, 25.5%
    - `","` for formats like: 1234,56, €1.234,56, 25,5%
- `date`: Date only (e.g., "2024-01-01", "01/15/2024", "Jan 15, 2024")
- `datetime`: Date with time (e.g., "2024-01-01 14:30:00", "01/15/2024 2:30 PM")
- `time`: Time only (e.g., "14:30:00", "2:30 PM")
- `str`: Text, codes, categories, or mixed content

## Common Patterns

### Pattern 1: Clean table (headers at row 0, data starts row 1, no aggregations)
```
Row 0: Date | Product | Amount
Row 1: 2024-01-01 | Widget | $100
Row 2: 2024-01-02 | Gadget | $200
...
```
→ `header_columns=[{"name": "Date", "type": "date"}, {"name": "Product", "type": "str"}, {"name": "Amount", "type": "number", "decimal_separator": "."}], data_start_row=1, data_end_row=<last_row>, skip_rows=[]`

### Pattern 2: Title rows at top
```
Row 0: Sales Report Q4 2024
Row 1: Generated: 2024-12-19
Row 2: (blank)
Row 3: Date | Product | Amount
Row 4: 2024-01-01 | Widget | $100
...
```
→ `header_columns=[{"name": "Date", "type": "date"}, {"name": "Product", "type": "str"}, {"name": "Amount", "type": "number", "decimal_separator": "."}], data_start_row=4, data_end_row=<last_row>, skip_rows=[]`

### Pattern 3: Aggregation rows at bottom
```
Row 0: Date | Product | Amount
Row 1-100: (data rows)
Row 101: TOTAL | | $50,000
Row 102: (blank)
```
→ `header_columns=[{"name": "Date", "type": "date"}, {"name": "Product", "type": "str"}, {"name": "Amount", "type": "number", "decimal_separator": "."}], data_start_row=1, data_end_row=100, skip_rows=[]`

### Pattern 4: Subtotals within data
```
Row 0: Date | Product | Amount
Row 1-50: (data rows)
Row 51: Subtotal Q1 | | $10,000
Row 52-100: (data rows)
Row 101: Subtotal Q2 | | $12,000
...
```
→ `header_columns=[{"name": "Date", "type": "date"}, {"name": "Product", "type": "str"}, {"name": "Amount", "type": "number", "decimal_separator": "."}], data_start_row=1, data_end_row=<last_data_row>, skip_rows=[51, 101, ...]`

## Example Interaction

**Input:**
```
Sheet: "Q4 Sales"
Row 0: Sales Report - Q4 2024
Row 1: Department: Electronics
Row 2: (blank)
Row 3: Date | Product | Customer | Amount | Qty
Row 4: 2024-10-01 | Widget | Acme Corp | $1,200 | 5
Row 5: 2024-10-02 | Gadget | TechCo | $850 | 3
...
Row 153: 2024-12-31 | Gizmo | MegaCorp | $2,100 | 7
Row 154: (blank)
Row 155: TOTAL | | | $450,000 | 1,523
```

**Your Response:**

Call `identify_table_range`:
```python
identify_table_range(
    header_columns=[
        {"name": "Date", "type": "date"},
        {"name": "Product", "type": "str"},
        {"name": "Customer", "type": "str"},
        {"name": "Amount", "type": "number", "decimal_separator": "."},
        {"name": "Qty", "type": "number", "decimal_separator": "."}
    ],
    data_start_row=4,
    data_end_row=153,
    start_col=0,
    end_col=4,
    skip_rows=[],
    table_name="q4_sales",
    description="Q4 2024 sales transactions with product, customer, and pricing"
)
```

## Important Notes

- **0-indexed rows**: Remember row numbers start at 0
- **Inclusive end**: `data_end_row` is inclusive (last row of data)
- **Column names**: Extract the actual column names from the header row (strip whitespace, keep original text)
- **Look for keywords**: "Total", "Sum", "Subtotal", "Average" indicate aggregation rows
- **Blank rows**: Multiple consecutive blank rows often separate sections
- **Be precise**: Get the exact column names and row numbers - the system will search for the header and extract data

## Success Criteria

Your analysis succeeds when:
1. ✅ You correctly extract the header column names
2. ✅ You correctly identify data start and end rows
3. ✅ You exclude all aggregation/summary rows
4. ✅ You provide a clean table name (lowercase, underscores)
5. ✅ You provide an accurate description

The system will then search for the header row, extract the data rows, and convert to CSV automatically (no pandas script needed from you).

Now, analyze the sheet and identify the table range!
"""

tools: list[Any] = [
    {
        "type": "function",
        "function": {
            "name": "identify_table_range",
            "description": "Submit your analysis of where the table is located in the sheet",
            "parameters": {
                "type": "object",
                "properties": {
                    "header_columns": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {
                                    "type": "string",
                                    "description": "Column name",
                                },
                                "type": {
                                    "type": "string",
                                    "enum": [
                                        "number",
                                        "date",
                                        "datetime",
                                        "time",
                                        "str",
                                    ],
                                    "description": "Data type of the column",
                                },
                                "decimal_separator": {
                                    "type": "string",
                                    "enum": [".", ","],
                                    "description": "For number columns only: decimal separator ('.' or ',')",
                                },
                            },
                            "required": ["name", "type"],
                        },
                        "description": "List of column definitions with name, type, and optional decimal_separator for numbers (e.g., [{'name': 'Date', 'type': 'date'}, {'name': 'Amount', 'type': 'number', 'decimal_separator': '.'}])",
                    },
                    "data_start_row": {
                        "type": "integer",
                        "description": "First row of actual data (0-indexed)",
                    },
                    "data_end_row": {
                        "type": "integer",
                        "description": "Last row of actual data (0-indexed, inclusive)",
                    },
                    "start_col": {
                        "type": "integer",
                        "description": "First column index with data (0-indexed)",
                    },
                    "end_col": {
                        "type": "integer",
                        "description": "Last column index with data (0-indexed, inclusive)",
                    },
                    "skip_rows": {
                        "type": "array",
                        "items": {"type": "integer"},
                        "description": "Row indices to skip within data range (empty array if none)",
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Suggested table name (lowercase, underscores, no spaces)",
                    },
                    "description": {
                        "type": "string",
                        "description": "Brief description of what this table contains",
                    },
                },
                "required": [
                    "header_columns",
                    "data_start_row",
                    "data_end_row",
                    "start_col",
                    "end_col",
                    "skip_rows",
                    "table_name",
                    "description",
                ],
            },
        },
    }
]


def read(
    file_path: str | Path,
    sheet_name: str | None = None,
    is_csv: bool = False,
    model: str = "gpt-4o-mini",
) -> TableReaderResult:
    """
    Process a spreadsheet file and extract table data.

    Uses an LLM to analyze the sheet structure and identify table boundaries,
    then deterministically extracts the data and parses it into typed dictionaries.

    Args:
        file_path: Path to the Excel (.xlsx) or CSV file
        sheet_name: Name of the sheet to process (required for Excel files, ignored for CSV)
        is_csv: Whether the file is a CSV file (True) or Excel file (False)
        model: Model name (default: "gpt-4o-mini" for OpenAI, use "deepseek-chat" for DeepSeek)
        api_key: API key for the LLM provider (defaults to OPENAI_API_KEY env var)
        base_url: Base URL for API (e.g., "https://api.deepseek.com" for DeepSeek)

    Returns:
        TableReaderResult containing a list of table iterators and any errors encountered
    """

    sheet_content = (
        get_csv_content(file_path)
        if is_csv
        else get_sheet_content(file_path, cast(str, sheet_name))
    )

    # Prepare context for the agent
    context = f"""
## Task

Analyze this Excel sheet and identify where the raw data table is located.

## Sheet Content

{sheet_content}

## Instructions

Find the table boundaries:
- Header row (column names)
- Data start row (first data row)
- Data end row (last data row before aggregations/totals)
- Rows to skip within data (subtotals, blank rows)

Then call identify_table_range() with your analysis.

Don't think too much, just identify the table boundaries.
"""
    # Initialize conversation
    messages: list[Any] = [
        {"role": "system", "content": CLEANING_AGENT_PROMPT},
        {"role": "user", "content": context},
    ]

    # Initialize OpenAI-compatible client
    client = OpenAI()

    max_iterations = 3  # Prevent infinite loops

    result = TableReaderResult(tables=[], errors=[])

    for _ in range(max_iterations):
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            tools=tools,
            tool_choice="auto",
        )

        message = response.choices[0].message

        # Add assistant message to conversation
        messages.append(message)

        if not message.tool_calls:
            break

        # Process all tool calls from this turn
        for tool_call in message.tool_calls:
            # Parse the range information
            range_info = json.loads(tool_call.function.arguments)  # type: ignore

            # fill empty column names with the column index
            for col_index, col in enumerate(range_info["header_columns"]):
                col["name"] = col["name"].strip().replace(" ", "_")
                if not col["name"]:
                    col["name"] = f"column_{col_index + 1}"

            # Extract the table deterministically

            table_range = (
                table_iterator(
                    data_start_row=range_info["data_start_row"],
                    data_end_row=range_info["data_end_row"],
                    start_col=range_info["start_col"],
                    end_col=range_info["end_col"],
                    skip_rows=range_info["skip_rows"],
                    file_path=file_path,
                )
                if is_csv
                else xls_table_iterator(
                    data_start_row=range_info["data_start_row"],
                    data_end_row=range_info["data_end_row"],
                    start_col=range_info["start_col"],
                    end_col=range_info["end_col"],
                    skip_rows=range_info["skip_rows"],
                    file_path=file_path,
                    sheet_name=cast(str, sheet_name),
                )
            )
            result.tables.append(
                parse_iterator(table_range, range_info["header_columns"])
            )
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": json.dumps(
                        {
                            "status": "success",
                            "message": "Table created successfully",
                        }
                    ),
                }
            )

    return result


if __name__ == "__main__":
    reader = read("/Users/mehdi/Downloads/test.xlsx", is_csv=False, sheet_name="sales")
    for table in reader.tables:
        for row in table:
            print(row)
