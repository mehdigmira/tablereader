"""
Table creator - converts CSV with schema metadata to properly-typed database tables.

Takes the output from cleaning_agent.py (CSV + schema) and loads it into SQLite
with proper column types and data conversion.
"""

from itertools import chain
import re
from datetime import datetime
from typing import Callable, Iterator, Literal, Optional, TypedDict


def clean_nulls(value: str) -> str | None:
    NULL_VALUES = {
        "",
        "null",
        "none",
        "n/a",
        "na",
        "#na",
        "nan",
        "nan",
        "-",
        "--",
        "—",  # em dash
        "–",  # en dash
        "?",
        "???",
        ".",
        "..",
        "...",
    }
    if value is None:
        return None
    value = value.strip().lower()
    if value in NULL_VALUES:
        return None

    return value


def with_clean(fn):
    def wrapper(value: str, *args, **kwargs):
        cleaned_value = clean_nulls(value)
        if cleaned_value is None:
            return None
        return fn(cleaned_value, *args, **kwargs)

    return wrapper


## parsers


@with_clean
def parse_number(value: str, decimal_separator: str = ".") -> Optional[float]:
    """
    Parse a number string with various formats.

    Handles:
    - Currency symbols: $1,234.56, €1.234,56
    - Percentages: 25.5%
    - Thousand separators: 1,234.56 or 1.234,56
    - Decimal separators: . or ,
    - NULL values (handled by @with_clean decorator)

    Args:
        value: String value to parse
        decimal_separator: "." or "," - which character is the decimal separator

    Returns:
        Float value or None if value is NULL or parsing fails
    """
    # Remove whitespace
    value = value.replace(" ", "")

    # Check for percentage
    is_percentage = value.endswith("%")
    if is_percentage:
        value = value[:-1].strip()

    # Remove currency symbols and whitespace
    value = re.sub(r"[€$£¥\s]", "", value)

    # Handle thousand/decimal separators based on decimal_separator
    if decimal_separator == ",":
        # European format: 1.234,56 -> remove dots (thousand sep), replace comma with dot
        value = value.replace(".", "")
        value = value.replace(",", ".")
    else:
        # US format: 1,234.56 -> remove commas (thousand sep)
        value = value.replace(",", "")

    # Parse as float
    result = float(value)

    # Convert percentage to decimal
    if is_percentage:
        result = result / 100.0

    return result


@with_clean
def parse_date(value: str, fmt: str):
    """
    Parse a date string with a specific format.

    NULL values are handled by @with_clean decorator.

    Args:
        value: String value to parse
        fmt: Date format string (e.g., "%Y-%m-%d")

    Returns:
        datetime object or None if value is NULL or parsing fails
    """
    return datetime.strptime(value, fmt)


@with_clean
def parse_datetime(value: str, fmt: str):
    """
    Parse a datetime string with a specific format.

    NULL values are handled by @with_clean decorator.

    Args:
        value: String value to parse
        fmt: Datetime format string (e.g., "%Y-%m-%d %H:%M:%S")

    Returns:
        datetime object or None if value is NULL or parsing fails
    """
    return datetime.strptime(value, fmt)


@with_clean
def parse_time(value: str, fmt: str):
    """
    Parse a time string with a specific format.

    NULL values are handled by @with_clean decorator.

    Args:
        value: String value to parse
        fmt: Time format string (e.g., "%H:%M:%S")

    Returns:
        datetime object or None if value is NULL or parsing fails
    """
    return datetime.strptime(value, fmt)


# formatters


def get_sample_rows(iterator: Iterator[list[str]]):
    sample_rows = []
    for idx, row in enumerate(iterator):
        if idx >= 100:
            break
        sample_rows.append(row)
    return sample_rows


def find_dt_format(
    sample: list[str], format_fn: Callable, formats: list[str]
) -> Optional[str]:
    for fmt in formats:
        for value in sample:
            try:
                format_fn(value, fmt)
            except Exception:
                break
        else:
            return fmt
    return None


class HeaderColumn(TypedDict):
    name: str
    type: Literal["number", "date", "datetime", "time", "str"]
    decimal_separator: str


def parse_iterator(iterator: Iterator[list[str]], header: list[HeaderColumn]):
    sample = get_sample_rows(iterator)
    formats = {}

    # find the format of the date, datetime, and time columns
    for idx, col in enumerate(header):
        col_sample = [row[idx] for row in sample]
        if col["type"] == "date":
            formats[col["name"]] = find_dt_format(
                col_sample,
                parse_date,
                [
                    "%Y-%m-%d",
                    "%Y/%m/%d",
                    "%m/%d/%Y",
                    "%m/%d/%y",
                    "%d/%m/%Y",
                    "%d/%m/%y",
                    "%d.%m.%Y",
                    "%d.%m.%y",
                    "%b %d, %Y",
                    "%B %d, %Y",
                    "%d %b %Y",
                    "%d %B %Y",
                ],
            )
        elif col["type"] == "datetime":
            formats[col["name"]] = find_dt_format(
                col_sample,
                parse_datetime,
                [
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d %H:%M",
                    "%m/%d/%Y %H:%M:%S",
                    "%m/%d/%Y %I:%M:%S %p",
                    "%m/%d/%Y %I:%M %p",
                    "%d/%m/%Y %H:%M:%S",
                    "%d/%m/%Y %H:%M",
                ],
            )
        elif col["type"] == "time":
            formats[col["name"]] = find_dt_format(
                col_sample, parse_time, ["%H:%M:%S", "%H:%M", "%I:%M:%S %p", "%I:%M %p"]
            )

    for row in chain(sample, iterator):
        row_as_dict = {}
        for idx, col in enumerate(header):
            if col["type"] == "date":
                row_as_dict[col["name"]] = parse_date(row[idx], formats[col["name"]])
            elif col["type"] == "datetime":
                row_as_dict[col["name"]] = parse_datetime(
                    row[idx], formats[col["name"]]
                )
            elif col["type"] == "time":
                row_as_dict[col["name"]] = parse_time(row[idx], formats[col["name"]])
            elif col["type"] == "number":
                row_as_dict[col["name"]] = parse_number(
                    row[idx], col["decimal_separator"]
                )
            else:
                row_as_dict[col["name"]] = row[idx]
        yield row_as_dict
