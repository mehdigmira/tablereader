import csv
from pathlib import Path


def _get_max_row_with_data(csv_path: str | Path, dialect: type[csv.Dialect]):
    max_row = 0
    with open(csv_path, "r") as f:
        reader = csv.reader(f, dialect=dialect)
        for i, row in enumerate(reader):
            if any(cell is not None for cell in row):
                max_row = i
    return max_row


def _get_row_str(idx, row):
    row_str = " | ".join(str(cell) if cell is not None else "" for cell in row)
    return f"Row {idx}: {row_str}"


def get_csv_content(file_path: str | Path):
    with open(file_path, "r") as f:
        dialect = csv.Sniffer().sniff(f.read(1024))

    row_count = _get_max_row_with_data(file_path, dialect) + 1

    content = []
    content.append(f"CSV file: {file_path}")
    content.append(f"Total rows: {row_count}")

    if row_count > 200:
        # only return preview
        content.append("CSV file preview (only first and last 20 rows):")
        content.append("--------------------------------")
        # keep only 20 first rows and 20 last rows
        with open(file_path, "r") as f:
            reader = csv.reader(f)
            for idx, row in enumerate(reader):
                if idx < 20:
                    content.append(_get_row_str(idx, row))
                elif idx == 20:
                    content.append("\n...\n")
                elif idx >= row_count - 20:
                    content.append(_get_row_str(idx, row))
                else:
                    continue
    else:
        content.append("CSV file full content:")
        content.append("--------------------------------")
        with open(file_path, "r") as f:
            reader = csv.reader(f)
            for idx, row in enumerate(reader):
                content.append(_get_row_str(idx, row))

    return "\n".join(content)


def table_iterator(
    data_start_row: int,
    data_end_row: int,
    start_col: int,
    end_col: int,
    skip_rows: list,
    *,
    file_path: str | Path,
):
    """
    Extract table data from CSV file based on specified range.

    Uses Python's csv module to stream data without loading entire file into memory.
    Yields rows within the specified range, skipping rows as indicated.
    """

    # Convert to set for faster lookup
    skip_rows_set = set(skip_rows) if skip_rows else set()

    # Write CSV directly
    with open(file_path, "r") as original_file:
        dialect = csv.Sniffer().sniff(original_file.read(1024))
        original_file.seek(0)
        reader = csv.reader(original_file, dialect=dialect)

        for row_idx, row in enumerate(reader):
            if row_idx < data_start_row or row_idx > data_end_row:
                continue
            if row_idx in skip_rows_set:
                continue
            yield row[start_col : end_col + 1]
