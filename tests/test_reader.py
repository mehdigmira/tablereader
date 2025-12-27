from pathlib import Path
from tablereader import read


def main(**kwargs):
    reader = read(**kwargs)
    for table in reader.tables:
        print("================================================")
        for row in table:
            print(row)


if __name__ == "__main__":
    """
================================================
{'Sold': 'iPhone 4', 'Purchase_date': datetime.datetime(2023, 12, 10, 0, 0), 'Amount': 100.12}
{'Sold': 'MacBook Pro', 'Purchase_date': datetime.datetime(2023, 12, 23, 0, 0), 'Amount': 1000.13}
================================================
{'Rental': 'iPhone 4', 'Amount': 10.12}
    """
    main(
        file_path=Path(__file__).parent / "test.xlsx", is_csv=False, sheet_name="sales"
    )
    """
    ================================================
    {'person': 'John', 'job': 'Dev'}
    {'person': 'James', 'job': 'Designer'}
    """
    main(
        file_path=Path(__file__).parent / "test.xlsx", is_csv=False, sheet_name="people"
    )
    main(file_path=Path(__file__).parent / "test.csv", is_csv=True)
    """
    ================================================
    {'person': 'John', 'job': 'Dev'}
    {'person': 'James', 'job': 'Designer'}
    """
