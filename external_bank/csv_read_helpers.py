import csv


def normalize_bank_code(value) -> str:
    """
    Capitalize and strip the bank code.

    :param value: The input bank code string (or None).
    :return: The normalized bank code.
    """
    return (value or "").strip().upper()


def normalize_loan_type(value) -> str:
    """
    Normalize the loan type (or data type).

    :param value: The input string.
    :return: The normalized type, e.g., 'RETAIL'.
    """
    # Frontend might send data_type; we normalize and map it to loan_type concept.
    v = (value or "").strip().upper()
    return v


def sniff_dialect(sample: str) -> csv.Dialect:
    """
    Guess the CSV delimiter from a sample string.

    :param sample: A text sample from the CSV file.
    :return: The detected csv.Dialect object.
    """
    """
    Try to sniff delimiter. Fallback to common delimiters.
    """
    try:
        return csv.Sniffer().sniff(sample)
    except csv.Error:
        # fallback: detect common delimiters
        if "\t" in sample:
            d = csv.excel_tab
        else:
            d = csv.excel
        # semicolon is common in TR exports
        if ";" in sample:
            d.delimiter = ";"
        return d


def detect_external_id(row: dict):
    """
    Find the unique loan ID in a row dictionary.

    It checks multiple common keys like 'loan_account_number' or 'id'.

    :param row: The CSV row dictionary.
    :return: The found ID as a string, or None.
    """
    """
    For your CSV, the correct unique loan id is loan_account_number.
    Fallbacks included for safety.
    """
    for key in ("loan_account_number", "external_id", "id", "loan_id", "ref"):
        v = row.get(key)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return None


def safe_str(v):
    """
    Convert a value to a clean string or return None.

    :param v: The input value.
    :return: A stripped string or None if empty.
    """
    if v is None:
        return None
    s = str(v).strip()
    return s if s != "" else None
