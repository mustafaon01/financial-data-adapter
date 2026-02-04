"""CSV helper functions for external bank."""

import csv


def normalize_bank_code(value) -> str:
    """
    Normalize bank code.
    """
    return (value or "").strip().upper()


def normalize_loan_type(value) -> str:
    """
    Normalize loan type.
    """
    # Frontend might send data_type; we normalize and map it to loan_type concept.
    v = (value or "").strip().upper()
    return v


def sniff_dialect(sample: str) -> csv.Dialect:
    """
    Detect CSV delimiter.
    """
    try:
        return csv.Sniffer().sniff(sample)
    except csv.Error:
        if "\t" in sample:
            d = csv.excel_tab
        else:
            d = csv.excel
        if ";" in sample:
            d.delimiter = ";"
        return d


def detect_external_id(row: dict):
    """
    Find loan id in row.
    """
    for key in ("loan_account_number", "external_id", "id", "loan_id", "ref"):
        v = row.get(key)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return None


def safe_str(v):
    """
    Convert value to clean string.
    """
    if v is None:
        return None
    s = str(v).strip()
    return s if s != "" else None
