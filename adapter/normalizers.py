"""Normalization helpers for adapter data."""

from datetime import date, datetime
from decimal import Decimal


class NormalizationService:
    """
    Service to normalize various data fields like dates, rates and categories.
    """

    @staticmethod
    def normalize_date(date_str):
        """
        Convert a date string to a date.
        """
        if date_str is None or date_str == "":
            return None

        if isinstance(date_str, datetime):
            return date_str.date()
        if isinstance(date_str, date):
            return date_str

        date_str = str(date_str).strip()

        for fmt in ("%Y%m%d", "%Y-%m-%d", "%d.%m.%Y", "%Y/%m/%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        return None

    @staticmethod
    def normalize_decimal(value):
        """Convert value to Decimal."""
        if value is None or value == "":
            return None
        if isinstance(value, Decimal):
            return value
        if isinstance(value, (int, float)):
            return Decimal(str(value))

        s = str(value).strip()
        if s == "":
            return None

        s = s.replace(" ", "").replace(",", ".")
        if s.count(".") > 1:
            parts = s.split(".")
            s = "".join(parts[:-1]) + "." + parts[-1]

        try:
            return Decimal(s)
        except Exception:
            return None

    @staticmethod
    def normalize_rate(rate_str):
        """
        Convert rate string to Decimal (0-1).
        """
        if rate_str is None or rate_str == "":
            return None

        s = str(rate_str).lower().strip()

        if "bps" in s:
            val = NormalizationService.normalize_decimal(s.replace("bps", "").strip())
            return val / Decimal("10000") if val is not None else None

        if "%" in s:
            val = NormalizationService.normalize_decimal(s.replace("%", "").strip())
            return val / Decimal("100") if val is not None else None

        val = NormalizationService.normalize_decimal(s)
        if val is None:
            return None

        if val > 1:
            if val >= Decimal("1000"):
                return val / Decimal("10000")
            return val / Decimal("100")
        return val

    @staticmethod
    def normalize_int(value):
        """Convert value to int."""
        if value is None or value == "":
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        s = str(value).strip()
        if s == "":
            return None
        try:
            return int(Decimal(s.replace(",", ".")).to_integral_value())
        except Exception:
            return None

    @staticmethod
    def normalize_string(value):
        """Convert value to clean string."""
        if value is None:
            return None
        s = str(value).strip()
        return s if s != "" else None

    @staticmethod
    def normalize_category(category_str):
        """
        Map category strings to standard codes.
        """
        if category_str is None or category_str == "":
            return "UNKNOWN"

        s = str(category_str).lower().strip()

        if s in ["k", "kapalı", "kapali", "paid", "closed"]:
            return "PAID"
        if s in ["a", "aktif", "active"]:
            return "ACTIVE"
        if s in ["g", "gecikmiş", "gecikmis", "overdue", "delinquent"]:
            return "OVERDUE"

        return "UNKNOWN"
