"""Validation helpers for adapter data."""

from .normalizers import NormalizationService
from .schemas import FieldSpec


class ErrorCodes:
    """Error codes used in validation."""

    INVALID_DECIMAL_FORMAT = "INVALID_DECIMAL_FORMAT"
    INVALID_RATE_FORMAT = "INVALID_RATE_FORMAT"
    INVALID_INT_FORMAT = "INVALID_INT_FORMAT"
    INVALID_DATE_FORMAT = "INVALID_DATE_FORMAT"
    INVALID_CATEGORY = "INVALID_CATEGORY"
    MISSING_FIELD = "MISSING_FIELD"
    OUT_OF_RANGE = "OUT_OF_RANGE"
    UNKNOWN_LOAN_ID = "UNKNOWN_LOAN_ID"


def error(code: str, field: str, message: str) -> dict:
    """Build a validation error dict."""
    return {"code": code, "field": field, "message": message}


def validate_and_normalize(
    payload: dict, schema: list[FieldSpec]
) -> tuple[dict, list[dict]]:
    """Validate and normalize one row."""
    errors: list[dict] = []
    normalized: dict = {}
    normalizer = NormalizationService()

    for spec in schema:
        raw_value = payload.get(spec.name)

        if raw_value is None or raw_value == "":
            if spec.required:
                errors.append(
                    error(ErrorCodes.MISSING_FIELD, spec.name, "Field is required.")
                )
            normalized[spec.name] = None
            continue

        if spec.field_type == "str":
            value = normalizer.normalize_string(raw_value)
            if value is None and spec.required:
                errors.append(
                    error(ErrorCodes.MISSING_FIELD, spec.name, "Field is required.")
                )
        elif spec.field_type == "int":
            value = normalizer.normalize_int(raw_value)
            if value is None:
                errors.append(
                    error(
                        ErrorCodes.INVALID_INT_FORMAT,
                        spec.name,
                        f"Invalid integer: {raw_value}",
                    )
                )
        elif spec.field_type == "decimal":
            value = normalizer.normalize_decimal(raw_value)
            if value is None:
                errors.append(
                    error(
                        ErrorCodes.INVALID_DECIMAL_FORMAT,
                        spec.name,
                        f"Invalid decimal: {raw_value}",
                    )
                )
        elif spec.field_type == "rate":
            value = normalizer.normalize_rate(raw_value)
            if value is None:
                errors.append(
                    error(
                        ErrorCodes.INVALID_RATE_FORMAT,
                        spec.name,
                        f"Invalid rate: {raw_value}",
                    )
                )
        elif spec.field_type == "date":
            value = normalizer.normalize_date(raw_value)
            if value is None:
                errors.append(
                    error(
                        ErrorCodes.INVALID_DATE_FORMAT,
                        spec.name,
                        f"Invalid date: {raw_value}",
                    )
                )
        elif spec.field_type == "category":
            value = normalizer.normalize_category(raw_value)
            if spec.required and value == "UNKNOWN":
                errors.append(
                    error(
                        ErrorCodes.INVALID_CATEGORY,
                        spec.name,
                        f"Unknown category: {raw_value}",
                    )
                )
        else:
            value = raw_value

        if value is not None:
            if spec.min_value is not None and value < spec.min_value:
                errors.append(
                    error(
                        ErrorCodes.OUT_OF_RANGE,
                        spec.name,
                        f"Value below minimum: {raw_value}",
                    )
                )
            if spec.max_value is not None and value > spec.max_value:
                errors.append(
                    error(
                        ErrorCodes.OUT_OF_RANGE,
                        spec.name,
                        f"Value above maximum: {raw_value}",
                    )
                )

        normalized[spec.name] = value

    return normalized, errors
