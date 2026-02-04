"""Schema definitions for adapter datasets."""

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class FieldSpec:
    """Schema field definition."""

    name: str
    field_type: str
    required: bool = False
    min_value: Decimal | int | None = None
    max_value: Decimal | int | None = None


CREDIT_FIELDS: list[FieldSpec] = [
    FieldSpec("loan_account_number", "str", required=True),
    FieldSpec("customer_type", "category"),
    FieldSpec("customer_id", "str"),
    FieldSpec("loan_product_type", "str"),
    FieldSpec("loan_status_code", "category", required=True),
    FieldSpec("loan_status_flag", "category"),
    FieldSpec("days_past_due", "int", min_value=0),
    FieldSpec("final_maturity_date", "date"),
    FieldSpec("total_installment_count", "int", min_value=0),
    FieldSpec("outstanding_installment_count", "int", min_value=0),
    FieldSpec("paid_installment_count", "int", min_value=0),
    FieldSpec("first_payment_date", "date"),
    FieldSpec("original_loan_amount", "decimal", required=True, min_value=0),
    FieldSpec("outstanding_principal_balance", "decimal", min_value=0),
    FieldSpec("nominal_interest_rate", "rate", min_value=0),
    FieldSpec("total_interest_amount", "decimal", min_value=0),
    FieldSpec("kkdf_rate", "rate", min_value=0),
    FieldSpec("kkdf_amount", "decimal", min_value=0),
    FieldSpec("bsmv_rate", "rate", min_value=0),
    FieldSpec("bsmv_amount", "decimal", min_value=0),
    FieldSpec("grace_period_months", "int", min_value=0),
    FieldSpec("installment_frequency", "int", min_value=0),
    FieldSpec("loan_start_date", "date", required=True),
    FieldSpec("loan_closing_date", "date"),
    FieldSpec("customer_region_code", "str"),
    FieldSpec("sector_code", "str"),
    FieldSpec("internal_credit_rating", "str"),
    FieldSpec("default_probability", "rate", min_value=0, max_value=1),
    FieldSpec("risk_class", "str"),
    FieldSpec("customer_segment", "str"),
    FieldSpec("internal_rating", "str"),
    FieldSpec("external_rating", "str"),
    FieldSpec("insurance_included", "category"),
]


PAYMENT_PLAN_FIELDS: list[FieldSpec] = [
    FieldSpec("loan_account_number", "str", required=True),
    FieldSpec("installment_number", "int", required=True, min_value=0),
    FieldSpec("actual_payment_date", "date"),
    FieldSpec("scheduled_payment_date", "date", required=True),
    FieldSpec("installment_amount", "decimal", required=True, min_value=0),
    FieldSpec("principal_component", "decimal", min_value=0),
    FieldSpec("interest_component", "decimal", min_value=0),
    FieldSpec("kkdf_component", "decimal", min_value=0),
    FieldSpec("bsmv_component", "decimal", min_value=0),
    FieldSpec("installment_status", "category"),
    FieldSpec("remaining_principal", "decimal", min_value=0),
    FieldSpec("remaining_interest", "decimal", min_value=0),
    FieldSpec("remaining_kkdf", "decimal", min_value=0),
    FieldSpec("remaining_bsmv", "decimal", min_value=0),
]


def get_schema(dataset_type: str) -> list[FieldSpec]:
    """Return schema list by dataset type."""
    dataset_type = (dataset_type or "").upper()
    if dataset_type == "CREDIT":
        return CREDIT_FIELDS
    if dataset_type == "PAYMENT_PLAN":
        return PAYMENT_PLAN_FIELDS
    raise ValueError("dataset_type must be CREDIT or PAYMENT_PLAN")


def get_clickhouse_columns(dataset_type: str) -> list[tuple[str, str]]:
    """Return ClickHouse columns for dataset."""
    type_map = {
        "str": "String",
        "category": "String",
        "int": "Int32",
        "decimal": "Decimal(18,2)",
        "rate": "Decimal(9,6)",
        "date": "Date",
    }
    cols: list[tuple[str, str]] = []
    for spec in get_schema(dataset_type):
        base = type_map[spec.field_type]
        ch_type = base if spec.required else f"Nullable({base})"
        cols.append((spec.name, ch_type))
    return cols


def get_field_names(dataset_type: str) -> list[str]:
    """Return field names for dataset."""
    return [spec.name for spec in get_schema(dataset_type)]


def get_numeric_fields(dataset_type: str) -> list[str]:
    """Return numeric fields for dataset."""
    numeric_types = {"int", "decimal", "rate"}
    return [
        spec.name
        for spec in get_schema(dataset_type)
        if spec.field_type in numeric_types
    ]


def get_categorical_fields(dataset_type: str) -> list[str]:
    """Return categorical fields for dataset."""
    cat_types = {"str", "category"}
    return [
        spec.name for spec in get_schema(dataset_type) if spec.field_type in cat_types
    ]
