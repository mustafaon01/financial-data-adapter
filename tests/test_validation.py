"""Tests for validation rules."""

import pytest

from adapter.schemas import CREDIT_FIELDS, PAYMENT_PLAN_FIELDS
from adapter.validators import validate_and_normalize


@pytest.mark.parametrize(
    "payload,field,code",
    [
        ({"loan_status_code": "A"}, "loan_account_number", "MISSING_FIELD"),
        ({"loan_account_number": "L1"}, "original_loan_amount", "MISSING_FIELD"),
        (
            {"loan_account_number": "L1", "original_loan_amount": -10},
            "original_loan_amount",
            "OUT_OF_RANGE",
        ),
    ],
)
def test_credit_validation_errors(payload, field, code):
    """Credit validation should return errors."""
    normalized, errors = validate_and_normalize(payload, CREDIT_FIELDS)
    assert any(e["field"] == field and e["code"] == code for e in errors)


def test_payment_plan_validation_errors():
    """Payment plan validation should return errors."""
    payload = {"loan_account_number": "L1", "installment_number": "x"}
    normalized, errors = validate_and_normalize(payload, PAYMENT_PLAN_FIELDS)
    assert any(e["field"] == "installment_number" for e in errors)
