"""Models for external bank data."""

import uuid

from django.db import models


class DatasetState(models.Model):
    """
    Track dataset version for a bank.
    """

    LOAN_TYPES = [
        ("RETAIL", "Retail Data"),
        ("COMMERCIAL", "Commercial Data"),
    ]

    DATASET_TYPES = [
        ("CREDIT", "Credit Mask"),
        ("PAYMENT_PLAN", "Payment Plan"),
    ]

    bank_code = models.CharField(max_length=20, db_index=True)
    loan_type = models.CharField(max_length=20, choices=LOAN_TYPES, db_index=True)
    dataset_type = models.CharField(max_length=30, choices=DATASET_TYPES, db_index=True)
    dataset_version = models.UUIDField(default=uuid.uuid4)
    checksum = models.CharField(max_length=64, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["bank_code", "loan_type", "dataset_type"],
                name="unique_dataset_state",
            )
        ]
        indexes = [
            models.Index(fields=["bank_code", "loan_type", "dataset_type"]),
        ]
        db_table = "external_bank_dataset_state"


class MockLoan(models.Model):
    """
    Current loan snapshot in external bank.
    """

    LOAN_TYPES = [
        ("RETAIL", "Retail Data"),
        ("COMMERCIAL", "Commercial Data"),
    ]

    bank_code = models.CharField(max_length=20, db_index=True)
    loan_type = models.CharField(max_length=20, choices=LOAN_TYPES, db_index=True)
    external_id = models.CharField(max_length=100, db_index=True)  # loan_account_number
    customer_id = models.CharField(max_length=100, null=True, blank=True)
    payload = models.JSONField()
    dataset_version = models.UUIDField(db_index=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["bank_code", "loan_type", "external_id"],
                name="unique_mock_loan",
            )
        ]
        indexes = [
            models.Index(fields=["bank_code", "loan_type"]),
        ]
        db_table = "external_bank_mock_loan"


class MockLoanPaymentPlan(models.Model):
    """
    Payment plan rows for a loan.
    """

    LOAN_TYPES = [
        ("RETAIL", "Retail Data"),
        ("COMMERCIAL", "Commercial Data"),
    ]

    bank_code = models.CharField(max_length=20, db_index=True)
    loan_type = models.CharField(max_length=20, choices=LOAN_TYPES, db_index=True)
    loan_external_id = models.CharField(max_length=100, db_index=True)
    installment_number = models.IntegerField(db_index=True, null=True, blank=True)
    customer_id = models.CharField(max_length=100, null=True, blank=True)
    payload = models.JSONField()
    dataset_version = models.UUIDField(db_index=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=[
                    "bank_code",
                    "loan_type",
                    "loan_external_id",
                    "installment_number",
                ],
                name="unique_mock_loan_payment_plan_row",
            )
        ]
        db_table = "external_bank_mock_loan_payment_plan"
        indexes = [
            models.Index(fields=["bank_code", "loan_type"]),
            models.Index(fields=["loan_external_id"]),
        ]
